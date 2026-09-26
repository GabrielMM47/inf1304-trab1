#!/bin/bash

# Quantidade total de consumidores desejada
TOTAL_CONSUMERS=${1:-4}

# Calcula a divisão entre base (máx 3) e extras
if [ "$TOTAL_CONSUMERS" -le 3 ]; then
    BASE_REPLICAS=$TOTAL_CONSUMERS
    EXTRA_CONSUMERS=0
else
    BASE_REPLICAS=3
    EXTRA_CONSUMERS=$((TOTAL_CONSUMERS - 3))
fi

echo "Ajustando para $TOTAL_CONSUMERS consumidores ($BASE_REPLICAS base, $EXTRA_CONSUMERS extras rápidas)..."

# Escala o deployment base
kubectl scale deployment consumer --replicas=$BASE_REPLICAS || true

if [ "$EXTRA_CONSUMERS" -gt 0 ]; then
    for i in $(seq 1 $EXTRA_CONSUMERS); do
        # Verifica se o consumidor extra já existe no Kubernetes
        if ! kubectl get deployment consumer-extra-$i > /dev/null 2>&1; then
            echo "Criando consumer-extra-$i..."
            cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer-extra-$i
  labels:
    app: consumer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: consumer
      consumer-id: extra-$i
  template:
    metadata:
      labels:
        app: consumer
        consumer-id: extra-$i
    spec:
      initContainers:
      - name: wait-for-kafka
        image: confluentinc/cp-kafka:latest
        command:
        - /bin/bash
        - -c
        - |
          echo "Waiting for Kafka topic 'dados-sensores' to be created..."
          while ! kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null | grep -q "dados-sensores"; do
            sleep 2
            echo "Waiting..."
          done
          echo "Kafka is ready and topics are created!"
      containers:
      - name: consumer
        image: smart-factory-consumer:latest
        imagePullPolicy: Never
        envFrom:
        - configMapRef:
            name: consumer-extra-config
        env:
        - name: PG_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: POSTGRES_PASSWORD
EOF
        else
            echo "Consumidor extra $i já existe."
        fi
    done
fi

# Limpa os consumidores excedentes
echo "Verificando se há consumidores extras excedentes para remover..."
CURRENT_DEPLOYMENTS=$(kubectl get deployments -l app=consumer -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
for dep in $CURRENT_DEPLOYMENTS; do
    # Trabalha apenas com os deployments 'consumer-extra-X'
    if [[ "$dep" == consumer-extra-* ]]; then
        ID=$(echo "$dep" | grep -oE '[0-9]+$')
        if [ -n "$ID" ] && [ "$ID" -gt "$EXTRA_CONSUMERS" ]; then
            echo "Removendo consumidor extra excedente: $dep..."
            kubectl delete deployment "$dep"
        fi
    fi
done

echo "Concluído!"
