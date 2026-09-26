#!/bin/bash

# Quantidade desejada de máquinas distintas
MACHINES=${1:-3}

echo "Garantindo que existem $MACHINES máquinas (deployments)..."

for i in $(seq 1 $MACHINES); do
    # Verifica se a máquina já existe no Kubernetes
    if ! kubectl get deployment producer-maquina-$i > /dev/null 2>&1; then
        echo "Criando producer-maquina-$i..."
        cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: producer-maquina-$i
  labels:
    app: producer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: producer
      sensor-id: maquina-$i
  template:
    metadata:
      labels:
        app: producer
        sensor-id: maquina-$i
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
      - name: producer
        image: smart-factory-producer:latest
        imagePullPolicy: Never
        envFrom:
        - configMapRef:
            name: producer-config
        env:
        - name: MAQUINA_ID
          value: "maquina-$i"
        - name: PG_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: POSTGRES_PASSWORD
        - name: GENERATION_INTERVAL
          value: "1.0"
EOF
    else
        echo "Máquina $i já existe."
    fi
done

# Limpa as máquinas excedentes
echo "Verificando se há máquinas excedentes para remover..."
CURRENT_DEPLOYMENTS=$(kubectl get deployments -l app=producer -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
for dep in $CURRENT_DEPLOYMENTS; do
    # Extrai o número do nome (ex: producer-maquina-4 -> 4)
    ID=$(echo "$dep" | grep -oE '[0-9]+$')
    if [ -n "$ID" ] && [ "$ID" -gt "$MACHINES" ]; then
        echo "Removendo máquina excedente: $dep..."
        kubectl delete deployment "$dep"
    fi
done

echo "Concluído!"
