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
          value: "10.0"
EOF
    else
        echo "Máquina $i já existe."
    fi
done

echo "Concluído!"
