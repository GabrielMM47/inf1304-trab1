#!/bin/bash
# Script para testar o rebalanceamento de consumidores do Kafka

echo "=========================================================="
echo "🔥 INJETANDO FALHA: DERRUBANDO UM CONSUMIDOR KAFKA 🔥"
echo "=========================================================="

# Busca um pod da pool de consumidores
POD_NAME=$(kubectl get pods -l app=consumer -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
    echo "❌ Nenhum consumidor encontrado no cluster."
    exit 1
fi

echo "💀 Alvo travado: $POD_NAME"
echo "Deletando pod..."

kubectl delete pod $POD_NAME

echo "✅ Falha injetada com sucesso!"
echo "O que observar agora:"
echo "1. O Kafka notará que o consumidor saiu do grupo 'sensor-group'."
echo "2. Ele fará o 'Rebalance', enviando as partições órfãs para o sobrevivente."
echo "3. Nenhuma mensagem será perdida durante essa transição."
echo "4. O Kubernetes subirá um novo pod para repor a réplica perdida."
echo "Acompanhe os logs rodando: make logs-consumer"
