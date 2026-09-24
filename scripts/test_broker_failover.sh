#!/bin/bash
# Script para testar a resiliência (failover) de um Broker Kafka no cluster

echo "=========================================================="
echo "🔥 INJETANDO FALHA: DERRUBANDO UM BROKER KAFKA 🔥"
echo "=========================================================="

# Busca o primeiro pod do StatefulSet do Kafka
POD_NAME=$(kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
    echo "❌ Nenhum broker Kafka encontrado no cluster. Eles estão rodando?"
    exit 1
fi

echo "💀 Alvo travado: $POD_NAME"
echo "Executando deleção forçada (simulando perda de energia ou kernel panic)..."

# Deleta o pod abruptamente (grace-period 0 ignora o desligamento seguro)
kubectl delete pod $POD_NAME --force --grace-period=0

echo "✅ Falha injetada com sucesso!"
echo "O que observar agora:"
echo "1. O Zookeeper notará a queda do broker e elegerá o outro broker como líder das partições."
echo "2. O tráfego dos Sensores não deve ser interrompido."
echo "3. O Kubernetes recriará o $POD_NAME mantendo o mesmo disco rígido!"
echo "Para acompanhar a recriação, rode: kubectl get pods -l app=kafka -w"
