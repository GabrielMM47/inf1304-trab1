#!/bin/bash
# Script para inspecionar o atraso (LAG) e a fila de mensagens do Kafka

echo "=========================================================="
echo "📊 INSPECIONANDO A FILA DE MENSAGENS DO KAFKA 📊"
echo "=========================================================="

echo "Verificando o status de processamento do grupo 'sensor-group'..."
echo "A coluna 'LAG' indica quantas mensagens estão paradas na fila aguardando processamento."
echo ""

KAFKA_POD=$(kubectl get pods -l app=kafka -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' | awk '{print $1}')

if [ -z "$KAFKA_POD" ]; then
    echo "Nenhum broker Kafka está rodando no momento!"
else
    kubectl exec -t $KAFKA_POD -- kafka-consumer-groups --bootstrap-server kafka-0.kafka-headless:9092,kafka-1.kafka-headless:9092,kafka-2.kafka-headless:9092 --describe --group sensor-group
fi

echo ""
echo "✅ Inspeção concluída!"
echo "Dica: Execute este comando repetidas vezes durante o teste de elasticidade (ou falha) para ver a fila aumentar, e depois diminuir quando mais consumidores subirem."
