#!/bin/bash
# Script para inspecionar o atraso (LAG) e a fila de mensagens do Kafka

echo "=========================================================="
echo "📊 INSPECIONANDO A FILA DE MENSAGENS DO KAFKA 📊"
echo "=========================================================="

echo "Verificando o status de processamento do grupo 'sensor-group'..."
echo "A coluna 'LAG' indica quantas mensagens estão paradas na fila aguardando processamento."
echo ""

kubectl exec -t kafka-0 -- kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group sensor-group

echo ""
echo "✅ Inspeção concluída!"
echo "Dica: Execute este comando repetidas vezes durante o teste de elasticidade (ou falha) para ver a fila aumentar, e depois diminuir quando mais consumidores subirem."
