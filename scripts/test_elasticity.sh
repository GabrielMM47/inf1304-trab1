#!/bin/bash
# Script para demonstrar a elasticidade sob carga

echo "=========================================================="
echo "📈 TESTE DE ELASTICIDADE E ESCALONAMENTO 📈"
echo "=========================================================="

echo "Escalando a máquina-1 (Produtores) para 3 réplicas para aumentar a taxa de geração de eventos e gerar backlog..."
kubectl scale deployment producer-maquina-1 --replicas=3

echo "Escalando os processadores de anomalias (Consumidores) para 4 réplicas para acelerar o consumo em paralelo..."
kubectl scale deployment consumer --replicas=4

echo "✅ Elasticidade engatilhada com sucesso!"
echo "Verifique os Pods criados com: make status"
