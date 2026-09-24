#!/bin/bash
# Script alternativo para inicializar tópicos manualmente (backup para o kafka-init-job)

echo "=========================================================="
echo "📝 INICIALIZANDO TÓPICOS KAFKA 📝"
echo "=========================================================="

echo "Criando tópico 'dados-sensores'..."
kubectl exec -it kafka-0 -- kafka-topics.sh --create --if-not-exists --topic dados-sensores --partitions 3 --replication-factor 2 --bootstrap-server localhost:9092

echo "Criando tópico 'comandos-fabrica'..."
kubectl exec -it kafka-0 -- kafka-topics.sh --create --if-not-exists --topic comandos-fabrica --partitions 3 --replication-factor 2 --bootstrap-server localhost:9092

echo "✅ Tópicos criados ou já existentes!"
