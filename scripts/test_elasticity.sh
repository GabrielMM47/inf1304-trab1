#!/bin/bash

# Script para demonstrar a elasticidade sob carga.
# Fase 1: escala os produtores (o LAG do Kafka deve subir).
# Fase 2: escala os consumidores (o LAG deve cair).
# Em cada fase coleta amostras do LAG e salva tudo em logs/.
#
# Variáveis opcionais:
#   PRODUCER_DEPLOYMENT / PRODUCER_REPLICAS   deployment de produtores e réplicas alvo (producer-maquina-1 / 3)
#   CONSUMER_DEPLOYMENT / CONSUMER_REPLICAS   deployment de consumidores e réplicas alvo (consumer / 4)
#   AMOSTRAS               número de amostras de LAG por fase (padrão 6)
#   INTERVALO_AMOSTRA_SEG  segundos entre amostras (padrão 10)
#   (demais variáveis: ver scripts/lib_logs.sh)

source "$(dirname "$0")/lib_logs.sh"

PRODUCER_DEPLOYMENT="${PRODUCER_DEPLOYMENT:-producer-maquina-1}"
PRODUCER_REPLICAS="${PRODUCER_REPLICAS:-3}"
CONSUMER_DEPLOYMENT="${CONSUMER_DEPLOYMENT:-consumer}"
CONSUMER_REPLICAS="${CONSUMER_REPLICAS:-4}"
AMOSTRAS="${AMOSTRAS:-6}"
INTERVALO_AMOSTRA_SEG="${INTERVALO_AMOSTRA_SEG:-10}"

# Coleta AMOSTRAS medições do LAG, uma a cada INTERVALO_AMOSTRA_SEG.
coletar_amostras() {
    for i in $(seq 1 "$AMOSTRAS"); do
        log_msg "Amostra $i/$AMOSTRAS"
        kafka_describe_group
        [ "$i" -lt "$AMOSTRAS" ] && sleep "$INTERVALO_AMOSTRA_SEG"
    done
}

log_init "elasticidade"

echo "=========================================================="
echo "📈 TESTE DE ELASTICIDADE E ESCALONAMENTO 📈"
echo "=========================================================="

log_section "ANTES: estado inicial"
log_cmd kubectl get pods -l app=producer
log_cmd kubectl get pods -l app=consumer
kafka_describe_topic
kafka_describe_group

log_section "FASE 1: escalando produtores ($PRODUCER_DEPLOYMENT -> $PRODUCER_REPLICAS réplicas) para gerar backlog"
log_cmd kubectl scale deployment "$PRODUCER_DEPLOYMENT" --replicas="$PRODUCER_REPLICAS"
coletar_amostras

log_section "FASE 2: escalando consumidores ($CONSUMER_DEPLOYMENT -> $CONSUMER_REPLICAS réplicas) para esvaziar a fila"
log_cmd kubectl scale deployment "$CONSUMER_DEPLOYMENT" --replicas="$CONSUMER_REPLICAS"
coletar_amostras

log_section "DEPOIS: estado final"
log_cmd kubectl get pods -l app=producer
log_cmd kubectl get pods -l app=consumer
kafka_describe_group

log_msg "✅ Elasticidade demonstrada e evidências salvas em: $LOG_FILE"
echo "O que observar:"
echo "1. Na Fase 1, a coluna LAG aumenta (mais mensagens produzidas do que consumidas)."
echo "2. Na Fase 2, o LAG diminui e as partições são redistribuídas entre os novos consumidores."
echo "3. Com $CONSUMER_REPLICAS consumidores e poucas partições, os excedentes ficam ociosos (o máximo de consumidores úteis é o número de partições)."
echo "Para voltar ao estado original: kubectl scale deployment $PRODUCER_DEPLOYMENT --replicas=1 && kubectl scale deployment $CONSUMER_DEPLOYMENT --replicas=2"
