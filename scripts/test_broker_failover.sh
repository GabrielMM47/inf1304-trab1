#!/bin/bash
# Script para testar a resiliência (failover) de um Broker Kafka no cluster.
# Deleta um pod Kafka e salva em logs/ o estado do tópico, do grupo e do banco ANTES e DEPOIS.
#
# Variáveis opcionais:
#   ESPERA_FAILOVER_SEG  segundos de espera após a falha antes de coletar o DEPOIS (padrão 30)
#   (demais variáveis: ver scripts/lib_logs.sh)
#
# Sobre as evidências: os logs dos produtores NÃO são usados como prova, porque o envio do
# kafka-python é assíncrono e imprime "Enviado" mesmo quando a entrega falha. As provas são:
# líder/ISR das partições, LAG do grupo, contagem de leituras no banco e logs dos consumidores.

source "$(dirname "$0")/lib_logs.sh"
ESPERA_FAILOVER_SEG="${ESPERA_FAILOVER_SEG:-30}"

log_init "failover_broker"

echo "=========================================================="
echo "🔥 INJETANDO FALHA: DERRUBANDO UM BROKER KAFKA 🔥"
echo "=========================================================="

# Busca o primeiro pod do StatefulSet do Kafka
POD_NAME=$(kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
    log_msg "❌ Nenhum broker Kafka encontrado no cluster. Eles estão rodando?"
    exit 1
fi

log_section "ANTES: brokers, partições (líder/réplicas/ISR), grupo de consumo e banco"
log_cmd kubectl get pods -l app=kafka -o wide
kafka_describe_topic
kafka_describe_group
db_contagem_leituras

log_section "FALHA: deletando à força o broker $POD_NAME"
log_msg "💀 Alvo travado: $POD_NAME"
log_msg "Executando deleção forçada (simulando perda de energia ou kernel panic)..."
# grace-period 0 ignora o desligamento seguro
log_cmd kubectl delete pod "$POD_NAME" --force --grace-period=0

log_msg "Aguardando ${ESPERA_FAILOVER_SEG}s para o cluster reagir..."
sleep "$ESPERA_FAILOVER_SEG"

log_section "DEPOIS: consultas feitas no broker sobrevivente (evitando $POD_NAME)"
log_cmd kubectl get pods -l app=kafka -o wide
kafka_describe_topic "$POD_NAME"
kafka_describe_group "$POD_NAME"
db_contagem_leituras

log_section "LOGS DOS CONSUMIDORES NA JANELA DA FALHA (últimos ${ESPERA_FAILOVER_SEG}s)"
log_cmd kubectl logs -l app=consumer --prefix --timestamps --since="${ESPERA_FAILOVER_SEG}s" --tail="$LOG_TAIL_LINHAS" --max-log-requests=10

log_msg "✅ Falha injetada e evidências salvas em: $LOG_FILE"
echo "O que observar:"
echo "1. No DEPOIS, o tópico ainda tem líder para todas as partições (Leader diferente de -1) e o Isr foi reduzido."
echo "2. O total de leituras no banco continua aumentando entre ANTES e DEPOIS."
echo "3. O Kubernetes recria o $POD_NAME. Acompanhe com: kubectl get pods -l app=kafka -w"
echo "Se algum comando acima falhou ou estourou o tempo limite, isso também é resultado do teste e está registrado no log."
