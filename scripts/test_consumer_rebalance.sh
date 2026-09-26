#!/bin/bash
# Redireciona a saída para um arquivo além do terminal
exec > >(tee -a tests_execution.log) 2>&1

# Script para testar o rebalanceamento de consumidores do Kafka.
# Deleta um pod consumidor e salva em logs/ a atribuição de partições ANTES e DEPOIS.
#
# Variáveis opcionais:
#   ESPERA_REBALANCE_SEG  segundos de espera após a falha, para o Kafka rebalancear (padrão 20)
#   (demais variáveis: ver scripts/lib_logs.sh)

source "$(dirname "$0")/lib_logs.sh"
ESPERA_REBALANCE_SEG="${ESPERA_REBALANCE_SEG:-20}"

log_init "rebalanco_consumidor"

echo "=========================================================="
echo "🔥 INJETANDO FALHA: DERRUBANDO UM CONSUMIDOR KAFKA 🔥"
echo "=========================================================="

# Busca um pod da pool de consumidores
POD_NAME=$(kubectl get pods -l app=consumer -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
    log_msg "❌ Nenhum consumidor encontrado no cluster."
    exit 1
fi

log_section "ANTES: consumidores e partições atribuídas a cada um"
log_cmd kubectl get pods -l app=consumer -o wide
kafka_describe_group

log_section "ÚLTIMAS LINHAS DE LOG DO CONSUMIDOR QUE SERÁ DERRUBADO ($POD_NAME)"
log_cmd kubectl logs "$POD_NAME" --tail="$LOG_TAIL_LINHAS" --timestamps

log_section "FALHA: deletando o pod $POD_NAME"
log_msg "💀 Alvo travado: $POD_NAME"
log_cmd kubectl delete pod "$POD_NAME"

log_msg "Aguardando ${ESPERA_REBALANCE_SEG}s para o Kafka detectar a saída e rebalancear..."
sleep "$ESPERA_REBALANCE_SEG"

log_section "DEPOIS: consumidores e partições (compare com o ANTES)"
log_cmd kubectl get pods -l app=consumer -o wide
kafka_describe_group

log_section "LOGS DOS CONSUMIDORES EM EXECUÇÃO (últimas $LOG_TAIL_LINHAS linhas de cada)"
log_cmd kubectl logs -l app=consumer --prefix --timestamps --tail="$LOG_TAIL_LINHAS" --max-log-requests=10

log_msg "✅ Falha injetada e evidências salvas em: $LOG_FILE"
echo "O que observar:"
echo "1. Na tabela do DEPOIS, as partições do consumidor derrubado passaram a ter outro CONSUMER-ID."
echo "2. Nenhuma partição deve ficar sem consumidor (CONSUMER-ID '-')."
echo "3. O Kubernetes sobe um novo pod para repor a réplica, causando um novo rebalanço."
