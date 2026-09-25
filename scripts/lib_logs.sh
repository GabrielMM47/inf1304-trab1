#!/bin/bash
# Funções compartilhadas pelos scripts de teste para salvar evidências em arquivo.
# Uso (no início do script de teste):
#   source "$(dirname "$0")/lib_logs.sh"
#   log_init "nome-do-teste"
#
# Todas as configurações abaixo podem ser sobrescritas por variável de ambiente,
# por exemplo: LOG_DIR=/tmp/evidencias ./scripts/test_elasticity.sh

LOG_DIR="${LOG_DIR:-logs}"                          # Pasta onde os arquivos de log são criados
KAFKA_TOPIC="${KAFKA_TOPIC:-dados-sensores}"        # Tópico inspecionado
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-sensor-group}"    # Grupo de consumo inspecionado
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}" # Endereço do broker, visto de dentro do pod Kafka
PG_USER="${PG_USER:-postgres}"                      # Usuário do Postgres (para contar leituras)
PG_DB="${PG_DB:-fabrica}"                           # Banco do Postgres
LOG_TAIL_LINHAS="${LOG_TAIL_LINHAS:-30}"            # Quantas linhas finais de log de pod são salvas
CMD_TIMEOUT_SEG="${CMD_TIMEOUT_SEG:-30}"            # Tempo máximo de cada comando (evita travar se o Kafka cair)

# Cria a pasta de logs e define LOG_FILE com data e hora no nome.
# Argumento: nome curto do teste (ex: "rebalanco_consumidor").
log_init() {
    mkdir -p "$LOG_DIR"
    LOG_FILE="$LOG_DIR/${1}_$(date +%Y%m%d_%H%M%S).log"
    : > "$LOG_FILE"
    log_msg "Início do teste '$1'. Arquivo de log: $LOG_FILE"
}

# Escreve uma mensagem com horário na tela e no arquivo de log.
log_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# Escreve um título de seção (separa ANTES / FALHA / DEPOIS no log).
log_section() {
    { echo ""; echo "=================== $* ==================="; } | tee -a "$LOG_FILE"
}

# Executa um comando, mostrando-o (linha "$ comando") e gravando a saída no log.
# Usa `timeout` quando disponível, para que um Kafka indisponível não trave o teste.
# Um código de saída diferente de zero é registrado, mas não interrompe o script:
# o objetivo é documentar o que aconteceu, inclusive as falhas.
log_cmd() {
    echo "\$ $*" | tee -a "$LOG_FILE"
    if command -v timeout >/dev/null 2>&1; then
        timeout "$CMD_TIMEOUT_SEG" "$@" 2>&1 | tee -a "$LOG_FILE"
    else
        "$@" 2>&1 | tee -a "$LOG_FILE"
    fi
    local rc=${PIPESTATUS[0]}
    if [ "$rc" -ne 0 ]; then
        log_msg "(comando terminou com código $rc; 124 significa que estourou CMD_TIMEOUT_SEG=${CMD_TIMEOUT_SEG}s)"
    fi
    return 0
}

# Imprime o nome de um pod Kafka em estado Running, excluindo o pod indicado
# (útil logo após derrubar um broker). Argumento opcional: pod a excluir.
kafka_pod_vivo() {
    kubectl get pods -l app=kafka --field-selector=status.phase=Running \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
        | grep -v -x "${1:-__nenhum__}" | head -n 1
}

# Salva a atribuição de partições do grupo de consumo (coluna CONSUMER-ID) e o LAG.
# Argumento opcional: pod Kafka a evitar (o que acabou de ser derrubado).
kafka_describe_group() {
    local pod
    pod=$(kafka_pod_vivo "$1")
    log_cmd kubectl exec "$pod" -- kafka-consumer-groups \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --group "$KAFKA_GROUP_ID"
}

# Salva o líder, as réplicas (Replicas) e as réplicas em sincronia (Isr) de cada partição.
# Argumento opcional: pod Kafka a evitar.
kafka_describe_topic() {
    local pod
    pod=$(kafka_pod_vivo "$1")
    log_cmd kubectl exec "$pod" -- kafka-topics \
        --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$KAFKA_TOPIC"
}

# Salva quantas leituras existem no banco. Se o número continua crescendo depois
# de uma falha, o sistema segue processando dados.
db_contagem_leituras() {
    log_cmd kubectl exec statefulset/postgres -- psql -U "$PG_USER" -d "$PG_DB" -tA \
        -c "SELECT now() AS momento, count(*) AS total_leituras FROM leituras_sensores;"
}
