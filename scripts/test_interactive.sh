#!/bin/bash
source "$(dirname "$0")/lib_logs.sh"
log_init "interactive"

# Script Interativo de Testes de Estresse, Falha e Auditoria

function press_enter() {
    echo ""
    echo -e "\e[1;32mPressione [ENTER] para continuar...\e[0m"
    read -r
}

function print_header() {
    clear
    log_section "$1"
    log_msg "$2"
}

function wait_for_rebalance() {
    log_msg "Aguardando o Kafka estabilizar (Rebalanceamento)... Isso pode levar alguns segundos."
    KAFKA_POD=$(kafka_pod_vivo)
    
    while true; do
        STATE_OUTPUT=$(kubectl exec -t $KAFKA_POD -- kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group sensor-group --state 2>/dev/null || true)
        
        if echo "$STATE_OUTPUT" | grep -q "Stable"; then
            log_msg "✓ Cluster Kafka estabilizado! (Status: Stable)"
            break
        elif echo "$STATE_OUTPUT" | grep -q "Empty"; then
            log_msg "✓ Cluster Kafka vazio, mas estável! (Status: Empty)"
            break
        fi
        sleep 2
    done
}

# FASE 0: Saúde
print_header "Fase 0: Verificação de Saúde Inicial" "Este é o ponto de partida. Vamos verificar se a infraestrutura está saudável."
log_cmd make status
press_enter

# FASE 1: Elasticidade
print_header "Fase 1: Elasticidade sob Carga (Injeção)" "Vamos forçar uma alta carga (Escalando a máquina-1). O LAG (Fila) do Kafka começará a crescer."
log_cmd make scale-producers
echo ""
log_msg "Entrando no painel dinâmico. Monitorando o LAG por 20 segundos..."
wait_for_rebalance

for i in 1 2 3 4 5; do
    clear
    log_section "MONITORAMENTO DE GARGALO (LAG) - Coletando amostra $i/5"
    log_cmd make kafka-lag
    sleep 5
done

print_header "Fase 1: Elasticidade sob Carga (Resgate)" "Vamos escalar os consumidores para esvaziar a fila acumulada."
log_cmd make scale-consumers
echo ""
log_msg "Entrando no painel dinâmico. Monitorando o alívio do LAG por 20 segundos..."
wait_for_rebalance

for i in 1 2 3 4 5; do
    clear
    log_section "MONITORAMENTO DE ALÍVIO (LAG) - Coletando amostra $i/5"
    log_cmd make kafka-lag
    sleep 5
done

# FASE 2: Rebalanceamento de Consumidores
print_header "Fase 2: Resiliência de Processamento (Consumer Rebalance)" "Iremos abater um consumidor. O Kafka reorganizará as tarefas entre os sobreviventes."
log_cmd ./scripts/test_consumer_rebalance.sh
log_msg "Entrando no painel dinâmico. Monitorando o rebalanceamento por 20 segundos..."
wait_for_rebalance

for i in 1 2 3 4 5; do
    clear
    log_section "MONITORAMENTO DE REBALANCEAMENTO - Coletando amostra $i/5"
    log_msg "Status dos Consumidores:"
    log_cmd kubectl get pods -l app=consumer
    echo ""
    log_cmd make kafka-lag
    sleep 4
done

# FASE 3: Caos na Infraestrutura
print_header "Fase 3: Caos na Infraestrutura (Broker Failover)" "Vamos deletar o broker líder do Kafka e observar a auto-recuperação do cluster KRaft."
log_cmd ./scripts/test_broker_failover.sh
log_msg "Entrando no painel dinâmico. Monitorando o failover por 20 segundos..."
wait_for_rebalance

for i in 1 2 3 4 5; do
    clear
    log_section "MONITORAMENTO DO KAFKA - Coletando amostra $i/5"
    log_cmd kubectl get pods -l app=kafka -o wide
    sleep 4
done

# FASE 4: Auditoria
while true; do
    clear
    log_section "Fase 4: Auditoria de Dados no Banco"
    echo "Digite um número (1-7) para executar uma Query Analítica, ou [ENTER] para finalizar:"
    echo "  1. Auditoria Geral (Últimos Eventos)"
    echo "  2. Ações Críticas (Infraestrutura Auto-Curada)"
    echo "  3. Média de Temperatura (Por Minuto)"
    echo "  4. Ranking dos Sensores (Mais Alertas)"
    echo "  5. Distribuição de Alertas (Por Máquina)"
    echo "  6. Estresse Máximo, Mínimo e Médio Absoluto"
    echo "  7. Volume de Eventos (Por Componente)"
    echo "----------------------------------------------------------"
    
    read -n 1 key
    echo ""
    case $key in
        1)
            log_cmd kubectl exec statefulset/postgres -- psql -U postgres -d fabrica -P pager=off -x -c "SELECT timestamp, component_name, event_type, details FROM event_table ORDER BY timestamp DESC LIMIT 20;"
            ;;
        2)
            log_cmd kubectl exec statefulset/postgres -- psql -U postgres -d fabrica -P pager=off -x -c "SELECT timestamp, component_name, details FROM event_table WHERE event_type = 'MACHINE_KILLED' OR event_type = 'CRITICAL_ALERT_KILL_SENT' ORDER BY timestamp DESC;"
            ;;
        3)
            log_cmd kubectl exec statefulset/postgres -- psql -U postgres -d fabrica -P pager=off -c "SELECT maquina_id, date_trunc('minute', timestamp) AS minuto, AVG(valor) AS temp_media FROM leituras_sensores WHERE tipo_sensor = 'temperatura' GROUP BY maquina_id, minuto ORDER BY minuto DESC;"
            ;;
        4)
            log_cmd kubectl exec statefulset/postgres -- psql -U postgres -d fabrica -P pager=off -c "SELECT tipo_sensor, COUNT(*) as total_alertas FROM leituras_sensores WHERE houve_alerta = true GROUP BY tipo_sensor ORDER BY total_alertas DESC;"
            ;;
        5)
            log_cmd kubectl exec statefulset/postgres -- psql -U postgres -d fabrica -P pager=off -c "SELECT maquina_id, tipo_sensor, COUNT(*) as total_alertas FROM leituras_sensores WHERE houve_alerta = true GROUP BY maquina_id, tipo_sensor ORDER BY total_alertas DESC;"
            ;;
        6)
            log_cmd kubectl exec statefulset/postgres -- psql -U postgres -d fabrica -P pager=off -c "SELECT maquina_id, tipo_sensor, MAX(valor) AS pico_maximo, MIN(valor) AS minimo, ROUND(AVG(valor), 2) AS media, COUNT(*) FILTER (WHERE houve_alerta = true) AS total_alertas FROM leituras_sensores GROUP BY maquina_id, tipo_sensor ORDER BY maquina_id, tipo_sensor;"
            ;;
        7)
            log_cmd kubectl exec statefulset/postgres -- psql -U postgres -d fabrica -P pager=off -c "SELECT component_name, event_type, COUNT(*) as volume FROM event_table GROUP BY component_name, event_type ORDER BY volume DESC;"
            ;;
        "")
            break
            ;;
        *)
            echo "Opção inválida."
            ;;
    esac
    press_enter
done

# FASE 5: Cleanup Automático
print_header "Fase 5: Cleanup Automático" "Restaurando a arquitetura ao seu estado pacífico padrão (Produtores=1, Consumidores=5)..."
log_cmd kubectl scale deployment producer-maquina-1 --replicas=1
log_cmd kubectl scale deployment consumer --replicas=5
echo ""
log_msg "🎉 Validação Arquitetural Finalizada com Sucesso! 🎉"
