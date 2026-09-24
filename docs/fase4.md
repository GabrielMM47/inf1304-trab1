# Fase 4: Simulação de Falhas e Elasticidade

Esta fase foca em provar a principal hipótese de uma arquitetura de sistemas distribuídos utilizando Kubernetes e Kafka: **Resiliência e Auto-Cura (Self-Healing)**.

Um sistema distribuído bem construído não é aquele que nunca falha, mas sim aquele onde as falhas são esperadas e toleradas sem intervenção humana. Para provarmos que os componentes construídos (Broker Kafka, Consumidor e Controlador) sobrevivem ao caos, criamos rotinas prontas de injeção de falha e estresse de carga.

## 1. Tolerância a Falhas no Barramento (Kafka)

Os brokers de mensageria não estão blindados contra falhas de hardware ou rede.

**Script de Caos:** `scripts/test_broker_failover.sh`

Para testar a resiliência do nosso `StatefulSet`:
1. Rode `chmod +x scripts/test_broker_failover.sh` (caso não seja executável).
2. Execute `./scripts/test_broker_failover.sh`.
3. **O que acontece?** O script manda um comando de deleção forçada (simulando um desligamento na tomada) no pod `kafka-0`.
4. **Resiliência esperada:** 
   - O tráfego de dados gerado pelos Sensores **não cai**, pois o Zookeeper elege o broker `kafka-1` sobrevivente como líder momentâneo das partições.
   - O Kubernetes recria o `kafka-0` mantendo a identidade intacta e conectando o disco persistente (`PVC`) novamente, fazendo com que ele recupere suas mensagens não processadas e retorne ao pool como se nada houvesse acontecido.

## 2. Tolerância a Falhas no Consumo

A lógica de processamento de anomalias (Consumer) também pode sofrer *crashes* de software (ex: memória estourando).

**Script de Caos:** `scripts/test_consumer_rebalance.sh`

1. Execute `./scripts/test_consumer_rebalance.sh`.
2. **O que acontece?** O script "mata" uma das instâncias de processamento de dados (`consumer`).
3. **Resiliência esperada:** 
   - O Kafka Group Coordinator notará rapidamente o ausente no `sensor-group`.
   - Ele ativa um evento chamado **Rebalance**: as partições que eram lidas pela instância que morreu são redirecionadas para as instâncias de processamento sobreviventes no mesmo exato momento.
   - Enquanto a segunda instância segura toda a carga, o K8s percebe que o `Deployment` caiu de 2 para 1 réplica e sobe uma nova instância fresca, invocando um novo rebalanceamento. Zero mensagens perdidas.

## 3. Testes de Escala (Elasticidade)

As fábricas virtuais podem subitamente gerar quantidades massivas de logs. O paralelismo é a nossa arma. 
Utilize o script de elasticidade (ou os atalhos do Makefile) para estressar o sistema:

**Script de Elasticidade:** `scripts/test_elasticity.sh`
- Execute `./scripts/test_elasticity.sh` (ele usa os comandos `kubectl scale`).

O que ele faz:

- **`make scale-producers`**: 
  Sobe agressivamente o número de *pods* simulando a `maquina-1` para gerar telemetria simultânea (o tráfego no Kafka se multiplicará).
- **`make scale-consumers`**: 
  Sobe o processador de anomalias para 4 réplicas. O Kafka automaticamente espalhará as requisições de leitura sobre esses 4 *workers*, garantindo que as filas de mensagens não travem (lag) e as detecções de falhas de máquinas não sofram atraso.

Você pode acompanhar essa elasticidade monitorando a interface do Adminer (descrita na Fase 3) ou usando o comando `make status` para enxergar as réplicas recém-criadas no cluster.
