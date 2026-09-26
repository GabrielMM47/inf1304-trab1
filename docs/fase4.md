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
   - O tráfego de dados gerado pelos Sensores **não cai**, pois o controller KRaft elege o broker `kafka-1` sobrevivente como líder momentâneo das partições.
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

- O script escala a `maquina-1` para mais pods (padrão 3) e, em seguida, o Deployment `consumer` para 4 réplicas, coletando amostras do LAG em cada fase.

Atalhos do Makefile para escalar manualmente:

- **`make scale-producers MACHINES=N`**: ajusta o número de máquinas (Deployments `producer-maquina-N`) para N, criando as que faltam e removendo as excedentes (padrão 8).
- **`make scale-machine MAQUINA=M P_REPLICAS=R`**: coloca R pods simultâneos na máquina M.
- **`make scale-consumers C_REPLICAS=N`**: ajusta o total de consumidores para N (padrão 12): até 3 no Deployment `consumer` e o restante como Deployments `consumer-extra-N`, que processam mais rápido. Como o tópico tem 12 partições, consumidores além do 12º ficam sem partição.

Você pode acompanhar essa elasticidade monitorando a interface do Adminer (descrita na Fase 3) ou usando o comando `make status` para enxergar as réplicas recém-criadas no cluster.

## 4. Evidências (logs salvos)

Os três scripts de teste (`test_broker_failover.sh`, `test_consumer_rebalance.sh` e `test_elasticity.sh`) salvam automaticamente a saída em arquivos dentro da pasta `logs/`, com data e hora no nome (ex: `logs/rebalanco_consumidor_20260925_193338.log`). Cada arquivo é dividido em seções (`ANTES`, `FALHA`, `DEPOIS`) e mostra, para cada comando executado, a linha `$ comando` seguida da saída. Um comando que falha ou estoura o tempo limite é **registrado no log**, sem interromper o teste: isso também é resultado do experimento.

| Script | Arquivo gerado | O que fica registrado |
|--------|----------------|-----------------------|
| `test_consumer_rebalance.sh` | `logs/rebalanco_consumidor_*.log` | Consumidores e atribuição de partições (`kafka-consumer-groups --describe`) antes e depois; últimas linhas de log do pod derrubado; logs dos consumidores sobreviventes |
| `test_broker_failover.sh` | `logs/failover_broker_*.log` | Líder, réplicas e ISR de cada partição (`kafka-topics --describe`), LAG do grupo e total de leituras no Postgres, antes e depois; logs dos consumidores na janela da falha |
| `test_elasticity.sh` | `logs/elasticidade_*.log` | Fase 1 (escala os produtores) e Fase 2 (escala os consumidores), cada uma com amostras periódicas do LAG e das partições |

**Como interpretar.**
- *Rebalanço:* compare a coluna `CONSUMER-ID` do ANTES com a do DEPOIS. As partições do consumidor derrubado devem aparecer com outro consumidor, e nenhuma pode ficar com `-`.
- *Failover de broker:* no DEPOIS, toda partição deve ter líder (`Leader` diferente de `-1`) e o total de leituras no banco deve continuar crescendo. Os logs dos produtores não servem de prova, porque o envio do `kafka-python` é assíncrono e imprime "Enviado" mesmo quando a entrega falha.
- *Elasticidade:* na Fase 1 a coluna `LAG` deve subir; na Fase 2 deve cair.

**Configuração.** Tudo é ajustável por variável de ambiente, sem editar os scripts (ex: `ESPERA_REBALANCE_SEG=40 ./scripts/test_consumer_rebalance.sh`). As variáveis de cada script estão descritas no comentário do seu cabeçalho; as comuns (pasta de logs, tópico, grupo, timeout) estão em `scripts/lib_logs.sh`.

**Uso pelo `test_interactive.sh`.** As fases de rebalanço e de failover do script interativo chamam os scripts individuais e, portanto, também geram logs. Além disso, o próprio roteiro interativo grava toda a sua saída em `logs/interactive_*.log`, incluindo as amostras de LAG da fase de elasticidade (feita com `make scale-producers` e `make scale-consumers`). O `./scripts/test_elasticity.sh` gera um log dedicado, `logs/elasticidade_*.log`.

**Versionamento.** A pasta `logs/` não está no `.gitignore`. Os logs que forem usados no relatório devem ser versionados (o enunciado pede os "logs de execução mostrando rebalanço" como entregável); use `git add` apenas nos arquivos desejados.

