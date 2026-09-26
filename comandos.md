# Comandos para Teste do Projeto

Para garantir que todos os requisitos da lista de tarefas (`tarefas.md`) foram cumpridos, você pode executar os comandos abaixo no seu servidor. Cada comando está associado aos requisitos que ele ajuda a avaliar.

### 1. Inicialização e Deploy da Infraestrutura
Para subir a infraestrutura completa, incluindo Kafka, banco de dados e aplicações.

**Comandos:**
```bash
make init
make start-cluster
make deploy
```
**Tarefas avaliadas:**
- **1.1.1. Cluster Kafka:** Brokers rodam em containers/pods.
- **1.2.1. Produtores (Sensores):** Simulados em pods separados.
- **1.4.1. Armazenamento / Log:** Banco de dados sobe junto.
- **4.1. e 4.2. Arquivos e Entregáveis:** Verifica se os arquivos `Makefile` e YAMLs estão presentes e operacionais.

---

### 2. Status do Cluster
Verifica se todos os pods e serviços (Brokers, Produtores, Consumidores, Banco de Dados) subiram corretamente.

**Comando:**
```bash
make status
```
**Tarefas avaliadas:**
- **1.1.1. Cluster Kafka:** Possui 2 ou mais brokers rodando.
- **1.2.1. e 1.3.1. Produtores / Consumidores:** Pods estão operando com as imagens corretas.

---

### 3. Verificar Geração de Dados (Logs)
Avalia se os produtores estão de fato gerando os JSONs e os consumidores os processando.

**Comandos:**
```bash
make logs-producer
make logs-consumer
```
**Tarefas avaliadas:**
- **1.2.2. Produtores (Sensores):** Geram e enviam dados periódicos para o tópico.
- **1.3.3. Consumidores:** Implementam processamento em tempo real (ex. detecção de anomalias/limites).

---

### 4. Fila e Consumer Group
Inspeciona as partições do tópico e o balanceamento dentro do grupo de consumo.

**Comandos:**
```bash
make kafka-lag
# ou
./scripts/inspect_queue.sh
```
**Tarefas avaliadas:**
- **1.1.2. Cluster Kafka:** Tópico com múltiplas partições.
- **1.3.2. Consumidores:** Configurados no mesmo grupo de consumo (*consumer group*) para dividir partições.

---

### 5. Cenário: Failover do Broker
Derruba um dos brokers Kafka para verificar a resiliência do cluster e ver se não há interrupção.

**Comando:**
```bash
./scripts/test_broker_failover.sh
```
**Tarefas avaliadas:**
- **2.1. Failover do Broker:** Cluster continua operando.
- **4.4. Arquivos:** Uso de scripts de simulação de falhas.

---

### 6. Cenário: Rebalanceamento de Consumidores
Pausa ou derruba um consumidor, obrigando o Kafka a repassar sua partição para os consumidores sobreviventes.

**Comando:**
```bash
./scripts/test_consumer_rebalance.sh
```
**Tarefas avaliadas:**
- **2.2. Rebalanceamento de Consumidores:** Outro consumidor do grupo assume a partição órfã.
- **4.4. Arquivos:** Uso de scripts de simulação de falhas.

---

### 7. Cenário: Elasticidade e Carga
Gera uma carga grande de mensagens e/ou adiciona mais consumidores para ver o sistema escalando.

**Comandos:**
```bash
make scale-producers
make scale-consumers
./scripts/test_elasticity.sh
```
**Tarefas avaliadas:**
- **2.3. Elasticidade e Carga:** Demonstração do sistema sob alto volume de sensores.

---

### 8. Armazenamento de Dados e Alertas
Acessa o banco de dados (via linha de comando ou web) para provar que os alertas foram persistidos após o processamento.

**Comandos:**
```bash
# Via terminal do Postgres:
make db-shell
# Via interface web (Adminer):
make db-ui
```
**Tarefas avaliadas:**
- **1.4.1. Armazenamento / Log:** Há um banco de dados registrando os dados processados e/ou alertas gerados.

---

### 9. Testes Interativos Completos
Caso queira executar a demonstração dos cenários de maneira guiada e interativa.

**Comando:**
```bash
make test-all
```
**Tarefas avaliadas:**
- **2.1., 2.2., 2.3. Testes Obrigatórios:** Avalia todos os cenários obrigatórios (Failover, Rebalanceamento e Elasticidade) de uma só vez, servindo também como prova visual.
- **4.5. Arquivos:** Logs de execução (podem ser salvos/anexados a partir dessa execução).
