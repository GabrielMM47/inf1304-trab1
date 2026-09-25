# INF1304 - Trabalho 1 - Arquitetura Produtor-Consumidor

Neste repositório será desenvolvido o projeto da matéria INF1304 - Distribuição e Concorrência da PUC-Rio.

O objetivo é simular uma arquitetura produtor-consumidor em um cluster de Kubernetes.

## Como Rodar (Quick Start)

Se você está em um ambiente limpo (ex: Debian) e quer testar a arquitetura inteira do zero, siga os passos providenciados pelo nosso orquestrador (`Makefile`):

1. **Instalar dependências vitais (Docker e K3s):**
   ```bash
   make init
   ```
2. **Ligar o cluster Kubernetes Local:**
   ```bash
   make start-cluster
   ```
3. **Construir as imagens e fazer o Deploy automático (Infra + Aplicações):**
   ```bash
   make all
   ```
4. **Acompanhar o cluster subindo e consultar os logs:**
   ```bash
   make status
   make logs-consumer
   ```
   *(O `make all` também cria, na primeira vez, o Secret com a senha aleatória do Postgres. Para consultá-la: `make db-password`.)*
5. *(Opcional)* **Acessar o banco de dados via interface visual:**
   ```bash
   make db-ui
   ```
   *(Abra `http://localhost:8080` no navegador - O login na base de dados é feito magicamente de forma automática).*

---

## Grupo

| Nome          | Matricula |
| ------------- | --------- |
| Eduardo Eugênio de Souza | 2310822 |
| Gabriel Augusto Gedey | 2210508 |
| Gabriel Martins Mendes | 231XXXX |

## Estrutura de pastas
```
smart-factory-kafka/
├── Makefile                          # Build, deploy, run tests, teardown
├── README.md                         # Setup & run instructions (Deliverable)
├── docs/
│   └── report.md                     # Architecture, test outcomes, logs (Deliverable)
├── scripts/
│   ├── init_topic.sh                 # Creates topic with partitions & replication
│   ├── test_broker_failover.sh       # Kills a broker pod and verifies survival
│   ├── test_consumer_rebalance.sh    # Kills a consumer and captures rebalance
│   └── test_elasticity.sh            # Scales producers/consumers up and down
├── src/
│   ├── producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt          # kafka-python or confluent-kafka
│   │   └── sensor.py                 # Telemetry generator with Docstrings
│   └── consumer/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── processor.py              # Anomaly detection & partition logging
└── k8s/
    ├── kafka/
    │   ├── kafka-config.yaml         # ConfigMap com o KAFKA_CLUSTER_ID
    │   ├── kafka-scripts.yaml        # ConfigMap com o setup-kraft.sh (Kafka em modo KRaft, sem Zookeeper)
    │   ├── kafka-statefulset.yaml    # 2+ replicas, persistent volumes
    │   ├── kafka-service.yaml        # Headless + internal broker services
    │   └── kafka-init-job.yaml       # Job to create topic 'dados-sensores'
    ├── apps/
    │   ├── configmap.yaml            # Shared broker endpoints, topic name, thresholds
    │   ├── producer-deployment.yaml  # Replicas simulating factory machines
    │   └── consumer-deployment.yaml  # Scalable pods in the same consumer-group
    └── kustomization.yaml            # (Optional) Bundles all manifests together
```

## Fases do Projeto

- **Fase 1 (Aplicações):** Desenvolvimento dos microsserviços em Python (Sensores, Processadores e Controlador) com suporte a banco de dados e empacotamento em Docker.
- **Fase 2 (Infraestrutura):** Configuração dos manifestos YAML no Kubernetes, incluindo banco de dados, brokers Kafka e RBAC de segurança.
- **Fase 3 (Automação):** Criação de um Makefile para padronizar e orquestrar as implantações no cluster.
- **Fase 4 (Resiliência):** Simulação de falhas e de testes de elasticidade na infraestrutura via scripts shell.
- **Fase 5 (Documentação):** Elaboração do relatório técnico e documentação final do projeto.

## Documentação

- [Fase 1: Desenvolvimento das Aplicações (Produtor/Consumidor)](docs/fase1.md)
- [Fase 2: Configuração da Infraestrutura (Kubernetes)](docs/fase2.md)
- [Fase 3: Automação e Orquestração (Makefile)](docs/fase3.md)
- [Visualização e Acesso ao Banco de Dados (Queries Úteis)](docs/banco-de-dados.md)
- [Fase 4: Simulação de Falhas e Elasticidade](docs/fase4.md)