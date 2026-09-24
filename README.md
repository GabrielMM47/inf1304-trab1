# INF1304 - Trabalho 1 - Arquitetura Produtor-Consumidor

Neste repositório será desenvolvido o projeto da matéria INF1304 - Distribuição e Concorrência da PUC-Rio.

O objetivo é simular uma arquitetura produtor-consumidor em um cluster de Kubernetes.

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
    │   ├── zookeeper.yaml            # (Omit if using Kafka in KRaft mode)
    │   ├── kafka-statefulset.yaml    # 2+ replicas, persistent volumes
    │   ├── kafka-service.yaml        # Headless + internal broker services
    │   └── kafka-init-job.yaml       # Job to create topic 'dados-sensores'
    ├── apps/
    │   ├── configmap.yaml            # Shared broker endpoints, topic name, thresholds
    │   ├── producer-deployment.yaml  # Replicas simulating factory machines
    │   └── consumer-deployment.yaml  # Scalable pods in the same consumer-group
    └── kustomization.yaml            # (Optional) Bundles all manifests together
```

## Documentação

- [Fase 1: Desenvolvimento das Aplicações (Produtor/Consumidor)](docs/fase1.md)