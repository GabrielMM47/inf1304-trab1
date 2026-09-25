# Fase 2: Configuração da Infraestrutura (Kubernetes)

Esta seção detalha os manifestos YAML responsáveis por subir toda a arquitetura de sistemas distribuídos dentro de um cluster Kubernetes.

## 1. O Padrão Arquitetural: StatefulSet vs Deployment

A forma mais clara de entender as decisões de infraestrutura desta fase é compreender a diferença entre `StatefulSet` e `Deployment`.

Em um **Deployment** (usado pelos seus sensores, processadores e controladores), os pods são como clones descartáveis. O Kubernetes cria pods com nomes aleatórios (ex: `sensor-7f8b9d-xyz`). Se um pod morre, outro com um nome totalmente novo nasce no lugar. Eles não guardam estado e a identidade individual não importa.

O **StatefulSet**, por outro lado, é desenhado para componentes que não podem perder a identidade nem a memória (o "estado"), como os brokers do **Kafka** e o banco de dados **PostgreSQL**. Ele fornece três garantias essenciais que um Deployment não consegue entregar:

* **Identidade de Rede Fixa e Sequencial:** O `StatefulSet` batiza os pods com nomes ordenados e previsíveis, como `kafka-0` e `kafka-1`. Se o `kafka-0` sofrer uma falha crítica, o Kubernetes garantirá que o pod substituto nasça com o exato nome `kafka-0`. Isso é vital para o protocolo do Kafka, pois os clientes precisam se conectar diretamente ao broker específico que lidera uma partição.
* **Armazenamento Vinculado à Identidade (Discos Persistentes):** Esta é a principal vantagem. Quando o `kafka-0` nasce, o Kubernetes aloca um disco virtual (`PVC`) exclusivo para ele. Se a máquina física for reiniciada, o Kubernetes sobe o novo `kafka-0` em outro servidor e, automaticamente, desconecta o disco antigo e o pluga no novo pod. O banco de dados ou o broker acorda com todos os logs e mensagens intactos no disco.
* **Ordem Estrita de Execução:** Em sistemas distribuídos, subir tudo de uma vez gera caos de rede. O `StatefulSet` inicializa o `kafka-0`; apenas quando este estiver 100% saudável, ele cria o `kafka-1`. Isso permite que clusters com múltiplos brokers se formem e elejam líderes de forma organizada.

Sem o `StatefulSet`, após uma falha simulada ou real, os seus brokers Kafka voltariam com nomes e IPs aleatórios e desvinculados dos discos originais, corrompendo as partições dos tópicos e inviabilizando a tolerância a falhas exigida pelo projeto.

## 2. O Cluster Kafka (`k8s/kafka/`)

O Kafka é o coração do barramento de eventos.

- **`kafka-scripts.yaml`**: Um `ConfigMap` com o script `setup-kraft.sh`, executado na inicialização de cada broker. O Kafka roda em **modo KRaft** (sem Zookeeper): cada pod atua como `broker` e `controller` ao mesmo tempo, e os próprios controllers formam um quórum que gerencia os metadados do cluster e a eleição de líderes de partição. O script deriva o `node.id` do nome do pod (`kafka-0` → `0`), monta o arquivo de configuração e formata o armazenamento antes de iniciar o broker.
- **`kafka-statefulset.yaml`**: Sobem-se múltiplas instâncias (`replicas: 2`) usando `StatefulSet`. Isso garante que as identidades dos brokers sejam persistentes (ex: `kafka-0` e `kafka-1`), o que é essencial para estabilidade do barramento.
- **`kafka-service.yaml`**: Um `Service Headless` (`clusterIP: None`) foi configurado para permitir a comunicação e resolução de DNS direta entre as réplicas do broker e os clientes produtores/consumidores.
- **`kafka-init-job.yaml`**: Um `Job` simples executado uma única vez que aguarda o broker subir e cria os tópicos `dados-sensores` e `comandos-fabrica` com múltiplas partições, preparando o terreno antes que os microsserviços conectem.

## 3. Bancos de Dados (`postgres.yaml` e `redis.yaml`)

- **`postgres.yaml`**: O banco de dados PostgreSQL roda como um `StatefulSet`. Diferente de aplicações puramente escaláveis (stateless), exige um volume persistente garantido pelo Kubernetes utilizando um `VolumeClaimTemplate` injetando um `PVC (PersistentVolumeClaim)` que atrela um disco físico/virtual ao banco. Se o pod reiniciar, os logs e eventos de auditoria não são perdidos.
- **`redis.yaml`**: O Redis roda como um `Deployment` simples em memória. Ele atua como um armazenamento distribuído ultra-rápido para os processadores compartilharem o histórico de falhas das máquinas em milissegundos antes de lançarem o alerta crítico.

## 4. As Aplicações (`k8s/apps/`)

- **`configmap.yaml`**: Centraliza todas as variáveis de ambiente necessárias para evitar *hard-coding* (hosts, portas, tópicos, limiares de alertas).
- **`producer-deployment.yaml`**: Implanta duas "máquinas" virtuais distintas (`maquina-1` e `maquina-2`). Usa deployments independentes com env vars específicas para provar a escalabilidade e paralelismo da injeção de dados.
- **`consumer-deployment.yaml`**: Instancia os processadores de anomalia, com `replicas: 2`. O Kafka distribui automaticamente a carga (as partições dos tópicos) entre elas devido à inscrição no mesmo `KAFKA_GROUP_ID`.
- **`controlador-deployment.yaml`**: Contém tanto o `Deployment` do microsserviço Controlador quanto a configuração de segurança **RBAC**. É criada uma `ServiceAccount`, uma `Role` que permite deletar `pods`, e um `RoleBinding` atrelando-a ao pod, conferindo a ele permissões de _In-Cluster Authentication_.

## Resumo da Arquitetura de Redes

Todas as aplicações na pasta `apps/` referenciam internamente os domínios locais providos pelos `Services` do cluster (ex: `kafka:9092`, `postgres:5432`) através da extração destas variáveis via `ConfigMap`, viabilizando o desacoplamento nativo da arquitetura.
