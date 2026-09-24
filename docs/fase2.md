# Fase 2: Configuração da Infraestrutura (Kubernetes)

Esta seção detalha os manifestos YAML responsáveis por subir toda a arquitetura de sistemas distribuídos dentro de um cluster Kubernetes.

## 1. O Cluster Kafka (`k8s/kafka/`)

O Kafka é o coração do barramento de eventos.

- **`zookeper.yaml`**: Utilizado para gerenciar a eleição de líderes e metadados dos brokers Kafka.
- **`kafka-statefulset.yaml`**: Sobem-se múltiplas instâncias (`replicas: 2`) usando `StatefulSet`. Isso garante que as identidades dos brokers sejam persistentes (ex: `kafka-0` e `kafka-1`), o que é essencial para estabilidade do barramento.
- **`kafka-service.yaml`**: Um `Service Headless` (`clusterIP: None`) foi configurado para permitir a comunicação e resolução de DNS direta entre as réplicas do broker e os clientes produtores/consumidores.
- **`kafka-init-job.yaml`**: Um `Job` simples executado uma única vez que aguarda o broker subir e cria os tópicos `dados-sensores` e `comandos-fabrica` com múltiplas partições, preparando o terreno antes que os microsserviços conectem.

## 2. Banco de Dados (`k8s/apps/postgres.yaml`)

O banco de dados PostgreSQL roda como um `StatefulSet`. Diferente de aplicações puramente escaláveis (stateless), o banco de dados exige um volume persistente garantido pelo Kubernetes:
- Utiliza um `VolumeClaimTemplate` injetando um `PVC (PersistentVolumeClaim)` que atrela um disco físico/virtual ao banco. Se o pod reiniciar ou for movido de nó, os logs e eventos de auditoria não são perdidos.

## 3. As Aplicações (`k8s/apps/`)

- **`configmap.yaml`**: Centraliza todas as variáveis de ambiente necessárias para evitar *hard-coding* (hosts, portas, tópicos, limiares de alertas).
- **`producer-deployment.yaml`**: Implanta duas "máquinas" virtuais distintas (`maquina-1` e `maquina-2`). Usa deployments independentes com env vars específicas para provar a escalabilidade e paralelismo da injeção de dados.
- **`consumer-deployment.yaml`**: Instancia os processadores de anomalia, com `replicas: 2`. O Kafka distribui automaticamente a carga (as partições dos tópicos) entre elas devido à inscrição no mesmo `KAFKA_GROUP_ID`.
- **`controlador-deployment.yaml`**: Contém tanto o `Deployment` do microsserviço Controlador quanto a configuração de segurança **RBAC**. É criada uma `ServiceAccount`, uma `Role` que permite deletar `pods`, e um `RoleBinding` atrelando-a ao pod, conferindo a ele permissões de _In-Cluster Authentication_.

## Resumo da Arquitetura de Redes

Todas as aplicações na pasta `apps/` referenciam internamente os domínios locais providos pelos `Services` do cluster (ex: `kafka:9092`, `postgres:5432`) através da extração destas variáveis via `ConfigMap`, viabilizando o desacoplamento nativo da arquitetura.
