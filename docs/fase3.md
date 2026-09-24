# Fase 3: Automação e Orquestração (Makefile)

Nesta fase, introduzimos um `Makefile` na raiz do projeto. Seu objetivo é encapsular a complexidade dos comandos operacionais em regras (targets) fáceis de memorizar. Em projetos distribuídos complexos que rodam sobre Kubernetes, recriar ambientes para testes exige digitar dezenas de comandos; o Makefile transforma isso em uma interface simples.

## Targets Disponíveis

### 1. Construção de Imagens (`make build`)
Como os manifestos Kubernetes foram configurados com `imagePullPolicy: Never`, o cluster K8s local (ex: Docker Desktop ou Minikube) busca as imagens no repositório local do próprio Docker (sem necessidade de um *container registry* na nuvem). 
O comando `make build`:
- Acessa as pastas `src/producer`, `src/consumer` e `src/controlador`.
- Aciona o `docker build` de cada um de forma automatizada.
- Etiqueta (*tags*) as imagens exatamente como referenciado nos YAMLs (ex: `smart-factory-producer:latest`).

### 2. Implantação Automática (`make deploy`)
O `make deploy` atua como um orquestrador mestre, chamando internamente dois outros targets:
- **`make deploy-kafka`**: Aplica primeiro os serviços vitais de infraestrutura de mensageria (Zookeeper -> Service Headless -> StatefulSet Kafka -> Job de inicialização de tópicos).
- **`make deploy-apps`**: Em seguida, injeta as configurações (`ConfigMaps`), sobe as persistências de dados (`postgres.yaml` e `redis.yaml`) e finaliza com os microserviços (Controlador, Produtor e Consumidor).

*Nota: Em sistemas distribuídos, a ordem de *apply* dos YAMLs nem sempre dita a ordem de inicialização devido à natureza assíncrona do Kubernetes, mas submetê-los de maneira organizada facilita o troubleshooting.*

### 3. Monitoramento Simplificado (`make status`, `make logs-*`)
Evita a digitação manual de comandos extensos no Kubectl:
- **`make status`**: Mostra uma visão geral consolidada do cluster, listando Pods, Services e StatefulSets.
- **`make logs-consumer` / `make logs-producer` / `make logs-controlador`**: Faz o *tailing* (`-f`) direto nos logs daquela categoria inteira de aplicação baseada em *labels*, permitindo debugar os microsserviços de forma unificada.

### 4. *Teardown* Rápido (`make clean`)
Deleta de uma só vez absolutamente todos os artefatos (incluindo discos de volume persistente, serviços e pods) das pastas `k8s/apps/` e `k8s/kafka/`, limpando o cluster e reciclando completamente o ambiente para um estado zerado.
