# Fase 3: Automação e Orquestração (Makefile)

Nesta fase, introduzimos um `Makefile` na raiz do projeto. Seu objetivo é encapsular a complexidade dos comandos operacionais em regras (targets) fáceis de memorizar. Em projetos distribuídos complexos que rodam sobre Kubernetes, recriar ambientes para testes exige digitar dezenas de comandos; o Makefile transforma isso em uma interface simples.

## Targets Disponíveis

### 1. Setup Local de Infraestrutura (`make init` e `make start-cluster`)
Se você clonou o repositório em uma instância de nuvem virgem (ex: Debian) e não tem nada preparado:
- **`make init`**: Roda um script shell automatizado que baixa e instala o **Docker** e o **K3s** (uma distribuição Kubernetes extremamente leve perfeita para VMs e cloud), configurando também os arquivos nativos de acesso ao `kubectl`.
- **`make start-cluster`**: Garante que o serviço do K3s está ligado e operacional.

### 2. Construção de Imagens (`make build`)
Como os manifestos Kubernetes foram configurados com `imagePullPolicy: Never`, o cluster não tentará baixar as imagens da internet (DockerHub), mas buscará do seu registro interno (containerd do K3s).
O comando `make build`:
- Acessa as pastas `src/producer`, `src/consumer` e `src/controlador` e aciona os processos de `docker build` no daemon local.
- Compacta as 3 imagens geradas e as **importa para dentro do cluster K3s** (`k3s ctr images import`), eliminando inteiramente a necessidade de exportar imagens para um *container registry* na nuvem.

### 2. Implantação Automática (`make deploy`)
O `make deploy` atua como um orquestrador mestre, chamando internamente dois outros targets:
- **`make deploy-kafka`**: Aplica primeiro os serviços vitais de infraestrutura de mensageria (ConfigMaps `kafka-config` e `kafka-scripts` -> Services -> StatefulSet Kafka -> Job de inicialização de tópicos).
- **`make deploy-apps`**: Em seguida (depois de rodar `make secrets`, que cria o Secret com a senha do Postgres se ele ainda não existir), injeta as configurações (`ConfigMaps`), sobe as persistências de dados (`postgres.yaml` e `redis.yaml`) e finaliza com os microserviços (Controlador, Produtor e Consumidor).

*Nota: Em sistemas distribuídos, a ordem de *apply* dos YAMLs nem sempre dita a ordem de inicialização devido à natureza assíncrona do Kubernetes, mas submetê-los de maneira organizada facilita o troubleshooting.*

### 3. Monitoramento Simplificado (`make status`, `make logs-*`)
Evita a digitação manual de comandos extensos no Kubectl:
- **`make status`**: Mostra uma visão geral consolidada do cluster, listando Pods, Services e StatefulSets.
- **`make logs-consumer` / `make logs-producer` / `make logs-controlador`**: Faz o *tailing* (`-f`) direto nos logs daquela categoria inteira de aplicação baseada em *labels*, permitindo debugar os microsserviços de forma unificada.

### 3.1. Senha do banco (`make secrets`, `make db-password`)
- **`make secrets`**: Cria o Secret `postgres-credentials` com uma senha aleatória **somente se ele não existir**. Roda sozinho dentro de `make deploy-apps`.
- **`make db-password`**: Imprime a senha atual, lida do Secret (exige acesso ao cluster). Útil para clientes SQL externos; o Adminer e o `make db-shell` não precisam dela.

### 4. *Teardown* Rápido (`make clean`)
Deleta de uma só vez absolutamente todos os artefatos (incluindo discos de volume persistente, serviços e pods) das pastas `k8s/apps/` e `k8s/kafka/`, limpando o cluster e reciclando completamente o ambiente para um estado zerado.
