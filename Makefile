.PHONY: all init start-cluster build deploy deploy-kafka secrets deploy-apps db-password clean status logs-producer logs-consumer logs-controlador db-shell db-ui scale-producers scale-machine scale-consumers db-reset kafka-lag test-all

# Imagens Docker
PRODUCER_IMG := smart-factory-producer:latest
CONSUMER_IMG := smart-factory-consumer:latest
CONTROLADOR_IMG := smart-factory-controlador:latest

all: build deploy

init:
	@echo "Instalando dependências no Debian (requer senha sudo)..."
	chmod +x scripts/*.sh
	sudo ./scripts/install_deps.sh

start-cluster:
	@echo "Garantindo que o K3s está rodando..."
	sudo systemctl start k3s

build:
	@echo "Construindo imagens Docker localmente..."
	docker build -t $(PRODUCER_IMG) ./src/producer
	docker build -t $(CONSUMER_IMG) ./src/consumer
	docker build -t $(CONTROLADOR_IMG) ./src/controlador
	@echo "Importando as imagens construídas para dentro do cluster K3s..."
	docker save $(PRODUCER_IMG) $(CONSUMER_IMG) $(CONTROLADOR_IMG) | sudo k3s ctr images import -

deploy-kafka:
	@echo "Subindo cluster Kafka em modo KRaft (Sem Zookeeper!)..."
	kubectl apply -f k8s/kafka/kafka-config.yaml
	kubectl apply -f k8s/kafka/kafka-scripts.yaml
	kubectl apply -f k8s/kafka/kafka-service.yaml
	kubectl apply -f k8s/kafka/kafka-statefulset.yaml
	kubectl apply -f k8s/kafka/kafka-init-job.yaml

# Cria o Secret do Postgres com uma senha aleatória, só se ele ainda não existir.
# Não recriar a cada execução: o Postgres só lê a senha quando cria o volume pela primeira vez.
secrets:
	@if kubectl get secret postgres-credentials >/dev/null 2>&1; then \
		echo "Secret postgres-credentials já existe (mantido)."; \
	else \
		echo "Gerando senha aleatória do PostgreSQL e criando o Secret postgres-credentials..."; \
		kubectl create secret generic postgres-credentials \
			--from-literal=POSTGRES_PASSWORD="$$(head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24)"; \
	fi

db-password:
	@kubectl get secret postgres-credentials -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d; echo

DEBUG ?= 0
deploy-apps: secrets
	@echo "Subindo Bancos de Dados e Aplicações..."
	@if [ "$(DEBUG)" = "1" ]; then \
		echo "⚙️  Modo DEBUG ativado (DEBUG=1)! Injetando nos ConfigMaps..."; \
		sed -i 's/DEBUG_MODE: "0"/DEBUG_MODE: "1"/g' k8s/apps/configmap.yaml; \
	else \
		sed -i 's/DEBUG_MODE: "1"/DEBUG_MODE: "0"/g' k8s/apps/configmap.yaml; \
	fi
	kubectl apply -f k8s/apps/configmap.yaml
	kubectl apply -f k8s/apps/postgres.yaml
	kubectl apply -f k8s/apps/redis.yaml
	kubectl apply -f k8s/apps/adminer.yaml
	kubectl apply -f k8s/apps/controlador-deployment.yaml
	kubectl apply -f k8s/apps/producer-deployment.yaml
	kubectl apply -f k8s/apps/consumer-deployment.yaml

deploy: deploy-kafka deploy-apps
	@echo "Deploys submetidos ao cluster. Use 'make status' para acompanhar."

clean:
	@echo "Removendo todos os recursos do Kubernetes..."
	kubectl delete -f k8s/apps/ || true
	kubectl delete $$(kubectl get deployments -o name | grep producer) || true
	kubectl delete -f k8s/kafka/ || true
	@echo "Encerrando túneis de rede ativos..."
	pkill -f "[k]ubectl port-forward" || true

status:
	@echo "Status dos Pods:"
	kubectl get pods
	@echo "\nStatus dos Serviços:"
	kubectl get svc
	@echo "\nStatus dos StatefulSets:"
	kubectl get statefulset

logs-producer:
	kubectl logs -l app=producer -f --max-log-requests=15

kafka-lag:
	@./scripts/inspect_queue.sh

logs-consumer:
	kubectl logs -l app=consumer -f --max-log-requests=15

logs-controlador:
	kubectl logs -l app=controlador -f --max-log-requests=15

db-shell:
	@echo "Acessando o terminal do PostgreSQL (Digite 'exit' para sair)..."
	kubectl exec -it statefulset/postgres -- psql -U postgres -d fabrica

db-reset:
	@echo "Deletando o volume persistente do PostgreSQL para um Deep Clean..."
	kubectl delete pvc pg-data-postgres-0 || true
	@echo "Banco resetado! Na próxima vez que o PostgreSQL subir, ele estará zerado."

db-ui:
	@echo "Abrindo o painel Adminer na porta 8080 (Acesse http://localhost:8080 ou pelo IP remoto na porta 8080)..."
	@echo "O Auto-Login está ativado. Você será conectado automaticamente na base fabrica!"
	kubectl port-forward svc/adminer 8080:8080 --address 0.0.0.0

MACHINES ?= 5
scale-producers:
	@echo "Criando novas máquinas (deployments) se não existirem (Total desejado: $(MACHINES))..."
	@./scripts/scale_producers.sh $(MACHINES)

MAQUINA ?= 1
P_REPLICAS ?= 3
scale-machine:
	@echo "Escalando a máquina-$(MAQUINA) para $(P_REPLICAS) instâncias simultâneas..."
	kubectl scale deployment producer-maquina-$(MAQUINA) --replicas=$(P_REPLICAS)

C_REPLICAS ?= 4
scale-consumers:
	@echo "Escalando o grupo de consumidores para $(C_REPLICAS) réplicas para acelerar o processamento..."
	kubectl scale deployment consumer --replicas=$(C_REPLICAS)

test-all:
	@./scripts/test_interactive.sh
