.PHONY: all init start-cluster build deploy deploy-kafka deploy-apps clean status logs-producer logs-consumer logs-controlador db-shell db-ui

# Imagens Docker
PRODUCER_IMG := smart-factory-producer:latest
CONSUMER_IMG := smart-factory-consumer:latest
CONTROLADOR_IMG := smart-factory-controlador:latest

all: build deploy

init:
	@echo "Instalando dependências no Debian (requer senha sudo)..."
	chmod +x scripts/install_deps.sh
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
	@echo "Subindo cluster Kafka e Zookeeper..."
	kubectl apply -f k8s/kafka/zookeeper.yaml
	kubectl apply -f k8s/kafka/kafka-service.yaml
	kubectl apply -f k8s/kafka/kafka-statefulset.yaml
	kubectl apply -f k8s/kafka/kafka-init-job.yaml

deploy-apps:
	@echo "Subindo Bancos de Dados e Aplicações..."
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
	kubectl delete -f k8s/kafka/ || true

status:
	@echo "Status dos Pods:"
	kubectl get pods
	@echo "\nStatus dos Serviços:"
	kubectl get svc
	@echo "\nStatus dos StatefulSets:"
	kubectl get statefulset

logs-producer:
	kubectl logs -l app=producer -f

logs-consumer:
	kubectl logs -l app=consumer -f

logs-controlador:
	kubectl logs -l app=controlador -f

db-shell:
	@echo "Acessando o terminal do PostgreSQL (Digite 'exit' para sair)..."
	kubectl exec -it statefulset/postgres -- psql -U postgres -d fabrica

db-ui:
	@echo "Abrindo o painel Adminer na porta 8080 (Acesse http://localhost:8080 no navegador)..."
	@echo "O Auto-Login está ativado. Você será conectado automaticamente na base fabrica!"
	kubectl port-forward svc/adminer 8080:8080
