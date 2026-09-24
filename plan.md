A implementação do sistema de monitoramento da fábrica inteligente exige uma progressão lógica, começando pelo código da aplicação, passando pela infraestrutura do cluster, e terminando nos testes de resiliência.

### Fase 1: Desenvolvimento das Aplicações (Python)

Nesta etapa, o foco é escrever os produtores e consumidores com boas práticas de documentação e configuração.

* **Produtor (Sensores):** Desenvolva um script em Python que simule a geração periódica de dados, como temperatura e vibração, em formato JSON. Este script deve enviar as mensagens para o tópico Kafka chamado `dados-sensores`. Comente o código utilizando DocStrings.


* **Consumidor (Processadores):** Crie um script Python que consuma as mensagens do tópico `dados-sensores` e detecte quando valores (como a temperatura) ultrapassam limites predefinidos. Todos os consumidores devem obrigatoriamente pertencer ao mesmo grupo de consumo para que a carga seja dividida (por exemplo, um lê a partição A e outro a B). Utilize DocStrings para documentar as funções.


* **Gestão de Configuração:** Não utilize constantes hard-coded no código. Todas as configurações (endereço do broker, nome do tópico, limites de alerta) devem ser lidas a partir de variáveis de ambiente.


* **Banco de Dados (Armazenamento):** Integre o consumidor a um banco PostgreSQL. Todos os dados processados e alertas gerados devem ser persistidos para histórico, utilizando conexão baseada em variáveis de ambiente.


* **Containerização:** Escreva um `Dockerfile` para o produtor e outro para o consumidor, garantindo que as aplicações rodem em containers isolados.



### Fase 2: Configuração da Infraestrutura (YAML e Kubernetes)

Com as imagens Docker prontas, estruture os recursos no Kubernetes.

* **Cluster Kafka (Brokers):** Escreva os manifestos YAML (geralmente um `StatefulSet` e `Services`) para implantar 2 ou mais instâncias do Kafka rodando em pods diferentes.


* **Configuração de Tópicos:** Crie um `Job` no Kubernetes ou um script de inicialização que utilize os comandos administrativos do Kafka para criar o tópico `dados-sensores`. O tópico deve ser dividido em várias partições e ter replicação configurada entre os brokers.


* **ConfigMaps e Secrets:** Defina um arquivo YAML de `ConfigMap` (ou `application.properties`) para injetar as variáveis de ambiente necessárias nos containers dos produtores e consumidores.


* **Banco de Dados PostgreSQL:** Crie os manifestos YAML (`StatefulSet` e `Service`) para subir o banco de dados no cluster, com os devidos volumes persistentes (`PVC`) para os dados armazenados.


* **Deployments das Aplicações:** Escreva os manifestos YAML de `Deployment` para os sensores (produtores) e para os consumidores, permitindo que rodem em múltiplos pods.



### Fase 3: Automação e Orquestração

* **Makefile:** Crie um arquivo `Makefile` na raiz do projeto para automatizar tarefas repetitivas, como construir as imagens Docker, aplicar os manifestos YAML com o `kubectl` e rodar os scripts de teste.



### Fase 4: Simulação de Falhas e Elasticidade (Scripts Shell)

Desenvolva scripts independentes para provar que o sistema atende aos requisitos de tolerância a falhas.

* **Teste de Failover (Kafka):** Escreva um script (`fail_broker.sh`) que pare ou delete o pod de um dos brokers do Kafka. Colete os logs para provar que o sistema continua funcionando e recebendo mensagens sem interrupção.


* **Teste de Rebalanço (Consumidor):** Crie um script (`fail_consumer.sh`) que mate o pod de um consumidor ativo. Capture os logs que demonstrem o rebalanço ocorrendo e outro consumidor assumindo a leitura daquela partição órfã.


* **Teste de Elasticidade:** Utilize comandos de escala do Kubernetes (ex: `kubectl scale`) para aumentar o número de pods de sensores, demonstrando visualmente ou via logs o comportamento do sistema sob maior carga.



### Fase 5: Documentação e Empacotamento

* **Geração de Logs:** Salve as saídas de terminal que mostram o rebalanço, a criação das partições e a sobrevivência do sistema após as falhas.


* **Relatório:** Escreva o relatório final detalhando a arquitetura implementada, os testes de falha realizados, além do que funcionou e o que não funcionou.


* **Manuais:** Inclua um documento claro com as instruções exatas de como instalar a aplicação e como operá-la e testá-la.