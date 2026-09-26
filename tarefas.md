### 1. Arquitetura e Configuração da Infraestrutura

* [x] **1.1. Cluster Kafka:**
* [ ] 1.1.1. Possui **2 ou mais brokers** Kafka rodando em containers (Docker) ou pods (Kubernetes) distintos.


* [ ] 1.1.2. O tópico `dados-sensores` foi criado com **múltiplas partições** e **fator de replicação configurado**.




* [ ] **1.2. Produtores (Sensores):**
* [x] 1.2.1. Estão simulados em containers/pods separados.


* [x] 1.2.2. Geram e enviam dados periódicos (ex.: JSON contendo temperatura, vibração, consumo de energia, etc.) para o tópico `dados-sensores`.




* [ ] **1.3. Consumidores (Processadores de Dados):**
* [x] 1.3.1. Desenvolvidos em Java ou Python.


* [ ] 1.3.2. Estão configurados no **mesmo grupo de consumo** (*consumer group*) para dividir e balancear as partições.


* [x] 1.3.3. Implementam o processamento em tempo real (ex.: detecção de anomalias/limites de temperatura).




* [ ] **1.4. Armazenamento / Log:**
* [x] 1.4.1. Há um banco de dados ou mecanismo de logging registrando os dados processados e/ou alertas gerados.





---

### 2. Cenários de Teste e Demonstração Obrigatórios

* [ ] **2.1. Failover do Broker:** Script ou procedimento demonstrando que, ao derrubar um broker Kafka, o cluster continua operando sem interrupção.


* [ ] **2.2. Rebalanceamento de Consumidores:** Script ou procedimento demonstrando que, ao parar um consumidor, outro consumidor do grupo assume a(s) partição(ões) órfã(s).


* [ ] **2.3. Elasticidade e Carga:** Demonstração visual ou via logs do comportamento do sistema ao variar o volume de sensores/mensagens.



---

### 3. Código-Fonte e Boas Práticas (Pontuadas no Trabalho)

* [ ] **3.1. Documentação de Código:** Comentários presentes no código usando **JavaDoc** (se Java) ou **DocString** (se Python).


* [ ] **3.2. Configurações Externas:** Nenhuma constante *hard-coded*; todas as configurações (endereços, portas, tópicos) devem estar em `application.properties` ou em variáveis de ambiente nos arquivos YAML.


* [x] **3.3. Gerenciador de Dependências:** `pom.xml` incluído e configurado (caso use Java/Maven) ou equivalente (`requirements.txt`).



---

### 4. Arquivos e Entregáveis do Pacote

Verifique se os seguintes arquivos estão presentes no repositório/compactado para envio no EaD:

* [x] **4.1. `Makefile`** para facilitar a inicialização, execução, subida dos serviços e testes.


* [x] **4.2. Arquivos YAML** com as definições de todos os serviços (Docker Compose ou manifestos Kubernetes).


* [x] **4.3. Código-fonte completo** dos produtores e consumidores.


* [x] **4.4. Scripts de simulação de falhas**.


* [ ] **4.5. Logs de execução salvos/anexados** evidenciando o rebalanceamento e o funcionamento sob estresse/falha.



---

### 5. Relatório Técnico

Certifique-se de que o documento inclui:

* [ ] **5.1. Guia de Instalação e Execução:** Instruções passo a passo de como subir a infraestrutura e rodar a aplicação.


* [ ] **5.2. Guia de Operação:** Instruções de como enviar comandos, monitorar ou visualizar os resultados.


* [ ] **5.3. Explicação da Arquitetura:** Desenho/descrição da solução (brokers, partições, tópicos, containers).


* [ ] **5.4. Resultados dos Testes de Falha:** Evidências comentadas das falhas forçadas e rebalanceamentos.


* [ ] **5.5. Avaliação Crítica:** Seção explícita indicando **o que funcionou** e **o que não funcionou**.



---

### 6. Preparação para a Apresentação Oral

* [ ] **6.1. Domínio Individual:** Todos os integrantes do grupo devem entender e saber explicar qualquer parte da solução, pois a **avaliação e a nota da apresentação serão individuais**.