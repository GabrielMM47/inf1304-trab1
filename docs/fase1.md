# Fase 1: Aplicações (Produtor e Consumidor)

Esta seção detalha o funcionamento dos scripts de simulação de telemetria da fábrica inteligente e os processadores de anomalia, desenvolvidos na primeira fase do projeto.

## 1. Produtor (`sensor.py`)

O script atua como um sensor ou terminal de uma máquina que envia informações sobre sua operação para o cluster Kafka. 

- **Dados Coletados:** Temperatura, Vibração, Consumo de Energia e Emissão de CO2.
- **Intervalos:** Diferentes grandezas precisam ser enviadas em diferentes frequências. Para isso, há uma variável base `GENERATION_INTERVAL` (ex: 2.0s) e multiplicadores individuais:
  - `MULTIPLICADOR_TEMPERATURA` (padrão 1.0)
  - `MULTIPLICADOR_VIBRACAO` (padrão 1.5)
  - `MULTIPLICADOR_ENERGIA` (padrão 2.0)
  - `MULTIPLICADOR_CO2` (padrão 5.0)
- **Identificação:** A máquina gera um `MAQUINA_ID` (pode ser sobrescrito como variável de ambiente) que vai atrelado ao envio (payload) para possibilitar rastreio no lado do consumidor.

## 2. Consumidor (`processor.py`)

O consumidor recebe as mensagens no tópico configurado (`KAFKA_TOPIC`) e avalia os limites operacionais estipulados.

- **Perfis de Limite:** Permite definir limites de operação através da variável `PERFIL_LIMITES` (opções: `strict`, `normal`, `loose`).
- **Sobrescrita Individual:** Limites específicos também podem ser controlados (ex: `LIMITE_TEMPERATURA`, `LIMITE_ENERGIA`).
- **Tolerância a Falhas e Janela de Alertas:** O consumidor não se baseia em um alerta simples para condenar uma máquina. Ele usa um histórico compartilhado:
  - Verifica o volume de falhas em uma janela de tempo (`JANELA_ANALISE_SEG`).
  - Se a máquina estourar o limite de alertas consecutivos (`MAX_ALERTAS_JANELA`), ele envia um comando `KILL` para o tópico de controle (`comandos-fabrica`).
  - Isso provê rastreabilidade e delega a responsabilidade de intervir ativamente na máquina para o microsserviço Controlador.

## 3. Controlador (`controlador.py`)

O controlador atua como o atuador da malha de feedback no Kubernetes. Ele é responsável por ler os comandos emitidos pelos consumidores e matar as máquinas danificadas.

- **Comandos via Kafka:** Escuta o tópico `comandos-fabrica` usando o mesmo `Consumer Group` para garantir que apenas uma instância tente realizar o comando de morte em um pod.
- **Integração Kubernetes (RBAC):** Usando a biblioteca Python `kubernetes` e as configurações _In-Cluster_, o controlador utiliza as credenciais de sua `ServiceAccount` no pod para identificar os recursos com a _label_ referenciada (`sensor-id`) e executar o comando de deleção nativo.

## 4. Auditoria e Persistência (PostgreSQL)

Os microsserviços não apenas escrevem `prints` no console, mas garantem rastreabilidade de eventos chaves em banco de dados:
- **Tabela de Eventos (`event_table`):** Todos os três (Produtor, Consumidor, Controlador) se conectam ao PostgreSQL para registrar logs auditáveis.
- **Tipos de Eventos Salvos:** Registram reinícios (`START`), coletas bem-sucedidas (`SEND_DATA`, `PROCESS_DATA`), anomalias parciais (`ALERT_TRIGGERED`) e intervenções críticas (`CRITICAL_ALERT_KILL_SENT`, `MACHINE_KILLED`, `MACHINE_NOT_FOUND`).

## 5. Configurações por Variáveis de Ambiente

Todo o projeto segue a abordagem de não *hard-codar* as lógicas. Utilizamos as seguintes variáveis principais (além dos limites e multiplicadores):
- `KAFKA_BROKER`, `KAFKA_TOPIC`, `KAFKA_TOPIC_COMANDOS`
- `KAFKA_GROUP_ID` (Garante paralelismo no particionamento do tópico para múltiplos consumidores)
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD` (Para armazenamento centralizado)

As três aplicações (sensor, processor e controlador) são empacotadas através de `Dockerfile` utilizando imagens reduzidas (`python:3.9-slim`).

## 6. Fluxo de Interações (Workflow)

Para consolidar o entendimento, o ecossistema opera continuamente no seguinte ciclo de vida:

1. **Inicialização (`START`):**
   - O **Produtor** liga, registra o evento de `START` no Postgres, e entra no loop de geração de telemetria.
   - O **Consumidor** liga, conecta-se ao Kafka e ao armazenamento de estado (Redis ou local), gravando o seu `START` no banco.
   - O **Controlador** liga, obtém as credenciais de autenticação da API do Kubernetes via ServiceAccount e loga seu `START`.

2. **Produção de Dados (`SEND_DATA`):**
   - O Produtor cria os dados (temperatura, vibração, etc.) com base nos multiplicadores e envia o JSON para o tópico `dados-sensores`. 
   - A `maquina_id` é usada como chave da mensagem no Kafka.
   - Um evento `SEND_DATA` é gravado de forma auditável no Postgres.

3. **Processamento Normal e Alertas Isolados (`PROCESS_DATA` / `ALERT_TRIGGERED`):**
   - O Consumidor consome a mensagem do Kafka.
   - Se a leitura está saudável, ele registra um `PROCESS_DATA` no Postgres e avança.
   - Se a leitura ultrapassa os limites (`strict`, `normal` ou `loose`), ele dispara um `ALERT_TRIGGERED` no banco e soma 1 erro no histórico daquela máquina durante a janela de tempo.

4. **Escalonamento Crítico (`CRITICAL_ALERT_KILL_SENT`):**
   - Se a mesma máquina acumular múltiplas falhas num curto espaço de tempo (estourando a janela), o Consumidor entende que a máquina física (ou seu software) quebrou.
   - O Consumidor publica a instrução `{"comando": "KILL", "maquina_id": "X"}` no tópico `comandos-fabrica`.
   - Ele registra `CRITICAL_ALERT_KILL_SENT` no banco de dados e zera o histórico de erros daquela máquina para não criar spam.

5. **Ação Automática de Resiliência (`MACHINE_KILLED`):**
   - O Controlador lê a ordem de morte do tópico `comandos-fabrica`.
   - Ele faz uma query na API do Kubernetes buscando o Pod (container) cuja label `sensor-id` bata exatamente com a `maquina_id` defeituosa.
   - O pod correspondente é deletado (o que fará o Kubernetes reiniciá-lo ou substituí-lo no cluster dependendo do Deployment).
   - O Controlador grava o evento `MACHINE_KILLED` (ou de erro, se não encontrar) no Postgres, fechando o loop de feedback.
