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
  - Se a máquina estourar o limite de alertas consecutivos (`MAX_ALERTAS_JANELA`), ele emite um log CRÍTICO.
  - Isso provê subsídios e rastreabilidade para orquestração decidir matar/escalar o serviço e intervir na máquina problemática.

## 3. Configurações por Variáveis de Ambiente

Todo o projeto segue a abordagem de não *hard-codar* as lógicas. Utilizamos as seguintes variáveis principais (além dos limites):
- `KAFKA_BROKER`
- `KAFKA_TOPIC`
- `KAFKA_GROUP_ID` (Garante paralelismo no particionamento do tópico para múltiplos consumidores)

As aplicações são empacotadas através de `Dockerfile` utilizando imagens reduzidas (`python:3.9-slim`).
