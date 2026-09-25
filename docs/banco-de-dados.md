# Visualização e Acesso ao Banco de Dados

O projeto inclui o banco de dados PostgreSQL para armazenar toda a telemetria e o histórico de auditoria (eventos) dos microsserviços.

Para facilitar a visualização e a validação de que os dados estão sendo armazenados corretamente, providenciamos duas formas de acesso: via Interface Gráfica (Adminer) e via Terminal (PSQL).

## 1. Acesso via Interface Gráfica (Adminer)

O Adminer é uma interface web incrivelmente leve que embutimos diretamente no seu cluster K8s para facilitar a administração visual do banco. Ele já foi **configurado para auto-login**, poupando seu tempo durante os testes.

Para acessá-lo:
1. No seu terminal, na raiz do projeto, rode:
   ```bash
   make db-ui
   ```
2. Abra o seu navegador e acesse: [http://localhost:8080](http://localhost:8080)
   *(Você será conectado instantaneamente na base de dados `fabrica`, sem precisar digitar sistema, usuário ou senha! O Adminer lê a senha do Secret `postgres-credentials`. Se precisar dela para outro cliente, use `make db-password`.)*

A partir da interface, você pode clicar em **"Comando SQL"** para executar as *queries* abaixo, ou simplesmente clicar em **"Selecionar"** nas tabelas `leituras_sensores` e `event_table` para ver as métricas entrando em tempo real.

## 2. Acesso Direto pelo Terminal (PSQL)

Caso prefira a agilidade da linha de comando sem precisar sair da sua IDE, criamos um atalho direto pelo Makefile:

```bash
make db-shell
```
Isso entrará dentro do Pod do PostgreSQL, fará o login nativo com as credenciais, e te deixará dentro da shell interativa `fabrica=#`.

## 3. Queries Úteis

Aqui estão algumas queries prontas para você investigar o comportamento da sua fábrica inteligente em execução:

**1. Auditoria Geral: Visualizar o fluxo do ecossistema**
*(Ideal para ver quando as máquinas ligaram, processaram dados, lançaram alertas e quando o Controlador agiu).*
```sql
SELECT timestamp, component_name, event_type, details 
FROM event_table 
ORDER BY timestamp DESC 
LIMIT 20;
```

**2. Isolando Ações Críticas (Auto-Cura da Infraestrutura)**
*(Checar apenas os momentos exatos em que os consumidores estouraram a janela e mandaram o comando KILL, e quando o Controlador respondeu matando a máquina no K8s).*
```sql
SELECT timestamp, component_name, details 
FROM event_table 
WHERE event_type = 'MACHINE_KILLED' OR event_type = 'CRITICAL_ALERT_KILL_SENT'
ORDER BY timestamp DESC;
```

**3. Média de Temperatura em Janelas de Tempo**
*(Monitorar a temperatura média de uma máquina específica a cada minuto).*
```sql
SELECT maquina_id, date_trunc('minute', timestamp) AS minuto, AVG(valor) AS temp_media
FROM leituras_sensores
WHERE tipo_sensor = 'temperatura'
GROUP BY maquina_id, minuto
ORDER BY minuto DESC;
```

**4. Ranking dos Sensores mais instáveis**
*(Analisar qual métrica tem sido responsável por disparar o maior número de anomalias/alertas).*
```sql
SELECT tipo_sensor, COUNT(*) as total_alertas
FROM leituras_sensores
WHERE houve_alerta = true
GROUP BY tipo_sensor
ORDER BY total_alertas DESC;
```

**5. Distribuição de Alertas por Máquina**
*(Descobrir quais máquinas da fábrica estão dando mais dor de cabeça em tempo real).*
```sql
SELECT maquina_id, tipo_sensor, COUNT(*) as total_alertas
FROM leituras_sensores
WHERE houve_alerta = true
GROUP BY maquina_id, tipo_sensor
ORDER BY total_alertas DESC;
```

**6. Estresse Máximo, Mínimo e Médio Absoluto**
*(Obter as estatísticas absolutas dos sensores para entender os picos de estresse gerados na fábrica).*
```sql
SELECT 
    maquina_id, 
    tipo_sensor, 
    MAX(valor) AS pico_maximo, 
    MIN(valor) AS minimo, 
    ROUND(AVG(valor), 2) AS media,
    COUNT(*) FILTER (WHERE houve_alerta = true) AS total_alertas
FROM leituras_sensores
GROUP BY maquina_id, tipo_sensor
ORDER BY maquina_id, tipo_sensor;
```

**7. Volume de Eventos por Componente**
*(Verificar o balanço do fluxo arquitetural: se o consumidor está processando mais dados do que lançando alertas, e se o controlador está atuando na proporção correta).*
```sql
SELECT component_name, event_type, COUNT(*) as volume
FROM event_table
GROUP BY component_name, event_type
ORDER BY volume DESC;
```
