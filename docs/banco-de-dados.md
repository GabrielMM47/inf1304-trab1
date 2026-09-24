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
   *(Você será conectado instantaneamente na base de dados `fabrica`, sem precisar digitar sistema, usuário ou senha!)*

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
