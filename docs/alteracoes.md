# Registro de Alterações

Este documento registra, em ordem cronológica, as correções feitas no projeto após a revisão dos requisitos do enunciado (`Trab1_2026-1.pdf`). Cada entrada diz **o que mudou**, **por quê** e **quais arquivos** foram tocados, para que todos do grupo consigam acompanhar e validar.

## Plano de correções

A revisão do repositório contra o enunciado gerou uma lista de itens. Os números abaixo são usados nas entradas do registro.

| # | Item | Situação |
|---|------|----------|
| 1 | Replicação do tópico `dados-sensores` (hoje `--replication-factor 1` no Job de criação de tópicos) | Pendente |
| 2 | Quórum KRaft: 2 brokers/controllers não toleram a perda de um nó (o Raft exige maioria) | Pendente |
| 3 | Kafka sem volume persistente (`log.dirs=/tmp`, sem `volumeClaimTemplates`) | A avaliar se se aplica |
| 4 | Teste de falha de broker (script apenas deleta o pod, não verifica nem salva evidência) | Pendente |
| 5 | Log de rebalanço no consumidor (`ConsumerRebalanceListener`, partição em cada leitura) | Pendente |
| 6 | Logs salvos pelos scripts de teste e relatório final (`docs/report.md`) | Em andamento: scripts salvam logs (feito); relatório pendente |
| 7 | Documentação citando Zookeeper, removido do projeto (Kafka roda em KRaft) | **Concluído** |
| 8 | Constantes hard-coded e docstrings faltantes (itens pontuados no enunciado) | Em andamento (escopo parcial): tempo de processamento, UUID do cluster e senha do Postgres feitos; faltam perfis de limites/faixas do sensor e docstrings |

Ordem combinada de execução: 7, depois 8 (parcial), depois a parte de salvamento de logs do 6, depois 1, 2, 4 e 5, e por fim o relatório do 6.

---

## Roteiro de validação no cluster

Este é o roteiro único para testar tudo o que foi alterado. **Cada nova entrada do registro deve acrescentar seus passos aqui.** Legenda: ✅ resultado esperado; ⚠️ o que indica problema. Marque as caixas e preencha a tabela de resultados ao final ao executar.

> Até o momento, **nada disto foi executado em um cluster real**: as alterações foram verificadas apenas localmente (sintaxe, compilação e simulação com `kubectl` falso). Os passos abaixo são a validação que falta.

### Pré-requisitos

- Máquina Debian com Docker e k3s (`make init` e `make start-cluster`), na raiz do repositório, com `kubectl` funcionando.
- Dar permissão de execução aos scripts: `chmod +x scripts/*.sh`. Eles estão versionados **sem** essa permissão; sem isso, `./scripts/test_*.sh` e o `make test-all` (que chama os scripts com `./`) falham com "Permission denied". Alternativa: rodar com `bash scripts/nome.sh`.
- Tempo estimado: 15 a 20 minutos no total.

### Passo 0: subir o ambiente

```bash
make all          # build das imagens + deploy (Kafka e aplicações)
make status       # repita até estabilizar (1 a 2 minutos)
```
- [ ] ✅ Pods `Running`: `kafka-0`, `kafka-1`, `postgres-0`, `redis`, `adminer`, 2 `controlador`, 3 `producer-*`, 2 `consumer`. O pod do Job `kafka-init-topics` fica `Completed`.
- ⚠️ `CreateContainerConfigError` indica Secret ausente (rode `make secrets`); `CrashLoopBackOff` em `kafka-*`, veja `kubectl logs kafka-0`.

### Passo 1: tempo de processamento configurável (item 8, `sleep`)

```bash
kubectl exec deploy/consumer -- env | grep TEMPO_PROCESSAMENTO
```
- [ ] ✅ `TEMPO_PROCESSAMENTO_MIN_SEG=0.5` e `TEMPO_PROCESSAMENTO_MAX_SEG=1.5`.

Teste de comportamento: em `k8s/apps/configmap.yaml`, mude ambos para `"3.0"` no `consumer-config`, e rode:
```bash
kubectl apply -f k8s/apps/configmap.yaml && kubectl rollout restart deployment consumer
kubectl logs -l app=consumer --timestamps --tail=10
```
- [ ] ✅ As linhas "Leitura recebida..." de um mesmo consumidor passam a ter cerca de 3 s entre si. Depois **volte os valores para 0.5 e 1.5**, aplique e reinicie de novo.

### Passo 2: senha do Postgres em Secret (item 8)

```bash
kubectl get secret postgres-credentials
make db-password
kubectl get configmap postgres-config producer-config consumer-config controlador-config -o yaml | grep -i password
make secrets
make db-password
```
- [ ] ✅ O Secret existe; `db-password` imprime uma senha de 24 caracteres; o `grep` não retorna nada (nenhuma senha nos ConfigMaps de configuração; o `adminer-autologin` não entra na lista porque só contém o código PHP, que lê a senha do ambiente); o segundo `make secrets` imprime "já existe (mantido)" e a senha impressa em seguida é **a mesma**.

As aplicações conseguem conectar no banco com a senha do Secret:
```bash
kubectl logs -l app=consumer | grep -i postgresql
kubectl logs -l app=producer | grep -i aviso
kubectl logs -l app=controlador | grep -i aviso
```
- [ ] ✅ O consumidor mostra "Conectado ao PostgreSQL com sucesso"; produtores e controlador **não** mostram "Aviso: Não foi possível conectar ao PostgreSQL". ⚠️ Se mostrarem, a senha do Secret não bate com a do banco (ver "Cuidados" na entrada do Secret).

Dados chegando e acesso ao banco:
```bash
make db-shell     # dentro do psql: SELECT count(*) FROM leituras_sensores;  (repita após ~10 s)
make db-ui        # abrir http://localhost:8080
```
- [ ] ✅ A contagem é maior que zero e **cresce**. O Adminer abre já conectado no banco `fabrica`, sem pedir senha.

### Passo 3: UUID do cluster Kafka (item 8)

```bash
kubectl logs kafka-0 | head -20
kubectl logs kafka-1 | head -20
kubectl exec kafka-0 -- cat /tmp/kraft-logs/meta.properties
kubectl exec kafka-1 -- cat /tmp/kraft-logs/meta.properties
kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list
```
- [ ] ✅ Os logs de ambos mostram `Formatando logs KRaft (cluster ID: zrH_urXmRdCh86VyykE9Vg)`; os dois `meta.properties` têm o **mesmo** `cluster.id=zrH_urXmRdCh86VyykE9Vg`; a listagem mostra `dados-sensores` e `comandos-fabrica`. ⚠️ `KAFKA_CLUSTER_ID não definido` indica que o `kafka-config.yaml` não foi aplicado antes do StatefulSet.

### Passo 4: o que o `make clean` mantém (correção de documentação)

```bash
make clean
kubectl get pvc
kubectl get secret postgres-credentials
```
- [ ] ✅ O PVC `pg-data-postgres-0` **ainda existe** e o Secret **ainda existe**.
```bash
make db-reset
kubectl get pvc
make all
```
- [ ] ✅ Depois do `db-reset` o PVC some; após o novo `make all` e alguns segundos, `SELECT count(*) FROM leituras_sensores;` recomeça com valor baixo (banco zerado) e as aplicações conectam normalmente (mesmo Secret).

### Passo 5: scripts de teste salvando logs (item 6)

Pré-condição: passo 0 concluído e `chmod +x scripts/*.sh` feito. Os arquivos aparecem em `logs/`.

**5a. Rebalanço de consumidor** (cerca de 1 minuto):
```bash
./scripts/test_consumer_rebalance.sh
ls logs/
```
- [ ] ✅ Cria `logs/rebalanco_consumidor_<data>_<hora>.log` com 5 seções (ANTES, últimas linhas do pod derrubado, FALHA, DEPOIS, logs dos consumidores). No ANTES, a tabela do grupo mostra as 3 partições distribuídas entre **2** `CONSUMER-ID` diferentes. No DEPOIS, nenhuma partição fica com `CONSUMER-ID` `-`, e as do pod derrubado passaram para outro consumidor.

**5b. Elasticidade** (cerca de 2 a 3 minutos):
```bash
./scripts/test_elasticity.sh
```
- [ ] ✅ Cria `logs/elasticidade_*.log` com 12 amostras (6 por fase). Na Fase 1 o `LAG` tende a **subir**; na Fase 2, a **cair**. Se o LAG não crescer, aumente o tempo de processamento (passo 1) para deixar o efeito visível. Ao final, volte ao estado original: `kubectl scale deployment producer-maquina-1 --replicas=1 && kubectl scale deployment consumer --replicas=2`.

**5c. Failover de broker** (cerca de 1 minuto):
```bash
./scripts/test_broker_failover.sh
```
- [ ] ✅ Cria `logs/failover_broker_*.log`. **Resultado esperado hoje, com os itens 1 e 2 ainda pendentes** (replicação 1 e quórum de 2 nós): partições sem líder, comandos que terminam com código 1 ou 124, ou contagem de leituras parada. Isso **documenta o problema**, não é erro do script. Depois de corrigir os itens 1 e 2, o esperado passa a ser `Leader` diferente de `-1` em todas as partições e contagem de leituras crescendo entre ANTES e DEPOIS.

**5d. Script interativo:**
```bash
make test-all
```
- [ ] ✅ As fases de rebalanço e de failover geram novos arquivos em `logs/`. A fase de elasticidade dele **não** gera log (usa `make scale-*`).

### Registro de resultados

Preencha ao executar (data, quem testou, resultado e observações).

| Passo | Data | Quem | Resultado (OK / falhou) | Observações |
|-------|------|------|-------------------------|-------------|
| 0 Subir ambiente | | | | |
| 1 Tempo de processamento | | | | |
| 2 Secret do Postgres | | | | |
| 3 UUID do cluster | | | | |
| 4 `make clean` / `db-reset` | | | | |
| 5a Rebalanço | | | | |
| 5b Elasticidade | | | | |
| 5c Failover de broker | | | | |
| 5d `make test-all` | | | | |

---

## Entradas

### Item 7: Documentação sem Zookeeper (2026-09-25)

**Problema.** O Kafka do projeto roda em modo KRaft (sem Zookeeper), mas três documentos e o README ainda descreviam o Zookeeper como parte da arquitetura. Isso deixava a documentação diferente do que está implementado e poderia gerar confusão na apresentação.

**O que foi feito.** Apenas texto de documentação; nenhum código ou manifesto foi alterado.

| Arquivo | Alteração |
|---------|-----------|
| `README.md` | Na árvore de pastas, a linha do `zookeeper.yaml` (arquivo que nunca existiu) foi trocada por `kafka-scripts.yaml`, o ConfigMap real que contém o `setup-kraft.sh`. |
| `docs/fase2.md` | O bullet do `zookeeper.yaml` foi substituído por uma explicação do `kafka-scripts.yaml`: cada pod é `broker` e `controller`, o `node.id` sai do nome do pod (`kafka-0` → `0`) e os controllers formam o quórum que cuida dos metadados e da eleição de líderes de partição. |
| `docs/fase3.md` | A ordem do `make deploy-kafka` foi corrigida para refletir o Makefile: ConfigMap com o script KRaft → Services → StatefulSet Kafka → Job de tópicos. |
| `docs/fase4.md` | "o Zookeeper elege o broker sobrevivente" passou a "o controller KRaft elege o broker sobrevivente". |

**Como validar.** Só texto, sem teste em cluster: `git grep -in zookeeper -- . ':!docs/alteracoes.md'` deve retornar 3 linhas (`Makefile`, `README.md` e `docs/fase2.md`), todas dizendo **"sem Zookeeper"**. A afirmação do `fase4.md` sobre o controller KRaft eleger o novo líder é comportamento e só é testada no passo 5c do [Roteiro de validação](#roteiro-de-validação-no-cluster).

**Pontos que ficaram para outros itens.**
- `docs/fase2.md` e `docs/fase4.md` ainda afirmam que o Kafka usa disco persistente (PVC). O StatefulSet do Kafka não tem `volumeClaimTemplates`; isso é tratado no item 3.
- A frase do `fase4.md` descreve o comportamento esperado do failover. Ele só é de fato garantido depois dos itens 1 e 2.

### Item 8 (parcial): Tempo de processamento simulado configurável (2026-09-25)

**Problema.** O consumidor simulava o processamento pesado com `time.sleep(random.uniform(0.5, 1.5))` fixo em `processor.py`. O enunciado pontua a ausência de constantes hard-coded, e alterar esse tempo (por exemplo, para deixar o lag mais visível no teste de elasticidade) exigia mexer no código e reconstruir a imagem.

**O que foi feito.** O intervalo passou a vir de variáveis de ambiente, seguindo o mesmo padrão das demais configurações do consumidor.

| Arquivo | Alteração |
|---------|-----------|
| `src/consumer/processor.py` | `obter_configuracao()` lê `TEMPO_PROCESSAMENTO_MIN_SEG` e `TEMPO_PROCESSAMENTO_MAX_SEG` (padrão 0.5 e 1.5, iguais ao valor antigo). `processar_mensagem()` usa esses valores no `sleep`. Docstring e comentário atualizados. |
| `k8s/apps/configmap.yaml` | As duas variáveis foram adicionadas ao `consumer-config`. |
| `docs/fase1.md` | Nova linha descrevendo o tempo de processamento simulado. |

**Comportamento.** Sem mudança com os valores atuais (0,5 a 1,5 s). Para alterar, edite o ConfigMap e reinicie os consumidores (`kubectl apply -f k8s/apps/configmap.yaml && kubectl rollout restart deployment consumer`); não é preciso rebuild.

**Como validar.** Ver o passo 1 do [Roteiro de validação](#roteiro-de-validação-no-cluster).

**Pendente no item 8 (na época desta entrada).** UUID do cluster e senha do Postgres foram tratados na entrada seguinte. Continuam pendentes: perfis de limites e faixas de valores do sensor (ainda no código) e docstrings faltantes.

### Item 8 (parcial): Senha do Postgres em Secret e UUID do cluster Kafka em ConfigMap (2026-09-25)

Duas correções do mesmo tema (valores que estavam fixos no código ou em ConfigMaps inadequados), feitas juntas.

#### A) Senha do Postgres: de ConfigMap para Secret, com geração aleatória

**Conceitos.** ConfigMap e Secret são objetos chave-valor do Kubernetes, injetados no pod como variáveis de ambiente ou arquivos. O ConfigMap serve para configuração **não sensível** (hosts, portas, limites). O Secret serve para dados **sensíveis** e o cluster o trata de forma diferente: o `kubectl describe` oculta os valores, o acesso pode ser restringido por RBAC separadamente do de ConfigMaps e, no nó, ele é montado em memória. Atenção: o conteúdo de um Secret é apenas **codificado em base64, não criptografado**; por isso um `secret.yaml` com a senha, se fosse versionado, exporia a senha a quem lê o repositório.

**Problema.** A senha `postgres` aparecia em texto puro em quatro ConfigMaps (`postgres-config`, `producer-config`, `consumer-config`, `controlador-config`) e como valor padrão dentro do código Python dos três serviços e do plugin do Adminer.

**O que foi feito.**

| Arquivo | Alteração |
|---------|-----------|
| `Makefile` | Novo alvo `secrets`: cria o Secret `postgres-credentials` com uma senha aleatória de 24 caracteres **apenas se ele ainda não existir**. `deploy-apps` passou a depender dele. Novo alvo `db-password`: imprime a senha lida do Secret. |
| `k8s/apps/configmap.yaml` | Removidos `POSTGRES_PASSWORD` e os três `PG_PASSWORD`. |
| `k8s/apps/postgres.yaml`, `adminer.yaml` | `POSTGRES_PASSWORD` agora vem do Secret via `secretKeyRef`. O plugin de auto-login do Adminer não tem mais senha padrão embutida. |
| `k8s/apps/producer-deployment.yaml` (3 deployments), `consumer-deployment.yaml`, `controlador-deployment.yaml` | `PG_PASSWORD` vem do Secret via `secretKeyRef`. |
| `src/producer/sensor.py`, `src/consumer/processor.py`, `src/controlador/controlador.py` | O valor padrão de `PG_PASSWORD` deixou de ser `"postgres"` e passou a vazio: sem a variável, o serviço não tem senha alguma no código. |
| `docs/fase1.md`, `fase2.md`, `fase3.md`, `banco-de-dados.md`, `README.md` | Atualizados para descrever o Secret, `make secrets` e `make db-password`. |

**Por que a senha é gerada uma vez e não a cada execução.** O Postgres só lê `POSTGRES_PASSWORD` quando cria o volume de dados pela primeira vez. Uma senha nova a cada `make all` deixaria o banco com a senha antiga e as aplicações falhariam ao conectar. Por isso o `make secrets` mantém o Secret existente.

**Como ver a senha.** Você não precisa dela para usar o Adminer (auto-login) nem o `make db-shell`. Para um cliente SQL externo: `make db-password`.

**Cuidados.**
- Se o **Secret for apagado e o volume do Postgres não** (`kubectl delete secret postgres-credentials`, mantendo o PVC), o `make secrets` gera outra senha que não bate com a do banco já criado. Para recomeçar, apague os dois: o Secret e o PVC (`make db-reset`).
- O `make clean` não apaga o Secret nem o PVC do Postgres (o Kubernetes não remove PVCs de StatefulSet ao deletar o StatefulSet), então os dois seguem consistentes entre um `clean` e um novo `all`.
- O Adminer continua com auto-login, e o `make db-ui` o expõe em `0.0.0.0:8080`: quem alcança essa porta entra no banco sem senha. Vale mencionar no relatório.
- A senha antiga (`postgres`) continua no histórico do Git, em commits anteriores.

#### B) UUID do cluster Kafka: de constante dentro do script para ConfigMap

**Conceito.** No modo KRaft, todo cluster tem um **cluster ID**, um UUID de 22 caracteres. O comando `kafka-storage format` o grava no `meta.properties` do disco de cada broker, e o broker recusa subir se o ID configurado divergir do gravado. Como cada pod roda o script de inicialização sozinho, o ID precisa ser um valor **compartilhado e estável**: um ID aleatório por pod impediria o cluster de se formar, e um pod recriado após falha não voltaria ao cluster.

**Problema.** O script `setup-kraft.sh` (em `kafka-scripts.yaml`) tinha o UUID escrito no código (`MkU3OEVBNTcwNTJENDM2Qk`), que é justamente o UUID de exemplo da documentação do Kafka, usado por qualquer cluster de tutorial.

**O que foi feito.**

| Arquivo | Alteração |
|---------|-----------|
| `k8s/kafka/kafka-config.yaml` (novo) | ConfigMap `kafka-config` com `KAFKA_CLUSTER_ID`, com um UUID novo gerado para este projeto. |
| `k8s/kafka/kafka-statefulset.yaml` | O container lê `kafka-config` via `envFrom`. |
| `k8s/kafka/kafka-scripts.yaml` | O `kafka-storage format` usa `$KAFKA_CLUSTER_ID`. Se a variável não existir, o script para com uma mensagem clara. |
| `Makefile` | `deploy-kafka` aplica o `kafka-config.yaml` antes do `kafka-scripts.yaml`. |
| `docs/fase2.md`, `fase3.md`, `README.md` | Descrevem o novo arquivo. |

**O ID não é segredo**, é só um identificador; por isso fica em ConfigMap e no Git.

**Quando trocar.** Somente ao recriar o cluster do zero (ambiente novo ou apagar todos os dados). Entre execuções normais, ele permanece o mesmo. Se um dia o Kafka ganhar volume persistente, trocar o ID exigirá apagar os volumes antes. Para gerar outro: `kafka-storage random-uuid` (dentro de um pod Kafka) ou, fora do cluster, `python3 -c "import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))"`.

#### Como validar (A e B)

Sem cluster, foi verificado: sintaxe dos YAMLs (PyYAML), `bash -n` do script KRaft, `py_compile` dos três serviços e o comportamento do alvo `secrets` com um `kubectl` simulado (1ª execução cria com 24 caracteres; 2ª mantém; `db-password` retorna a mesma senha). **Ainda falta testar no k3s:** passos 2 (Secret) e 3 (UUID) do [Roteiro de validação](#roteiro-de-validação-no-cluster).

### Correção de documentação: o que o `make clean` realmente apaga (2026-09-25)

**Problema.** O `docs/fase3.md` afirmava que o `make clean` deleta "absolutamente todos os artefatos (incluindo discos de volume persistente...)". Isso não é verdade: o `clean` roda `kubectl delete -f k8s/apps/` e `kubectl delete -f k8s/kafka/`, que removem só os recursos declarados nesses arquivos. Por padrão, o Kubernetes **mantém** os PVCs criados por `volumeClaimTemplates` quando o StatefulSet é deletado, e o Secret `postgres-credentials` não está em nenhum manifesto (é criado pelo `make secrets`).

**O que foi feito.** Somente texto, em `docs/fase3.md` (seção "Teardown Rápido"): passou a listar o que o `clean` apaga, o que ele **não** apaga (PVC do Postgres e Secret) e como recomeçar do zero (`make db-reset` e apagar o Secret juntos). Nenhum código ou manifesto foi alterado.

**Por que importa.** Depois de um `make clean` e de um novo `make all`, os dados do Postgres e a senha continuam os mesmos. Quem esperava um ambiente zerado (por exemplo, para um teste de demonstração) veria dados antigos na tabela `leituras_sensores`.

**Como validar (no cluster).** Passo 4 do [Roteiro de validação](#roteiro-de-validação-no-cluster).

### Item 6 (parte logs): scripts de teste passam a salvar evidências em `logs/` (2026-09-25)

**Problema.** Os scripts `test_broker_failover.sh`, `test_consumer_rebalance.sh` e `test_elasticity.sh` apenas executavam a falha e imprimiam "o que observar". Nada era salvo, mas o enunciado exige como entregáveis os "logs de execução mostrando rebalanço" e a demonstração de elasticidade, e o relatório precisa de evidências.

**O que foi feito.**

| Arquivo | Alteração |
|---------|-----------|
| `scripts/lib_logs.sh` (novo) | Funções compartilhadas, carregadas com `source`: `log_init` (cria `logs/<teste>_<data>_<hora>.log`), `log_msg`, `log_section`, `log_cmd` (executa um comando mostrando-o e gravando a saída; registra o código de saída e usa `timeout` para não travar se o Kafka cair) e helpers `kafka_describe_group`, `kafka_describe_topic`, `db_contagem_leituras`, `kafka_pod_vivo`. Pasta de logs, tópico, grupo, timeouts e usuário/banco do Postgres vêm de variáveis de ambiente (com padrões), sem valores fixos no código. |
| `scripts/test_consumer_rebalance.sh` | Continua deletando um consumidor, mas salva ANTES (pods e partições por consumidor), o final do log do pod derrubado, a FALHA e o DEPOIS (após `ESPERA_REBALANCE_SEG`, padrão 20 s) com os logs dos sobreviventes. |
| `scripts/test_broker_failover.sh` | Continua deletando à força um broker, mas salva ANTES/DEPOIS de: líder, réplicas e ISR das partições, LAG do grupo, total de leituras no banco e logs dos consumidores. Os comandos do DEPOIS rodam no broker **sobrevivente** (`kafka_pod_vivo` exclui o pod derrubado). |
| `scripts/test_elasticity.sh` | Antes escalava produtores e consumidores ao mesmo tempo. Agora são duas fases (produtores, depois consumidores), cada uma com amostras periódicas do LAG (`AMOSTRAS`, `INTERVALO_AMOSTRA_SEG`). Réplicas e nomes dos deployments vêm de variáveis, com os mesmos padrões de antes (3 e 4). |
| `docs/fase4.md` | Nova seção "Evidências (logs salvos)": o que cada script grava, como interpretar e variáveis. |

**Decisões que afetam a interpretação.**
- Os logs dos **produtores não são usados como prova de que o sistema continuou funcionando**: o `kafka-python` envia de forma assíncrona e imprime "Enviado" mesmo se a entrega falhar. A prova é o total de leituras no Postgres, o LAG e o líder/ISR das partições.
- Os scripts **não escondem falhas**: se o Kafka estiver indisponível durante o teste, o comando correspondente aparece no log com seu código de saída (124 = estourou o timeout). Isso é intencional: com os problemas atuais dos itens 1 e 2 (replicação 1 e quórum de 2 nós), o teste de broker deve mostrar partições sem líder, e esse resultado documenta o problema.
- `logs/` **não** está no `.gitignore`: os logs usados no relatório devem ser versionados.

**Limitação.** O `test_interactive.sh` chama os scripts de rebalanço e de failover (que agora geram log), mas sua fase de elasticidade usa `make scale-*` e não gera log. Para essa evidência, rode `./scripts/test_elasticity.sh`.

**Como validar.** Sem cluster, foi verificado: `bash -n` em todos os scripts e a execução dos três com um `kubectl` simulado (arquivos criados, seções corretas, escolha do broker sobrevivente, registro do erro de um comando que falha e número de amostras). **Ainda falta testar no k3s:** passo 5 do [Roteiro de validação](#roteiro-de-validação-no-cluster), principalmente a coluna `CONSUMER-ID` e a saída do `kafka-topics --describe`.

### Correção no README: matrícula de Gabriel Martins Mendes (2026-09-25)

A tabela de integrantes do `README.md` tinha o marcador `231XXXX` no lugar da matrícula de Gabriel Martins Mendes. Foi substituído por `2311271`. Alteração de uma linha, sem impacto em código ou manifestos. Validação (só texto): `grep -n 2311271 README.md` deve retornar uma linha, na tabela de integrantes.
