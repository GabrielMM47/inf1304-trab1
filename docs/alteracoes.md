# Registro de Alterações

Este documento registra, em ordem cronológica, as correções feitas no projeto após a revisão dos requisitos do enunciado (`Trab1_2026-1.pdf`). Cada entrada diz **o que mudou**, **por quê** e **quais arquivos** foram tocados, para que todos do grupo consigam acompanhar e validar.

## Plano de correções

A revisão do repositório contra o enunciado gerou uma lista de itens. Os números abaixo são usados nas entradas do registro.

| # | Item | Situação |
|---|------|----------|
| 1 | Replicação do tópico `dados-sensores` (era `--replication-factor 1` no Job de criação de tópicos) | **Concluído no código** (fator 3, 6 partições). Falta evidência em log com a configuração nova |
| 2 | Quórum KRaft: 2 brokers/controllers não toleravam a perda de um nó (o Raft exige maioria) | **Concluído no código** (3 brokers/controllers). Falta evidência em log com a configuração nova |
| 3 | Kafka sem volume persistente (`log.dirs=/tmp`, sem `volumeClaimTemplates`) | Não aplicado. Com fator de replicação 3, um broker recriado recupera os dados das réplicas dos outros; registrar como limitação no relatório |
| 4 | Teste de falha de broker (script apenas deletava o pod, sem verificar nem salvar evidência) | Script concluído (ver item 6). **Os logs versionados são anteriores aos itens 1 e 2 e precisam ser refeitos** |
| 5 | Log de rebalanço no consumidor (`ConsumerRebalanceListener`, partição em cada leitura) | Não implementado. A biblioteca `kafka-python` já registra no log do consumidor as partições revogadas e atribuídas a cada rebalanço (`Revoking previously assigned partitions` / `Setting newly assigned partitions`), o que serve de evidência |
| 6 | Logs salvos pelos scripts de teste e relatório final | Scripts salvam logs (**concluído**). Relatório em andamento na branch `relatorio`, em Typst (`docs/relatorio/`); faltam resultados e conclusão, que dependem dos testes novos |
| 7 | Documentação citando Zookeeper, removido do projeto (Kafka roda em KRaft) | **Concluído** |
| 8 | Constantes hard-coded e docstrings faltantes (itens pontuados no enunciado) | Em andamento (escopo parcial): tempo de processamento, UUID do cluster, senha do Postgres e docstrings feitos; faltam perfis de limites e faixas de valores do sensor |

Ordem combinada de execução: 7, depois 8 (parcial), depois a parte de salvamento de logs do 6, depois 1, 2, 4 e 5, e por fim o relatório do 6. Os itens 1 e 2 estão descritos na entrada [Itens 1 e 2 e ajustes nos serviços](#itens-1-e-2-e-ajustes-nos-serviços-2026-09-26).

---

## Pontos a verificar

Observações feitas na revisão que ainda **não foram confirmadas** com execução real. Cada ponto deve ser verificado nos testes e, se confirmado, refletido na documentação e no relatório. Ao concluir um ponto, registre o resultado abaixo dele.

### 1. Chave da mensagem, partições e elasticidade

**Contexto.** O sensor usa o identificador da máquina como chave da mensagem (`sensor.py`), então todas as leituras de uma máquina vão para a mesma partição. Dentro do grupo `sensor-group`, cada partição é lida por um único consumidor por vez. O script `test_elasticity.sh` escala `producer-maquina-1` para mais réplicas, e todas usam o mesmo `MAQUINA_ID`, ou seja, a mesma chave.

**O que pode acontecer (hipóteses).**
- Na Fase 1 (escala dos produtores), o `LAG` cresce **concentrado em uma partição**, a da chave `maquina-1`, e não distribuído.
- Na Fase 2 (escala dos consumidores), o `LAG` dessa partição **não diminui** com consumidores extras, porque ela continua sendo lida por um só consumidor. Com 3 partições, consumidores além do terceiro ficam ociosos.
- Com só 3 chaves (`maquina-1`, `-2`, `-3`) e 3 partições, o hash pode colocar duas máquinas na mesma partição e deixar outra sem tráfego.

**Como verificar.** Em `logs/elasticidade_*.log`, comparar o `LAG` **por partição** (não só o total) entre as amostras das duas fases: em qual partição ele cresce, qual `CONSUMER-ID` a lê e se o total cai na Fase 2. Conferir também se alguma partição fica sem mensagens novas.

**Se confirmado.**
- Registrar no relatório (seção de resultados) que o efeito vem da chave por máquina, e não de falha do Kafka.
- Alternativas para uma demonstração de elasticidade mais fiel a "mais sensores": escalar máquinas com identificadores diferentes (novos Deployments com outro `MAQUINA_ID`), ou deixar de usar a chave (perde a ordem por máquina; o histórico de alertas já fica no Redis compartilhado, então a lógica de alertas continua funcionando).

**Documentação a revisar depois da verificação.**
- `docs/fase4.md`, seção 3: afirma que o Kafka "espalhará as requisições de leitura sobre esses 4 *workers*" e que as filas não travam. Isso não vale integralmente com 3 partições e uma única chave em carga.
- `docs/relatorio/main.typ` (branch `relatorio`): seções "Sensores (produtores)" (uso da chave) e "Elasticidade".

## Roteiro de validação no cluster

Este é o roteiro único para testar tudo o que foi alterado. **Cada nova entrada do registro deve acrescentar seus passos aqui.** Legenda: ✅ resultado esperado; ⚠️ o que indica problema. Marque as caixas e preencha a tabela de resultados ao final ao executar.

> **Situação em 26/09:** os scripts de teste já foram executados no cluster (logs em `logs/` e `tests_execution.log`), mas **antes** da correção dos itens 1 e 2: esses logs mostram o tópico com `PartitionCount: 1` e `ReplicationFactor: 1`. **Os passos abaixo precisam ser executados de novo com a configuração atual**, e a tabela de resultados ao final ainda não foi preenchida.

### Pré-requisitos

- Máquina Debian com Docker e k3s (`make init` e `make start-cluster`), na raiz do repositório, com `kubectl` funcionando.
- Os scripts de `scripts/` já estão versionados com permissão de execução, e o `make init` a aplica de novo a todos (ver [Problema resolvido: scripts sem permissão de execução](#problema-resolvido-scripts-sem-permissão-de-execução)).
- Tempo estimado: 15 a 20 minutos no total.

### Problema resolvido: scripts sem permissão de execução

Até 25/09, todos os arquivos de `scripts/` estavam versionados com o modo `100644`, sem permissão de execução. Numa clonagem nova, `./scripts/test_*.sh` falhava com `Permission denied`, e no `make test-all` as fases de rebalanço e de failover falhavam sem gerar log, porque o alvo só dava `chmod` no `test_interactive.sh`.

**Situação: resolvido em 26/09.** Os scripts passaram a ser versionados como executáveis (modo `100755`) e o `make init` executa `chmod +x scripts/*.sh`. Para conferir: `git ls-files -s scripts` deve mostrar `100755` em todos os arquivos. O contorno manual (`chmod +x scripts/*.sh`) não é mais necessário.

### Passo 0: subir o ambiente

```bash
make all          # build das imagens + deploy (Kafka e aplicações)
make status       # repita até estabilizar (1 a 2 minutos)
```
- [ ] ✅ Pods `Running`: `kafka-0`, `kafka-1`, `kafka-2`, `postgres-0`, `redis`, `adminer`, 2 `controlador`, 3 `producer-*`, 3 `consumer`. O pod do Job `kafka-init-topics` fica `Completed`. Produtores e consumidores ficam em `Init` até o tópico `dados-sensores` existir (contêiner de inicialização `wait-for-kafka`); isso é esperado nos primeiros segundos.
- ⚠️ `CreateContainerConfigError` indica Secret ausente (rode `make secrets`); `CrashLoopBackOff` em `kafka-*`, veja `kubectl logs kafka-0`.

### Passo 1: tempo de processamento configurável (item 8, `sleep`)

```bash
kubectl exec deploy/consumer -- env | grep TEMPO_PROCESSAMENTO
```
- [ ] ✅ `TEMPO_PROCESSAMENTO_MIN_SEG=0.4` e `TEMPO_PROCESSAMENTO_MAX_SEG=0.8` (valores atuais do `consumer-config`).

Teste de comportamento: em `k8s/apps/configmap.yaml`, mude ambos para `"3.0"` no `consumer-config`, e rode:
```bash
kubectl apply -f k8s/apps/configmap.yaml && kubectl rollout restart deployment consumer
kubectl logs -l app=consumer --timestamps --tail=10
```
- [ ] ✅ As linhas "Leitura recebida..." de um mesmo consumidor passam a ter cerca de 3 s entre si. Depois **volte os valores para 0.4 e 0.8**, aplique e reinicie de novo.

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
for i in 0 1 2; do kubectl logs kafka-$i | head -20; done
for i in 0 1 2; do kubectl exec kafka-$i -- cat /tmp/kraft-logs/meta.properties; done
kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list
```
- [ ] ✅ Os logs dos três brokers mostram `Formatando logs KRaft (cluster ID: zrH_urXmRdCh86VyykE9Vg)`; os três `meta.properties` têm o **mesmo** `cluster.id=zrH_urXmRdCh86VyykE9Vg`; a listagem mostra `dados-sensores` e `comandos-fabrica`. ⚠️ `KAFKA_CLUSTER_ID não definido` indica que o `kafka-config.yaml` não foi aplicado antes do StatefulSet.

### Passo 4: o que o `make clean` mantém (correção de documentação)

```bash
make clean
kubectl get pvc
kubectl get secret postgres-credentials
```
- [ ] ✅ O PVC `pg-data-postgres-0` **ainda existe** e o Secret **ainda existe**. Os Deployments de máquinas criadas durante a execução (`make scale-producers`) **foram removidos** (`kubectl get deployments` não lista nenhum `producer-*`).
```bash
make db-reset
kubectl get pvc
make all
```
- [ ] ✅ Depois do `db-reset` o PVC some; após o novo `make all` e alguns segundos, `SELECT count(*) FROM leituras_sensores;` recomeça com valor baixo (banco zerado) e as aplicações conectam normalmente (mesmo Secret).

### Passo 5: scripts de teste salvando logs (item 6)

Pré-condição: passo 0 e **passo 6** concluídos (o tópico precisa estar com 6 partições e fator 3, senão os logs repetem o problema dos logs antigos). Os arquivos aparecem em `logs/`.

**5a. Rebalanço de consumidor** (cerca de 1 minuto):
```bash
./scripts/test_consumer_rebalance.sh
ls logs/
```
- [ ] ✅ Cria `logs/rebalanco_consumidor_<data>_<hora>.log` com 5 seções (ANTES, últimas linhas do pod derrubado, FALHA, DEPOIS, logs dos consumidores). No ANTES, a tabela do grupo mostra as **6** partições distribuídas entre os **3** `CONSUMER-ID` (duas partições por consumidor). No DEPOIS, nenhuma partição fica com `CONSUMER-ID` `-`, e as do pod derrubado passaram para outro consumidor.

**5b. Elasticidade** (cerca de 2 a 3 minutos):
```bash
./scripts/test_elasticity.sh
```
- [ ] ✅ Cria `logs/elasticidade_*.log` com 12 amostras (6 por fase). Na Fase 1 o `LAG` tende a **subir**; na Fase 2, a **cair**. Se o LAG não crescer, aumente o tempo de processamento (passo 1) para deixar o efeito visível. Com 4 consumidores e 6 partições, todos recebem partições. Ao final, volte ao estado original: `kubectl scale deployment producer-maquina-1 --replicas=1 && kubectl scale deployment consumer --replicas=3`.

**5c. Failover de broker** (cerca de 1 minuto):
```bash
./scripts/test_broker_failover.sh
```
- [ ] ✅ Cria `logs/failover_broker_*.log`. No ANTES, o `kafka-topics --describe` mostra `PartitionCount: 6` e `ReplicationFactor: 3`, com `Isr` de 3 brokers. No DEPOIS, todas as partições têm `Leader` diferente de `-1` e de `0` (o broker derrubado), o `Isr` foi reduzido para os brokers sobreviventes e a contagem de leituras no banco **cresceu** entre ANTES e DEPOIS. ⚠️ Se o log mostrar `PartitionCount: 1` ou `ReplicationFactor: 1`, o tópico não foi recriado com a configuração nova (ver passo 6).

**5d. Script interativo:**
```bash
make test-all
```
- [ ] ✅ Cria `logs/interactive_*.log` com toda a execução, e as fases de rebalanço e de failover geram também seus próprios arquivos em `logs/`. Na fase de elasticidade, o roteiro cria novas máquinas (`make scale-producers`, 5 no total) e eleva os consumidores para 10 (`make scale-consumers`); com 6 partições, **4 consumidores ficam sem partição**, o que é esperado. Ao final o roteiro deixa `producer-maquina-1` com 1 réplica e os consumidores com **5** (e não 3, como no manifesto).

### Passo 6: partições, replicação e quórum (itens 1 e 2)

Executar **antes** do passo 5.
```bash
kubectl get pods -l app=kafka
kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --describe --topic dados-sensores
kubectl exec kafka-0 -- kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status
```
- [ ] ✅ Três pods `kafka-*` em `Running`. O tópico mostra `PartitionCount: 6` e `ReplicationFactor: 3`, e cada partição lista 3 brokers em `Replicas` e em `Isr`, com líderes distribuídos entre os três. O quórum lista 3 votantes (`CurrentVoters` com os ids 0, 1 e 2). ⚠️ `PartitionCount: 1` indica que o tópico foi criado automaticamente antes do Job (ou que o Job não conseguiu recriá-lo): rode `kubectl delete job kafka-init-topics && kubectl apply -f k8s/kafka/kafka-init-job.yaml` e confira de novo.

### Registro de resultados

Preencha ao executar (data, quem testou, resultado e observações).

| Passo | Data | Quem | Resultado (OK / falhou) | Observações |
|-------|------|------|-------------------------|-------------|
| 0 Subir ambiente | | | | |
| 1 Tempo de processamento | | | | |
| 2 Secret do Postgres | | | | |
| 3 UUID do cluster | | | | |
| 4 `make clean` / `db-reset` | | | | |
| 6 Partições, replicação e quórum | | | | |
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
- A frase do `fase4.md` descreve o comportamento esperado do failover. Ele só é de fato garantido depois dos itens 1 e 2. *(Atualização 26/09: itens 1 e 2 feitos no código; falta confirmar em log.)*

### Item 8 (parcial): Tempo de processamento simulado configurável (2026-09-25)

**Problema.** O consumidor simulava o processamento pesado com `time.sleep(random.uniform(0.5, 1.5))` fixo em `processor.py`. O enunciado pontua a ausência de constantes hard-coded, e alterar esse tempo (por exemplo, para deixar o lag mais visível no teste de elasticidade) exigia mexer no código e reconstruir a imagem.

**O que foi feito.** O intervalo passou a vir de variáveis de ambiente, seguindo o mesmo padrão das demais configurações do consumidor.

| Arquivo | Alteração |
|---------|-----------|
| `src/consumer/processor.py` | `obter_configuracao()` lê `TEMPO_PROCESSAMENTO_MIN_SEG` e `TEMPO_PROCESSAMENTO_MAX_SEG` (padrão 0.5 e 1.5, iguais ao valor antigo). `processar_mensagem()` usa esses valores no `sleep`. Docstring e comentário atualizados. |
| `k8s/apps/configmap.yaml` | As duas variáveis foram adicionadas ao `consumer-config`. |
| `docs/fase1.md` | Nova linha descrevendo o tempo de processamento simulado. |

**Comportamento.** Sem mudança com os valores da época (0,5 a 1,5 s). *(Atualização 26/09: os valores foram reduzidos para 0,4 a 0,8 s no `consumer-config`, e os padrões do código para 0,2 a 0,5 s.)* Para alterar, edite o ConfigMap e reinicie os consumidores (`kubectl apply -f k8s/apps/configmap.yaml && kubectl rollout restart deployment consumer`); não é preciso rebuild.

**Como validar.** Ver o passo 1 do [Roteiro de validação](#roteiro-de-validação-no-cluster).

**Pendente no item 8 (na época desta entrada).** UUID do cluster e senha do Postgres foram tratados na entrada seguinte, e os docstrings em 26/09. Continuam pendentes: perfis de limites e faixas de valores do sensor (ainda no código).

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
- Os scripts **não escondem falhas**: se o Kafka estiver indisponível durante o teste, o comando correspondente aparece no log com seu código de saída (124 = estourou o timeout). Isso é intencional: um teste que falha também é resultado e deve aparecer no log.
- `logs/` **não** está no `.gitignore`: os logs usados no relatório devem ser versionados.

**Limitação (na época desta entrada).** O `test_interactive.sh` não gerava log na fase de elasticidade. *(Atualização 26/09: o roteiro interativo passou a gravar toda a saída em `logs/interactive_*.log`, e a `lib_logs.sh` passou a redirecionar toda a saída do script para o arquivo com `exec > >(tee ...)`, em vez de usar `tee` em cada comando.)*

**Como validar.** Sem cluster, foi verificado: `bash -n` em todos os scripts e a execução dos três com um `kubectl` simulado (arquivos criados, seções corretas, escolha do broker sobrevivente, registro do erro de um comando que falha e número de amostras). **Ainda falta testar no k3s:** passo 5 do [Roteiro de validação](#roteiro-de-validação-no-cluster), principalmente a coluna `CONSUMER-ID` e a saída do `kafka-topics --describe`.

### Correção no README: matrícula de Gabriel Martins Mendes (2026-09-25)

A tabela de integrantes do `README.md` tinha o marcador `231XXXX` no lugar da matrícula de Gabriel Martins Mendes. Foi substituído por `2311271`. Alteração de uma linha, sem impacto em código ou manifestos. Validação (só texto): `grep -n 2311271 README.md` deve retornar uma linha, na tabela de integrantes.

### Itens 1 e 2 e ajustes nos serviços (2026-09-26)

**Problema.** O tópico tinha fator de replicação 1 e o quórum KRaft tinha 2 nós, então a queda de um broker não era tolerada (itens 1 e 2). Além disso, produtores e consumidores podiam iniciar antes de o tópico existir, e a queda ou remoção de um consumidor demorava a ser percebida pelo grupo.

**Itens 1 e 2: cluster Kafka.**

| Arquivo | Alteração |
|---------|-----------|
| `k8s/kafka/kafka-statefulset.yaml` | 3 réplicas (`kafka-0` a `kafka-2`). Tópicos internos com fator de replicação 3 e mínimo de 2 réplicas em sincronia (`KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`). Criação automática de tópicos desativada (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`). |
| `k8s/kafka/kafka-scripts.yaml` | `controller.quorum.voters` com os 3 nós: com 3 votantes, a maioria é 2, e o quórum tolera a perda de um. |
| `k8s/kafka/kafka-init-job.yaml` | Os tópicos `dados-sensores` e `comandos-fabrica` são **apagados e recriados** a cada deploy, com 6 partições e fator de replicação 3. Consequência: cada `make deploy` começa com os tópicos vazios. |
| `scripts/init_topic.sh` | Script alternativo com `--replication-factor 5`, o que **falharia** com 3 brokers (o fator não pode ser maior que o número de brokers). Não é usado pelo Makefile. |

**Produtores e consumidores.**

| Arquivo | Alteração |
|---------|-----------|
| `src/producer/sensor.py` | **As mensagens deixaram de ter chave** (`key=maquina_id` removido): o cliente distribui as mensagens entre as partições, e as de uma mesma máquina podem ir para consumidores diferentes; perde-se a ordem por máquina. Tratamento de `SIGTERM` para encerrar de forma controlada. Nível de log configurável (`DEBUG_MODE`). |
| `src/consumer/processor.py` | Confirmação manual da posição de leitura (`enable_auto_commit=False`, `commit()` após cada mensagem). Detecção de falha mais rápida (`session_timeout_ms=10000`, `heartbeat_interval_ms=3000`). Tratamento de `SIGTERM`. Reconexão ao Postgres (`check_and_reconnect_pg`). Erro em uma mensagem não derruba o consumidor. |
| `k8s/apps/consumer-deployment.yaml` | 3 réplicas e contêiner de inicialização que espera o tópico existir. |
| `k8s/apps/producer-deployment.yaml` | Contêiner de inicialização que espera o tópico. Novos intervalos base: `maquina-1` 10 s, `maquina-2` 5 s, `maquina-3` 25 s. |
| `k8s/apps/configmap.yaml` | `DEBUG_MODE` nos três ConfigMaps. Janela de alertas de 30 s. Tempo de processamento de 0,4 a 0,8 s. |

**Makefile e scripts.**

| Arquivo | Alteração |
|---------|-----------|
| `Makefile` | `init` dá `chmod +x` em todos os scripts. `deploy-apps` aceita `DEBUG=1`. `scale-producers MACHINES=N` cria máquinas novas (Deployments) até N (padrão 5), via `scripts/scale_producers.sh`. Novo `scale-machine MAQUINA=M P_REPLICAS=R`. `scale-consumers C_REPLICAS=N` (padrão 10). `clean` também remove as máquinas criadas dinamicamente. `kafka-lag` usa `scripts/inspect_queue.sh`, que consulta qualquer broker em execução. |
| `scripts/test_interactive.sh` | Grava toda a saída em `logs/interactive_*.log`, espera o grupo de consumo voltar ao estado `Stable` após cada rebalanço e usa os novos alvos de escala. |
| `scripts/*.sh` | Versionados como executáveis (`100755`). |
| `logs/`, `tests_execution.log` | Logs de execução dos testes. **Atenção:** são de 26/09 por volta de 01:00, **antes** da correção dos tópicos, e mostram `PartitionCount: 1` e `ReplicationFactor: 1`. Não servem como evidência dos itens 1 e 2. |

**Pendências e cuidados observados.**
- **Refazer os testes** com a configuração atual e versionar os logs novos (passos 6 e 5 do roteiro). Ainda não existe nenhum `logs/elasticidade_*.log`.
- Nos logs antigos, o grupo `sensor-group` entrava em rebalanço a cada poucos segundos (`Group sensor-group is rebalancing`). Verificar nos testes novos se isso ainda acontece com os ajustes de `session_timeout_ms` e `heartbeat_interval_ms`.
- O `make deploy-apps` altera o arquivo versionado `k8s/apps/configmap.yaml` com `sed -i` para ligar ou desligar o `DEBUG_MODE`. Depois de um deploy com `DEBUG=1`, o arquivo fica modificado e pode ser commitado por engano; confira `git status` antes de commitar.
- Comentários enganosos em `k8s/apps/producer-deployment.yaml`: "Máquina mais fria", "Muita vibração!", "Super quente, gera anomalias!" e "Gasta muita energia". Os `MULTIPLICADOR_*` só mudam o **intervalo** de envio de cada tipo de sensor (quanto maior, menos frequente); os valores são sorteados nas mesmas faixas para todas as máquinas (`gerar_dados` em `sensor.py`).
- `docs/fase2.md` ainda cita `replicas: 2` para o Kafka e para os consumidores.

### Item 8 (parcial): docstrings nas funções que não tinham (2026-09-26)

**Problema.** O enunciado pontua a documentação do código com DocString. Das 22 funções dos três serviços, 9 não tinham docstring, algumas delas adicionadas recentemente (como `check_and_reconnect_pg`).

**O que foi feito.** Apenas docstrings; nenhuma linha de lógica foi alterada. O estilo segue o dos docstrings existentes (português, seções "Argumentos:" e "Retorna:").

| Arquivo | Funções documentadas |
|---------|----------------------|
| `src/consumer/processor.py` | `check_and_reconnect_pg`, `log_event`, `obter_limite` (função interna de `obter_configuracao`) |
| `src/controlador/controlador.py` | `obter_configuracao`, `init_postgres`, `log_event`, `main` |
| `src/producer/sensor.py` | `init_postgres`, `log_event` |

**Como validar (sem cluster).**
- `python3 -m py_compile src/*/*.py` deve terminar sem erros.
- Cobertura: o comando abaixo deve mostrar `10/10`, `6/6` e `6/6`.
  ```bash
  python3 -c "import ast,glob
  for f in sorted(glob.glob('src/*/*.py')):
      fs=[n for n in ast.walk(ast.parse(open(f).read())) if isinstance(n,ast.FunctionDef)]
      print(f, sum(1 for n in fs if ast.get_docstring(n)), '/', len(fs))"
  ```
- Como só foram adicionados comentários, não é preciso reconstruir as imagens para validar; na próxima execução do `make build` elas passam a incluir os docstrings.
