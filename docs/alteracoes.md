# Registro de Alterações

Este documento registra, em ordem cronológica, as correções feitas no projeto após a revisão dos requisitos do enunciado (`Trab1_2026-1.pdf`). Cada entrada diz **o que mudou**, **por quê** e **quais arquivos** foram tocados, para que todos do grupo consigam acompanhar e validar.

## Plano de correções

A revisão do repositório contra o enunciado gerou uma lista de itens. Os números abaixo são usados nas entradas do registro.

| # | Item | Situação |
|---|------|----------|
| 1 | Replicação do tópico `dados-sensores` (era `--replication-factor 1` no Job de criação de tópicos) | **Concluído** (fator 3, 12 partições). Evidência em `logs/failover_broker_20260926_180852.log` |
| 2 | Quórum KRaft: 2 brokers/controllers não toleravam a perda de um nó (o Raft exige maioria) | **Concluído** (3 brokers/controllers). Evidência em `logs/failover_broker_20260926_180852.log` |
| 3 | Kafka sem volume persistente (`log.dirs=/tmp`, sem `volumeClaimTemplates`) | Não aplicado. Com fator de replicação 3, um broker recriado recupera os dados das réplicas dos outros; registrar como limitação no relatório |
| 4 | Teste de falha de broker (script apenas deletava o pod, sem verificar nem salvar evidência) | **Concluído.** Executado em 26/09 com a configuração nova; resultado no relatório |
| 5 | Log de rebalanço no consumidor (`ConsumerRebalanceListener`, partição em cada leitura) | Não implementado. A biblioteca `kafka-python` já registra no log do consumidor as partições revogadas e atribuídas a cada rebalanço (`Revoking previously assigned partitions` / `Setting newly assigned partitions`), o que serve de evidência |
| 6 | Logs salvos pelos scripts de teste e relatório final | Scripts salvam logs (**concluído**). Relatório em Typst (`docs/relatorio/`) com resultados, conclusão e apêndice de logs (**concluído**) |
| 7 | Documentação citando Zookeeper, removido do projeto (Kafka roda em KRaft) | **Concluído** |
| 8 | Constantes hard-coded e docstrings faltantes (itens pontuados no enunciado) | Em andamento (escopo parcial): tempo de processamento, UUID do cluster, senha do Postgres e docstrings feitos; faltam perfis de limites e faixas de valores do sensor |

Ordem combinada de execução: 7, depois 8 (parcial), depois a parte de salvamento de logs do 6, depois 1, 2, 4 e 5, e por fim o relatório do 6. Os itens 1 e 2 estão descritos na entrada [Itens 1 e 2 e ajustes nos serviços](#itens-1-e-2-e-ajustes-nos-serviços-2026-09-26).

---

## Pontos a verificar

Observações feitas na revisão que precisavam ser confirmadas com execução real.

### 1. Chave da mensagem, partições e elasticidade

**Situação: resolvido.** A hipótese era que a chave por máquina (`key=maquina_id`) concentraria a carga de `maquina-1` em uma única partição, limitando o efeito de escalar consumidores. O sensor deixou de usar chave (ver a entrada [Itens 1 e 2 e ajustes nos serviços](#itens-1-e-2-e-ajustes-nos-serviços-2026-09-26)), e no ensaio de 26/09 às 18:07 o _lag_ se distribuiu entre as 12 partições (`logs/interactive_20260926_180644.log`).

---

## Roteiro de validação no cluster

Este é o roteiro único para testar tudo o que foi alterado. **Cada nova entrada do registro deve acrescentar seus passos aqui.** Legenda: ✅ resultado esperado; ⚠️ o que indica problema. Marque as caixas e preencha a tabela de resultados ao final ao executar.

> **Situação em 26/09:** os passos 5a, 5c, 5d e parte do 6 foram cobertos pela execução de 26/09 entre 18:06 e 18:09, com o tópico em 12 partições e fator 3 (logs em `logs/`; resumo na tabela de resultados). Os logs antigos, anteriores à correção dos itens 1 e 2, foram removidos. Os demais passos ainda não têm registro de execução.

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
- [ ] ✅ `TEMPO_PROCESSAMENTO_MIN_SEG=0.2` e `TEMPO_PROCESSAMENTO_MAX_SEG=0.5` (valores atuais do `consumer-config`; os consumidores extras usam 0.1 e 0.2, do `consumer-extra-config`).

Teste de comportamento: em `k8s/apps/configmap.yaml`, mude ambos para `"3.0"` no `consumer-config`, e rode:
```bash
kubectl apply -f k8s/apps/configmap.yaml && kubectl rollout restart deployment consumer
kubectl logs -l app=consumer --timestamps --tail=10
```
- [ ] ✅ As linhas "Leitura recebida..." de um mesmo consumidor passam a ter cerca de 3 s entre si. Depois **volte os valores para 0.2 e 0.5**, aplique e reinicie de novo.

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

Pré-condição: passo 0 e **passo 6** concluídos (o tópico precisa estar com 12 partições e fator 3). Os arquivos aparecem em `logs/`.

**5a. Rebalanço de consumidor** (cerca de 1 minuto):
```bash
./scripts/test_consumer_rebalance.sh
ls logs/
```
- [ ] ✅ Cria `logs/rebalanco_consumidor_<data>_<hora>.log` com 5 seções (ANTES, últimas linhas do pod derrubado, FALHA, DEPOIS, logs dos consumidores). No ANTES, a tabela do grupo mostra as **12** partições distribuídas entre os consumidores em execução (com 3 consumidores, quatro partições cada; com 12, uma cada). No DEPOIS, nenhuma partição fica com `CONSUMER-ID` `-`, e as do pod derrubado passaram para outro consumidor. A consulta do DEPOIS é feita 2 s após a remoção (`ESPERA_REBALANCE_SEG`) e costuma ainda mostrar `is rebalancing`; a conclusão do rebalanço aparece nos logs dos consumidores (`Successfully joined group ... Generation N` e `Setting newly assigned partitions`). Para registrar a tabela já estável, rode com `ESPERA_REBALANCE_SEG=20`.

**5b. Elasticidade** (cerca de 2 a 3 minutos):
```bash
./scripts/test_elasticity.sh
```
- [ ] ✅ Cria `logs/elasticidade_*.log` com 12 amostras (6 por fase). Na Fase 1 o `LAG` tende a **subir**; na Fase 2, a **cair**. Se o LAG não crescer, aumente o tempo de processamento (passo 1) para deixar o efeito visível. Com 4 consumidores e 12 partições, todos recebem partições. Ao final, volte ao estado original: `kubectl scale deployment producer-maquina-1 --replicas=1 && kubectl scale deployment consumer --replicas=3`.

**5c. Failover de broker** (cerca de 1 minuto):
```bash
./scripts/test_broker_failover.sh
```
- [ ] ✅ Cria `logs/failover_broker_*.log`. No ANTES, o `kafka-topics --describe` mostra `PartitionCount: 12` e `ReplicationFactor: 3`, com `Isr` de 3 brokers. No DEPOIS, todas as partições têm `Leader` diferente de `-1` e de `0` (o broker derrubado), o `Isr` foi reduzido para os brokers sobreviventes e a contagem de leituras no banco **cresceu** entre ANTES e DEPOIS. ⚠️ Se o log mostrar `PartitionCount: 1` ou `ReplicationFactor: 1`, o tópico não foi recriado com a configuração nova (ver passo 6).

**5d. Script interativo:**
```bash
make test-all
```
- [ ] ✅ Cria `logs/interactive_*.log` com toda a execução, e as fases de rebalanço e de failover geram também seus próprios arquivos em `logs/`. Na fase de elasticidade, o roteiro ajusta as máquinas para 8 (`make scale-producers`) e os consumidores para 12 (`make scale-consumers`: 3 no Deployment `consumer` e 9 `consumer-extra-N`); com 12 partições, cada consumidor recebe uma. O _lag_ deve subir após a escala dos produtores e cair após a dos consumidores. Ao final, o roteiro volta para 3 máquinas e 3 consumidores (os extras são removidos).

### Passo 6: partições, replicação e quórum (itens 1 e 2)

Executar **antes** do passo 5.
```bash
kubectl get pods -l app=kafka
kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --describe --topic dados-sensores
kubectl exec kafka-0 -- kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status
```
- [ ] ✅ Três pods `kafka-*` em `Running`. O tópico mostra `PartitionCount: 12` e `ReplicationFactor: 3`, e cada partição lista 3 brokers em `Replicas` e em `Isr`, com líderes distribuídos entre os três. O quórum lista 3 votantes (`CurrentVoters` com os ids 0, 1 e 2). ⚠️ `PartitionCount: 1` indica que o tópico foi criado automaticamente antes do Job (ou que o Job não conseguiu recriá-lo): rode `kubectl delete job kafka-init-topics && kubectl apply -f k8s/kafka/kafka-init-job.yaml` e confira de novo.

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

**Execução de 26/09 (18:06 a 18:09).** Coberta pelos logs `logs/interactive_20260926_180644.log`, `logs/rebalanco_consumidor_20260926_180813.log` e `logs/failover_broker_20260926_180852.log`:
- **5a (rebalanço):** 12 consumidores (3 + 9 extras), uma partição cada. A partição 1, do pod removido, passou a outro consumidor; nenhuma partição ficou sem consumidor. Geração 9 do grupo concluída cerca de 4 s depois, com o substituto recebendo a partição 4. A tabela do DEPOIS ainda mostrava `is rebalancing`.
- **5c (failover):** `PartitionCount: 12`, `ReplicationFactor: 3`. Antes: 4 líderes em cada broker, ISR `0,1,2`. Depois de remover `kafka-0`: 12 partições com líder (7 em `kafka-1`, 5 em `kafka-2`), ISR `1,2`; leituras no banco de 56.573 para 56.706 em cerca de 8 s.
- **5d (interativo):** _lag_ total 8 no início; após 8 máquinas, 20, 84, 145, 222 e 290; após 12 consumidores, 361, 84, 30, 13 e 7.
- **6 (parcial):** partições e replicação confirmadas pelo `kafka-topics --describe` dos logs acima; o quórum (`kafka-metadata-quorum`) não foi registrado.
- **5b (`test_elasticity.sh`):** não executado.

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
| `k8s/kafka/kafka-init-job.yaml` | Os tópicos `dados-sensores` e `comandos-fabrica` são **apagados e recriados** a cada deploy, com 6 partições e fator de replicação 3 (depois aumentadas para 12). Consequência: cada `make deploy` começa com os tópicos vazios. |
| `scripts/init_topic.sh` | Script alternativo com `--replication-factor 5`, o que **falharia** com 3 brokers (o fator não pode ser maior que o número de brokers). Não é usado pelo Makefile. |

**Produtores e consumidores.**

| Arquivo | Alteração |
|---------|-----------|
| `src/producer/sensor.py` | **As mensagens deixaram de ter chave** (`key=maquina_id` removido): o cliente distribui as mensagens entre as partições, e as de uma mesma máquina podem ir para consumidores diferentes; perde-se a ordem por máquina. Tratamento de `SIGTERM` para encerrar de forma controlada. Nível de log configurável (`DEBUG_MODE`). |
| `src/consumer/processor.py` | Confirmação manual da posição de leitura (`enable_auto_commit=False`, `commit()` após cada mensagem). Detecção de falha mais rápida (`session_timeout_ms=10000`, `heartbeat_interval_ms=3000`). Tratamento de `SIGTERM`. Reconexão ao Postgres (`check_and_reconnect_pg`). Erro em uma mensagem não derruba o consumidor. |
| `k8s/apps/consumer-deployment.yaml` | 3 réplicas e contêiner de inicialização que espera o tópico existir. |
| `k8s/apps/producer-deployment.yaml` | Contêiner de inicialização que espera o tópico. Novos intervalos base: `maquina-1` 10 s, `maquina-2` 5 s, `maquina-3` 25 s. |
| `k8s/apps/configmap.yaml` | `DEBUG_MODE` nos três ConfigMaps. Janela de alertas de 30 s. Tempo de processamento de 0,4 a 0,8 s (valores alterados depois; ver a entrada seguinte). |

**Makefile e scripts.**

| Arquivo | Alteração |
|---------|-----------|
| `Makefile` | `init` dá `chmod +x` em todos os scripts. `deploy-apps` aceita `DEBUG=1`. `scale-producers MACHINES=N` cria máquinas novas (Deployments) até N (padrão 5), via `scripts/scale_producers.sh`. Novo `scale-machine MAQUINA=M P_REPLICAS=R`. `scale-consumers C_REPLICAS=N` (padrão 10). `clean` também remove as máquinas criadas dinamicamente. `kafka-lag` usa `scripts/inspect_queue.sh`, que consulta qualquer broker em execução. |
| `scripts/test_interactive.sh` | Grava toda a saída em `logs/interactive_*.log`, espera o grupo de consumo voltar ao estado `Stable` após cada rebalanço (espera removida depois) e usa os novos alvos de escala. |
| `scripts/*.sh` | Versionados como executáveis (`100755`). |
| `logs/`, `tests_execution.log` | Logs de execução dos testes de 26/09 por volta de 01:00, **antes** da correção dos tópicos (`PartitionCount: 1`, `ReplicationFactor: 1`). Foram removidos e substituídos pelos logs da execução de 18:06 (ver a entrada seguinte). |

**Pendências e cuidados observados (situação atualizada na entrada [Ajustes para os ensaios finais e resultados no relatório](#ajustes-para-os-ensaios-finais-e-resultados-no-relatório-2026-09-26)).**
- Refazer os testes com a configuração nova: **feito** em 26/09 às 18:06.
- Rebalanços repetidos do grupo nos logs antigos: nos logs novos, o grupo passa por rebalanços quando consumidores entram ou saem, e se estabiliza em poucos segundos.
- O `make deploy-apps` altera o arquivo versionado `k8s/apps/configmap.yaml` com `sed -i` para ligar ou desligar o `DEBUG_MODE`. Depois de um deploy com `DEBUG=1`, o arquivo fica modificado e pode ser commitado por engano; confira `git status` antes de commitar.
- Comentários enganosos em `k8s/apps/producer-deployment.yaml`: **corrigidos**.
- `docs/fase2.md` citando `replicas: 2`: **corrigido**.

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

### Ajustes para os ensaios finais e resultados no relatório (2026-09-26)

**Contexto.** Para a execução final dos testes, a carga e a capacidade do sistema foram ajustadas, os testes foram executados com a configuração nova (12 partições, fator 3) e os resultados foram escritos no relatório.

**Configuração do cluster e dos serviços.**

| Arquivo | Alteração |
|---------|-----------|
| `k8s/kafka/kafka-init-job.yaml` | Tópicos `dados-sensores` e `comandos-fabrica` com **12 partições** (antes 6), fator de replicação 3. |
| `k8s/apps/producer-deployment.yaml`, `k8s/apps/configmap.yaml` | Intervalos base dez vezes menores: `maquina-1` 1 s, `maquina-2` 0,5 s, `maquina-3` 2,5 s e as máquinas criadas por `make scale-producers`, 1 s. O multiplicador de temperatura da `maquina-2` passou de 0,5 para 1. Os comentários dos multiplicadores foram corrigidos para descrever o que eles fazem (mudam a frequência de envio, não os valores). |
| `k8s/apps/configmap.yaml` | `consumer-config`: janela de alertas de 7 s, 8 alertas para o comando `KILL`, processamento simulado de 0,2 a 0,5 s. Novo `consumer-extra-config`, igual, mas com processamento de 0,1 a 0,2 s. |
| `src/consumer/processor.py` | No máximo 10 mensagens por leitura (`max_poll_records=10`). |
| `src/controlador/controlador.py` | Tratamento de `SIGTERM`, como no sensor e no consumidor. |

**Escala.**

| Arquivo | Alteração |
|---------|-----------|
| `scripts/scale_consumers.sh` (novo) e `Makefile` | `make scale-consumers C_REPLICAS=N` (padrão 12) ajusta o total de consumidores: até 3 no Deployment `consumer` e o restante como Deployments `consumer-extra-N`, que usam o `consumer-extra-config` (processamento mais rápido). Extras além do necessário são removidos. |
| `scripts/scale_producers.sh` e `Makefile` | `make scale-producers MACHINES=N` (padrão 8) também remove as máquinas excedentes; as máquinas novas esperam o tópico existir antes de iniciar. |
| `Makefile` | `make clean` também remove os consumidores extras. |

**Testes.**

| Arquivo | Alteração |
|---------|-----------|
| `scripts/test_consumer_rebalance.sh` | Espera padrão após a remoção reduzida de 20 s para **2 s**; os logs dos consumidores coletados são só os dos últimos 3 s. |
| `scripts/test_broker_failover.sh` | Espera padrão após a falha reduzida de 30 s para **3 s**. |
| `scripts/test_interactive.sh` | Mostra o _lag_ na fase inicial; removida a espera pelo estado `Stable` do grupo; ao final, volta para 3 máquinas e 3 consumidores; consultas ao banco limitadas a 20 linhas. |
| `logs/` | Logs antigos (anteriores à correção dos tópicos) e `tests_execution.log` removidos. Novos: `interactive_20260926_180644.log`, `rebalanco_consumidor_20260926_180813.log` e `failover_broker_20260926_180852.log`. |
| `scripts/init_topic.sh` | Script alternativo (não usado pelo Makefile) corrigido para 12 partições e fator 3; usava fator 5, que falharia com 3 brokers, e o comando `kafka-topics.sh`, que não existe na imagem `confluentinc/cp-kafka` (o nome é `kafka-topics`). |

**Relatório (`docs/relatorio/main.typ`).** Testes, resultados, conclusão e apêndice de logs preenchidos com os dados da execução de 26/09. Os números citados foram conferidos contra os logs; dois valores de _lag_ foram corrigidos (a segunda e a quinta amostras após a escala dos consumidores são 84 e 7). Também foram atualizados: tabela de intervalos dos sensores, parâmetros dos consumidores, consumidores extras, padrões dos comandos de escala e a descrição do roteiro interativo.

**Documentação.** `docs/fase2.md` (3 brokers, 3 consumidores, 12 partições, consumidores extras), `docs/fase4.md` (comandos de escala) e `README.md` (pasta do relatório no lugar de `report.md`; `kustomization.yaml` marcado como não utilizado).

**Cuidados e limitações.**
- Com as esperas de 2 s e 3 s, os testes de rebalanço e de failover registram o estado **durante** a recuperação. No rebalanço, a tabela do grupo ainda mostra `is rebalancing`; a conclusão aparece nos logs dos consumidores. Para registrar o estado estável, use `ESPERA_REBALANCE_SEG=20` e `ESPERA_FAILOVER_SEG=30`.
- Na demonstração de elasticidade, os 9 consumidores extras processam mais rápido que os 3 principais. A queda do _lag_ vem dos dois efeitos (mais consumidores e processamento mais rápido), e o ensaio não os separa.
- O script `test_elasticity.sh` não foi executado; não há `logs/elasticidade_*.log`.
- O Deployment `consumer` seleciona pods pelo rótulo `app: consumer`, que os consumidores extras também têm. Na prática não há conflito, porque cada ReplicaSet acrescenta ao seletor o rótulo `pod-template-hash`, mas os seletores dos Deployments se sobrepõem.

**Como validar.**
- Relatório: `typst compile docs/relatorio/main.typ` sem erros; os números dos capítulos de testes e resultados conferem com os três logs citados.
- `bash -n scripts/init_topic.sh` sem erros.
- Configuração: passos 0 a 6 do [Roteiro de validação](#roteiro-de-validação-no-cluster).
