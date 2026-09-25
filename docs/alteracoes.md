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
| 6 | Logs salvos pelos scripts de teste e relatório final (`docs/report.md`) | Pendente |
| 7 | Documentação citando Zookeeper, removido do projeto (Kafka roda em KRaft) | **Concluído** |
| 8 | Constantes hard-coded e docstrings faltantes (itens pontuados no enunciado) | Em andamento (escopo parcial): tempo de processamento, UUID do cluster e senha do Postgres feitos; faltam perfis de limites/faixas do sensor e docstrings |

Ordem combinada de execução: 7, depois 8 (parcial), depois a parte de salvamento de logs do 6, depois 1, 2, 4 e 5, e por fim o relatório do 6.

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

**Como validar.** `grep -rni zookeeper .` deve retornar apenas a linha do Makefile (`"Subindo cluster Kafka em modo KRaft (Sem Zookeeper!)..."`), que já estava correta.

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

**Como validar.** `python3 -m py_compile src/consumer/processor.py` e, no cluster, `kubectl exec deploy/consumer -- env | grep TEMPO_PROCESSAMENTO`.

**Pendente no item 8.** Perfis de limites e faixas de valores do sensor (ainda no código), UUID do cluster Kafka, credenciais do Postgres (ConfigMap vs Secret) e docstrings faltantes. Cada um será registrado aqui quando for tratado.

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

Sem cluster, foi verificado: sintaxe dos YAMLs (PyYAML), `bash -n` do script KRaft, `py_compile` dos três serviços e o comportamento do alvo `secrets` com um `kubectl` simulado (1ª execução cria com 24 caracteres; 2ª mantém; `db-password` retorna a mesma senha). **Ainda falta testar no k3s**:
1. `make all` e depois `make status`: todos os pods `Running` (sem `CreateContainerConfigError`, que indicaria Secret ausente).
2. `kubectl logs kafka-0 | head`: deve mostrar `Formatando logs KRaft (cluster ID: zrH_urXmRdCh86VyykE9Vg)`.
3. `kubectl logs -l app=consumer`: deve mostrar "Conectado ao PostgreSQL com sucesso".
4. `make db-ui` e abrir `http://localhost:8080`: deve entrar sem senha.
5. `make secrets` uma segunda vez: deve imprimir "já existe (mantido)".
