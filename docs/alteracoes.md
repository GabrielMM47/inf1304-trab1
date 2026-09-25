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
| 8 | Constantes hard-coded e docstrings faltantes (itens pontuados no enunciado) | Pendente (escopo parcial) |

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
