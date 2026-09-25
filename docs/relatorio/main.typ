#import "funcoes.typ": *
#set page(paper: "a4", margin: 2.5cm)
#set text(font: ("Arial", "Roboto"), size: 12pt) 
#set par(first-line-indent: (amount: 1.25cm, all: true), justify: true)
#set text(
  lang: "pt"
)

// ---------------------------------------------------------
// 1. CAPA (Página 1 - Sem cabeçalho e rodapé)
// ---------------------------------------------------------
#align(center)[
  #image("images/logo-puc.png", width: 55%) 
]

#v(3cm)

#align(center)[
  #text(weight: "bold", size: 14pt)[INF1304 – Distribuição e Concorrência]

  #v(0.6cm)

  #text(weight: "bold", size: 14pt)[Trabalho 1: Balanceamento de Carga, Elasticidade e Failover com Kafka em Kubernetes]
]

#v(3cm)

#grid(
  columns: (1fr, 1fr),
  gutter: 2cm,
  [
    #text(weight: "bold")[Alunos:]
    #v(0.4cm)
    #grid(
      columns: (auto, auto),
      column-gutter: 1.5em,
      row-gutter: 1em,
      [2310822], [Eduardo Eugênio de Souza],
      [2210508], [Gabriel Augusto Gedey],
      [2311271], [Gabriel Martins Mendes]
    )
  ],
  [
    #text(weight: "bold")[Professor:]
    #v(0.4cm)
    A definir // TODO: confirmar o nome do professor
  ]
)

#v(1fr) 

#align(center)[
  Rio de Janeiro,

  #v(0.5em)
  Setembro de 2026
]


// ---------------------------------------------------------
// QUEBRA DE PÁGINA E CONFIGURAÇÕES GERAIS
// ---------------------------------------------------------
#pagebreak()

#set page(
  header: align(bottom)[
    #line(length: 100%, stroke: 1.5pt + rgb("555555"))
  ],
  footer: [
    INF1304 – Trabalho 1 #h(1fr) #context counter(page).display()
  ]
)

#set figure(supplement: "Figura")
#show figure.where(kind: table): set figure(supplement: "Tabela")


// ---------------------------------------------------------
// REGRA DE FORMATAÇÃO E RECUOS EXATOS DOS TÍTULOS
// ---------------------------------------------------------
// Aplica a fonte Arial 12pt Negrito em todos os títulos
#show heading: set text(font: ("Arial", "Roboto"), size: 12pt, weight: "bold")

// Constrói a lógica de espaçamento idêntica à régua do Google Docs
#show heading: it => {
  // Se for título sem número (Sumário, Capa, Listas), aplica sem recuo
  if it.numbering == none {
    return block(above: 1.5em, below: 1em)[#it]
  }

  // Medidas solicitadas
  let num-offset = 0cm
  let text-gap = 0.63cm

  if it.level == 2 {
    num-offset = 0.63cm
    text-gap = 1.27cm
  } else if it.level >= 3 {
    num-offset = 1.27cm
    text-gap = 1.27cm
  }

  // O uso do "grid" com "1fr" garante o recuo pendente (Hanging Indent) perfeito.
  // Mesmo que o título seja gigantesco e quebre de linha, ele jamais invadirá
  // o espaço reservado para a numeração.
  block(above: 1.5em, below: 1.5em)[
    #pad(left: num-offset)[
      #grid(
        columns: (auto, 1fr),
        column-gutter: 0.35cm, 
        counter(heading).display(it.numbering),
        it.body
      )
    ]
  ]
}


// ---------------------------------------------------------
// PÁGINAS DE LISTAGEM (Sumário e Figuras)
// ---------------------------------------------------------

// Garante que o texto dentro das entradas do sumário fique com 11pt 
#show outline.entry: set text(size: 11pt)

// Mantém centralizado apenas os títulos Principais (Nível 1) destas páginas
#show heading.where(level: 1): set align(center)

#heading(level: 1, outlined: false)[SUMÁRIO]
#v(1em)
#outline(title: none, depth: 3)

#pagebreak()

#heading(level: 1, outlined: false)[LISTA DE FIGURAS]
#v(1em)
#outline(title: none, target: figure.where(kind: image))

#pagebreak()

#heading(level: 1, outlined: false)[LISTA DE TABELAS]
#v(1em)
#outline(title: none, target: figure.where(kind: table))

#pagebreak()


// ---------------------------------------------------------
// INÍCIO DO CONTEÚDO DO PROJETO
// ---------------------------------------------------------

// Ativa a numeração no formato (1., 1.1., 1.1.1.) a partir daqui
#set heading(numbering: "1.1.")

// Os títulos principais passam a ser alinhados à esquerda no corpo do texto
#show heading.where(level: 1): set align(left)



= INTRODUÇÃO

// TODO: contexto (fábrica inteligente, sensores enviando dados continuamente), objetivo do trabalho
// (tolerância a falhas, escalabilidade e balanceamento de carga com Kafka) e organização do relatório.

= ARQUITETURA

== Visão geral

// TODO: diagrama (figura) e descrição do fluxo: sensores (produtores) -> tópico `dados-sensores`
// -> consumidores (mesmo grupo de consumo) -> PostgreSQL. Incluir o controlador e o Redis.

== Cluster Kafka

// TODO: brokers em StatefulSet, modo KRaft (sem Zookeeper), partições e replicação do tópico,
// script de inicialização e identificador do cluster (KAFKA_CLUSTER_ID).

== Sensores (produtores)

// TODO: geração periódica de temperatura, vibração, energia e CO2 em JSON; um Deployment por máquina.

== Consumidores (processadores de dados)

// TODO: grupo de consumo `sensor-group`, detecção de anomalias por limites, tempo de processamento
// simulado, persistência no banco.

== Controlador

// TODO: leitura do tópico de comandos e remoção de pods problemáticos via API do Kubernetes (RBAC).

== Armazenamento e configuração

// TODO: PostgreSQL (StatefulSet e volume persistente), Redis, ConfigMaps e Secret com a senha do banco.

= INSTALAÇÃO E USO

== Pré-requisitos

// TODO: Debian, Docker e K3s; permissão de execução dos scripts.

== Instalação

// TODO: passos do Makefile (`make init`, `make start-cluster`, `make all`) e conferência com `make status`.

== Operação

// TODO: logs, Adminer (`make db-ui`), consulta à senha (`make db-password`), fila e LAG (`make kafka-lag`).

= TESTES DE FALHA E ELASTICIDADE

== Falha de um broker Kafka

// TODO: procedimento (scripts/test_broker_failover.sh), o que foi observado e evidências em logs.

== Falha de um consumidor e rebalanço

// TODO: procedimento (scripts/test_consumer_rebalance.sh) e partições antes e depois.

== Elasticidade

// TODO: procedimento (scripts/test_elasticity.sh), comportamento do LAG ao escalar produtores e consumidores.

= RESULTADOS

== O que funcionou

// TODO: preencher depois de executar os testes no cluster.

== O que não funcionou

// TODO: preencher depois de executar os testes no cluster; incluir as limitações conhecidas.

= CONCLUSÃO

// TODO: síntese dos resultados e possíveis melhorias.

#pagebreak()
#bibliography("referencias.yaml", style: "associacao-brasileira-de-normas-tecnicas", title: "REFERÊNCIAS")

// APÊNDICES (material de autoria do grupo)

#show: apendices

= Logs de execução <apendice-logs>

// TODO: trechos dos logs gerados em logs/ que mostram o rebalanço e o failover.

// ANEXOS (material de terceiros)

// #show: anexos
