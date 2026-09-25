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
    #set par(justify: false)
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
    Alexandre Malheiros Meslin
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



// -------------------------------------------------------------------
// VALORES QUE DEPENDEM DOS ITENS 1 E 2 (replicação e número de brokers).
// Atualizar aqui quando o cluster Kafka for corrigido; o texto usa estas variáveis.
// -------------------------------------------------------------------
#let num-brokers = "dois"        // TODO(itens 1 e 2): "três" se o StatefulSet passar a ter 3 réplicas
#let fator-replicacao = "1"      // TODO(item 1): fator de replicação do tópico `dados-sensores` após a correção

= INTRODUÇÃO

Uma fábrica inteligente possui diversas máquinas espalhadas em setores como linha de produção, refrigeração e empacotamento. Cada máquina é equipada com sensores que enviam continuamente medições de temperatura, vibração, consumo de energia e emissão de CO2 para um sistema central, que as processa em tempo real para detectar anomalias, falhas ou padrões de uso.

A equipe de engenharia dessa fábrica exige três propriedades do sistema central: ser tolerante a falhas, isto é, não parar quando uma de suas partes cair; escalar com facilidade conforme o número de sensores aumenta; e balancear a carga de forma eficiente entre os processadores de dados. Este trabalho apresenta uma implementação desse sistema com o Apache Kafka @kafka_docs executado em um cluster Kubernetes @kubernetes_docs, e demonstra o balanceamento de carga, a elasticidade e o _failover_ por meio de testes de falha.

O objetivo é construir e validar um cluster Kafka com múltiplos brokers e tópico particionado, sensores simulados como produtores em containers distintos, consumidores organizados em um grupo de consumo com balanceamento automático de partições, e mecanismos para simular a queda de um broker e de um consumidor, registrando o comportamento do sistema por meio de logs.

Este relatório está organizado da seguinte forma. O capítulo de arquitetura descreve os componentes e como eles se comunicam. O capítulo de instalação e uso explica como implantar e operar a aplicação. O capítulo de testes de falha e elasticidade descreve os procedimentos executados, e o capítulo de resultados apresenta o que funcionou e o que não funcionou.

= ARQUITETURA

== Visão geral

O sistema é composto por cinco tipos de componentes, todos executados como pods em um cluster Kubernetes (K3s) e configurados por variáveis de ambiente. Os sensores publicam medições em formato JSON no tópico `dados-sensores` do Kafka. Os consumidores, que pertencem a um mesmo grupo de consumo, leem esse tópico, comparam cada medição com limites predefinidos e gravam as leituras e os alertas em um banco PostgreSQL. Os consumidores também registram os alertas de cada máquina em um Redis compartilhado. Quando uma máquina acumula alertas em excesso, publicam um comando no tópico `comandos-fabrica`, lido pelo controlador, que remove o pod do sensor correspondente por meio da API do Kubernetes; o Deployment do sensor então recria o pod, fechando o laço mostrado na figura. A @fig-arquitetura resume esse fluxo e a @tab-componentes lista os componentes.

#figure(
  kind: image,
  supplement: [Figura],
  caption: [Arquitetura do sistema: fluxo de dados (linha central) e malha de controle (linha inferior).],
)[
  #set text(size: 9pt)
  #set par(first-line-indent: 0pt, justify: false)
  #let caixa(titulo, detalhe, cor) = rect(
    width: 100%, inset: 6pt, radius: 3pt, stroke: 0.6pt + luma(90), fill: cor,
  )[#align(center)[*#titulo* \ #detalhe]]
  #let seta(simbolo) = text(size: 12pt)[#simbolo]
  #grid(
    columns: (1fr, auto, 1fr, auto, 1fr, auto, 1fr),
    column-gutter: 4pt,
    row-gutter: 3pt,
    align: horizon + center,
    // linha do Redis: histórico de alertas compartilhado pelos consumidores
    [], [], [], [], caixa([Redis], [histórico de alertas], rgb("#fff8e1")), [], [],
    [], [], [], [], seta(sym.arrow.t.b), [], [],
    // fluxo principal de dados
    caixa([Sensores], [3 Deployments \ `producer-maquina-N`], rgb("#e6f2ff")),
    seta(sym.arrow.r),
    caixa([Kafka], [tópico \ `dados-sensores`], rgb("#fdebd9")),
    seta(sym.arrow.r),
    caixa([Consumidores], [grupo \ `sensor-group`], rgb("#e8f5e9")),
    seta(sym.arrow.r),
    caixa([PostgreSQL], [`leituras_sensores` \ `event_table`], rgb("#f3e5f5")),
    // ligações entre o fluxo principal e a malha de controle
    seta(sym.arrow.t), [], [], [], seta(sym.arrow.b), [], [],
    // malha de controle: consumidores publicam KILL, o controlador remove o pod do sensor
    caixa([API do Kubernetes], [remove o pod \ do sensor], rgb("#eceff1")),
    seta(sym.arrow.l),
    caixa([Controlador], [grupo \ `controlador-instancias-group`], rgb("#e8f5e9")),
    seta(sym.arrow.l),
    caixa([Kafka], [tópico \ `comandos-fabrica`], rgb("#fdebd9")),
    [], [],
  )
] <fig-arquitetura>

#figure(
  [
  #set par(justify: false, first-line-indent: 0pt)
  #set text(size: 10pt)
  #table(
    columns: (2.5cm, 4.6cm, 1.5cm, 1fr),
    align: (left, left, center, left),
    stroke: 0.5pt + luma(150),
    fill: (_, row) => if row == 0 { rgb("#f5a371") } else { none },
    [*Componente*], [*Recurso Kubernetes*], [*Réplicas*], [*Função*],
    [Brokers Kafka], [StatefulSet `kafka`], [#num-brokers], [Armazenam e distribuem as mensagens dos tópicos],
    [Sensores], [Deployments `producer-maquina-1` a `3`], [1 cada], [Geram medições periódicas e as publicam em `dados-sensores`],
    [Consumidores], [Deployment `consumer`], [2], [Processam as medições, detectam anomalias e gravam no banco],
    [Controlador], [Deployment `controlador`], [2], [Remove pods de máquinas problemáticas a pedido dos consumidores],
    [PostgreSQL], [StatefulSet `postgres`], [1], [Guarda leituras, alertas e eventos de auditoria],
    [Redis], [Deployment `redis`], [1], [Compartilha entre os consumidores o histórico de alertas por máquina],
    [Adminer], [Deployment `adminer`], [1], [Interface web para consultar o banco],
  )
  ],
  caption: [Componentes do sistema e recursos Kubernetes correspondentes.],
) <tab-componentes>

== Cluster Kafka

O cluster Kafka é implantado como um StatefulSet que cria #num-brokers pods, um broker em cada, com nomes estáveis e ordenados (`kafka-0`, `kafka-1`, ...). Essa identidade fixa é necessária porque cada broker precisa ser localizável pelos demais e pelos clientes. Um Service headless (`kafka-headless`) fornece a resolução de nomes por pod, e um Service comum (`kafka`, porta 9092) serve de ponto de entrada para produtores e consumidores.

O cluster opera em modo KRaft, sem Zookeeper: cada pod exerce ao mesmo tempo os papéis de broker e de controller, e os controllers formam um quórum que mantém os metadados do cluster e conduz a eleição de líderes de partição. A configuração de cada broker é gerada na inicialização por um script (`setup-kraft.sh`), armazenado em um ConfigMap. O script deriva o identificador do nó a partir do nome do pod (`kafka-0` recebe o identificador 0), monta o arquivo de configuração com os listeners de cliente (porta 9092) e de controller (porta 9093) e formata o armazenamento com o identificador do cluster. Esse identificador, um UUID compartilhado por todos os brokers, é lido da variável `KAFKA_CLUSTER_ID`, definida em um ConfigMap; ele precisa ser o mesmo em todos os nós e só deve mudar quando o cluster é recriado do zero.

Os tópicos `dados-sensores` e `comandos-fabrica` são criados por um Job do Kubernetes (`kafka-init-topics`) logo após a subida dos brokers, cada um com três partições. As partições permitem que a leitura de um mesmo tópico seja dividida entre vários consumidores do grupo. O fator de replicação atual dos tópicos é #fator-replicacao. // TODO(item 1): descrever a replicação definitiva e o efeito dela na tolerância à queda de um broker.

== Sensores (produtores)

Os sensores são simulados por um programa em Python (`sensor.py`) empacotado em uma imagem Docker. Cada máquina da fábrica corresponde a um Deployment (`producer-maquina-1`, `-2` e `-3`), o que permite dar a cada uma um comportamento próprio por variáveis de ambiente: a `maquina-1` usa os intervalos de geração padrão, a `maquina-2` produz medições com uma frequência quatro vezes maior e com vibração elevada, e a `maquina-3` produz poucas medições, com temperatura e consumo de energia altos, o que tende a gerar anomalias.

A cada intervalo, o sensor gera uma medição de um dos quatro tipos (temperatura, vibração, energia ou CO2) e a envia ao tópico `dados-sensores` como uma mensagem JSON contendo o identificador da máquina, o valor medido e um _timestamp_. O intervalo de cada tipo de sensor é o intervalo base multiplicado por um fator configurável. O identificador da máquina é usado como chave da mensagem, o que faz o Kafka enviar todas as mensagens de uma mesma máquina para a mesma partição e preserva a ordem entre elas.

== Consumidores (processadores de dados)

Os consumidores são um programa Python (`processor.py`) executado em um Deployment com duas réplicas. Todas as réplicas usam o mesmo identificador de grupo (`sensor-group`), de modo que o Kafka divide as partições do tópico entre elas: cada partição é lida por apenas um consumidor do grupo, e a carga é balanceada automaticamente. Se um consumidor entra ou sai do grupo, o Kafka redistribui as partições entre os que restam, num processo chamado de rebalanço.

Para cada mensagem, o consumidor compara o valor medido com o limite do tipo de sensor. Os limites seguem um perfil (`strict`, `normal` ou `loose`), escolhido por variável de ambiente e que pode ser sobrescrito individualmente; no perfil `normal`, por exemplo, os limites são 80 para a temperatura, 3,5 para a vibração, 350 para a energia e 600 para o CO2. Cada leitura é gravada no banco, com uma marca indicando se ela disparou um alerta, e cada alerta gera também um evento de auditoria. Para simular um processamento analítico custoso e assim tornar visível o acúmulo de mensagens (_lag_) sob carga, o consumidor aguarda, a cada mensagem, um tempo sorteado entre valores mínimo e máximo configuráveis (padrão de 0,5 a 1,5 segundo).

Além de registrar alertas isolados, os consumidores avaliam a frequência de alertas por máquina. O histórico de alertas é mantido em um Redis compartilhado, o que permite somar alertas de uma mesma máquina lidos por consumidores diferentes. Quando uma máquina atinge o número máximo de alertas (padrão de cinco) dentro da janela de análise (padrão de 60 segundos), o consumidor publica no tópico `comandos-fabrica` um comando `KILL` com o identificador da máquina.

== Controlador

O controlador é um programa Python (`controlador.py`) executado em um Deployment com duas réplicas, todas no mesmo grupo de consumo (`controlador-instancias-group`) e lendo o tópico `comandos-fabrica`. Como pertencem ao mesmo grupo, cada comando é entregue a apenas uma réplica, o que evita que duas tentem remover o mesmo pod. Ao receber um comando `KILL`, o controlador usa a API do Kubernetes para localizar os pods cujo rótulo `sensor-id` corresponde à máquina e removê-los; o Deployment do sensor então cria um novo pod no lugar, o que simula o reinício de uma máquina com defeito.

Pods não podem remover outros pods por padrão, então o controlador executa com uma ServiceAccount própria, associada por um RoleBinding a uma Role que permite apenas listar, consultar e deletar pods no namespace. Cada ação do controlador (comando recebido, pod removido, máquina não encontrada) é registrada na tabela de eventos.

== Armazenamento e configuração

Os dados processados são guardados em um PostgreSQL executado como StatefulSet, com um volume persistente de 1 GiB. O consumidor cria duas tabelas: `leituras_sensores`, com uma linha por medição (máquina, tipo de sensor, valor, indicação de alerta e instante), e `event_table`, com os eventos de auditoria de todos os componentes (início, envio de dados, alertas, comandos e remoções de pods). O Adminer, uma interface web, permite consultar essas tabelas.

Nenhum valor de configuração está fixo nas imagens. Endereços, nomes de tópicos, limites, intervalos e tempos vêm de variáveis de ambiente, definidas em ConfigMaps do Kubernetes e injetadas nos pods. A senha do banco, por ser um dado sensível, não fica em ConfigMap: é guardada em um Secret (`postgres-credentials`) criado durante a instalação com uma senha aleatória, gerada uma única vez, e lida pelos pods por referência ao Secret. Assim, a senha não aparece em nenhum arquivo do repositório.

= INSTALAÇÃO E USO

== Pré-requisitos

A aplicação foi preparada para uma máquina Debian com acesso à internet e a um usuário com permissão de `sudo`. A instalação usa o Docker para construir as imagens e o K3s, uma distribuição leve do Kubernetes que já inclui o `kubectl`. Todos os comandos abaixo são executados na raiz do repositório.

Os scripts de teste da pasta `scripts/` precisam de permissão de execução. Se o repositório for clonado sem ela, execute uma vez `chmod +x scripts/*.sh`.

== Instalação

A instalação é automatizada pelo Makefile, em três etapas.

+ Instalar as dependências (Docker e K3s) com `make init`. O comando exige `sudo` e, se o Docker for instalado pela primeira vez, é preciso encerrar a sessão e entrar novamente para que o usuário passe a pertencer ao grupo `docker`.
+ Ligar o cluster local com `make start-cluster`.
+ Construir as imagens e implantar tudo com `make all`. Esse comando constrói as imagens do sensor, do consumidor e do controlador, as importa para o K3s, aplica os manifestos do Kafka (ConfigMaps, Services, StatefulSet e Job de criação dos tópicos) e aplica os da aplicação (Secret com a senha do banco, ConfigMaps, PostgreSQL, Redis, Adminer, controlador, sensores e consumidores).

A conferência é feita com `make status`, que lista pods, serviços e StatefulSets. Após um ou dois minutos, todos os pods devem estar em estado `Running`, e o pod do Job de criação dos tópicos em `Completed`.

== Operação

A @tab-comandos reúne os principais comandos de operação.

#figure(
  [
  #set par(justify: false, first-line-indent: 0pt)
  #set text(size: 10pt)
  #table(
    columns: (6.2cm, 1fr),
    align: left,
    stroke: 0.5pt + luma(150),
    fill: (_, row) => if row == 0 { rgb("#f5a371") } else { none },
    [*Comando*], [*O que faz*],
    [`make status`], [Lista pods, serviços e StatefulSets],
    [`make logs-producer`, `make logs-consumer`, `make logs-controlador`], [Acompanha os logs dos sensores, consumidores e controlador],
    [`make kafka-lag`], [Mostra, por partição, o consumidor responsável e o _lag_ do grupo `sensor-group`],
    [`make db-shell`], [Abre o `psql` dentro do pod do PostgreSQL],
    [`make db-ui`], [Expõe o Adminer na porta 8080, já autenticado no banco],
    [`make db-password`], [Exibe a senha do banco, lida do Secret],
    [`make scale-producers`, `make scale-consumers`], [Aumenta o número de sensores da `maquina-1` e de consumidores],
    [`make test-all`], [Executa o roteiro interativo de testes],
    [`make clean`], [Remove os recursos declarados nos manifestos],
    [`make db-reset`], [Apaga o volume do PostgreSQL, zerando o banco],
  )
  ],
  caption: [Principais comandos de operação do Makefile.],
) <tab-comandos>

O comando `make clean` não apaga o volume do PostgreSQL nem o Secret com a senha: por isso, os dados e a senha continuam os mesmos depois de um `make clean` seguido de `make all`. Para zerar o banco, use `make db-reset`; para também trocar a senha, apague o Secret junto com o volume.

O Adminer é aberto com login automático. Por isso, a porta 8080 exposta por `make db-ui` dá acesso ao banco sem senha a quem alcançar a máquina pela rede, o que é aceitável apenas em ambiente de demonstração.

= TESTES DE FALHA E ELASTICIDADE

Os testes são executados por scripts da pasta `scripts/`. Cada script salva a saída em um arquivo na pasta `logs/`, com data e hora no nome, dividido em seções (antes, falha e depois). Cada comando executado aparece no arquivo seguido de sua saída, e um comando que falha ou excede o tempo limite também é registrado, sem interromper o teste. Os tempos de espera, os nomes dos deployments e o número de réplicas são configuráveis por variáveis de ambiente.

== Falha de um broker Kafka

O script `test_broker_failover.sh` derruba à força o primeiro pod do StatefulSet do Kafka, simulando uma perda de energia. Antes e depois da falha, ele registra o líder, as réplicas e as réplicas em sincronia de cada partição do tópico, o _lag_ do grupo de consumo, o total de leituras gravadas no banco e os logs dos consumidores. Os comandos posteriores à falha são executados no broker que sobreviveu. A evidência de que o sistema continuou funcionando é o crescimento do total de leituras no banco, e não os logs dos sensores, porque o cliente Kafka dos sensores envia mensagens de forma assíncrona e registra "enviado" mesmo quando a entrega falha. // TODO: descrever o que foi observado (líderes, ISR, contagem de leituras) e citar o arquivo em `logs/`.

== Falha de um consumidor e rebalanço

O script `test_consumer_rebalance.sh` remove um dos pods consumidores. Antes da falha, ele registra a tabela do grupo de consumo, que mostra qual consumidor lê cada partição, e as últimas linhas de log do pod que será removido. Depois de um intervalo suficiente para o Kafka detectar a saída do consumidor, registra novamente a tabela do grupo e os logs dos consumidores restantes. O rebalanço é demonstrado pela mudança do consumidor responsável pelas partições entre as duas tabelas. // TODO: descrever o que foi observado e citar o arquivo em `logs/`.

== Elasticidade

O script `test_elasticity.sh` demonstra a elasticidade em duas fases. Na primeira, aumenta o número de pods de um sensor (de um para três por padrão) e coleta amostras periódicas do _lag_ do grupo, que tende a crescer porque as mensagens passam a ser produzidas mais depressa do que consumidas. Na segunda, aumenta o número de consumidores (de dois para quatro por padrão) e coleta novas amostras, esperando que o _lag_ diminua conforme as partições são redistribuídas. Como o tópico tem três partições, consumidores além do terceiro ficam ociosos, já que cada partição é lida por um único consumidor do grupo. // TODO: descrever o que foi observado e citar o arquivo em `logs/`.
// TODO(verificar): checar se o LAG se concentra em uma partição por causa da chave `maquina-1`
// (ver "Pontos a verificar" em docs/alteracoes.md) e, se sim, explicar aqui o motivo.

O roteiro interativo (`make test-all`) percorre as mesmas situações passo a passo, com painéis de monitoramento atualizados na tela.

= RESULTADOS

== O que funcionou

// TODO: preencher depois de executar os testes no cluster.

== O que não funcionou

// TODO: preencher depois de executar os testes no cluster. Incluir as limitações conhecidas
// (ver docs/alteracoes.md): replicação do tópico, quórum KRaft de 2 nós, ausência de volume
// persistente no Kafka, Adminer sem senha, scripts versionados sem permissão de execução.

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
