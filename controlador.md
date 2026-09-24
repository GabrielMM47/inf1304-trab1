Para implementar um microsserviço controlador resiliente e com múltiplas instâncias, a estratégia baseia-se em combinar **Consumer Groups** do Kafka com **RBAC** (Role-Based Access Control) do Kubernetes. O Kafka garantirá que o comando de deleção seja processado apenas uma vez, enquanto o Kubernetes garantirá o isolamento de segurança.

**1. Configuração do Tópico de Controle (Infraestrutura)**

* **Particionamento Obrigatório:** Crie o novo tópico `comandos-fabrica` garantindo que ele tenha múltiplas partições (ex: 3 ou mais). O Kafka só consegue distribuir a carga de leitura entre as instâncias do seu controlador se o número de partições for igual ou maior que o número de réplicas do microsserviço.
* **Atualização do Init Job:** Adicione a criação deste tópico no seu arquivo `kafka-init-job.yaml` ou no script de inicialização existente.

**2. Código do Microsserviço Controlador (Python)**

* **Consumer Group Único:** Configure todas as instâncias do controlador com o mesmo `group.id` (ex: `controlador-instancias-group`). Quando uma mensagem chegar no tópico `comandos-fabrica`, o Kafka entregará o evento para apenas **uma** das instâncias ativas, evitando tentativas simultâneas de deleção do mesmo pod.
* **Autenticação In-Cluster:** Utilize a biblioteca `kubernetes` (instalada via `pip install kubernetes`). No código, inicie a conexão chamando `config.load_incluster_config()`. Isso permite que o script utilize os certificados de segurança injetados automaticamente pelo Kubernetes dentro do pod.
* **Lógica de Deleção:** Ao ler a mensagem, extraia o `maquina_id`. Utilize a API do Kubernetes para buscar e deletar o pod correspondente. A forma mais segura é deletar baseado em *labels* (ex: deletar pods onde a label `sensor-id` seja igual ao `maquina_id` recebido).

**3. Configuração de Permissões (YAML RBAC)**
Por segurança, pods não podem deletar outros pods por padrão. Você precisará criar três recursos no Kubernetes antes de fazer o deploy do controlador:

* **ServiceAccount:** Uma identidade para o seu controlador (ex: `controlador-sa`).
* **Role:** Um conjunto de permissões especificando que recursos do tipo `pods` podem sofrer ações de `get`, `list` e `delete`.
* **RoleBinding:** O elo que vincula a sua `Role` à sua `ServiceAccount`.

**4. Deployment do Controlador (YAML)**

* **Múltiplas Réplicas:** Crie um `controlador-deployment.yaml` com `replicas: 2` (ou o número desejado de instâncias para garantir failover).
* **Vínculo de Identidade:** Na especificação do pod (dentro do `template.spec`), adicione o campo `serviceAccountName: controlador-sa`. Isso aplicará as permissões configuradas no passo anterior aos containers do controlador.