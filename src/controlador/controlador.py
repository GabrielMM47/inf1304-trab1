"""
Microsserviço Controlador.

Consome mensagens do tópico de comandos e utiliza a API do Kubernetes
para deletar pods (sensores) problemáticos.
"""

import os
import json
import logging

if os.environ.get("DEBUG_MODE") == "1":
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)
import psycopg2
from kafka import KafkaConsumer
from kubernetes import client, config as k8s_config

def obter_configuracao():
    """
    Recupera a configuração do controlador a partir de variáveis de ambiente.

    Retorna:
        dict: Endereço do broker Kafka, tópico de comandos, grupo de consumo das
        instâncias do controlador, namespace do Kubernetes onde os pods são
        procurados e dados de conexão do PostgreSQL.
    """
    return {
        "broker": os.environ.get("KAFKA_BROKER", "localhost:9092"),
        "topico": os.environ.get("KAFKA_TOPIC_COMANDOS", "comandos-fabrica"),
        "group_id": os.environ.get("KAFKA_GROUP_ID_CONTROLADOR", "controlador-instancias-group"),
        "namespace": os.environ.get("NAMESPACE", "default"),
        "pg_host": os.environ.get("PG_HOST", ""),
        "pg_port": os.environ.get("PG_PORT", "5432"),
        "pg_db": os.environ.get("PG_DB", "fabrica"),
        "pg_user": os.environ.get("PG_USER", "postgres"),
        "pg_password": os.environ.get("PG_PASSWORD", "")
    }

def init_postgres(config):
    """
    Conecta ao PostgreSQL e garante a existência da tabela `event_table`.

    Os dados de conexão vêm da configuração (variáveis de ambiente). Se `PG_HOST`
    não estiver definido, o controlador roda sem banco. A conexão usa autocommit,
    então cada evento gravado é persistido imediatamente.

    Argumentos:
        config (dict): Configuração com as chaves `pg_host`, `pg_port`, `pg_db`,
            `pg_user` e `pg_password`.

    Retorna:
        A conexão com o banco, ou None se `PG_HOST` não estiver definido ou se a
        conexão falhar.
    """
    if not config.get("pg_host"):
        return None
    try:
        conn = psycopg2.connect(
            host=config["pg_host"],
            port=config["pg_port"],
            dbname=config["pg_db"],
            user=config["pg_user"],
            password=config["pg_password"]
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS event_table (
                    id SERIAL PRIMARY KEY,
                    component_name VARCHAR(50),
                    event_type VARCHAR(50),
                    details JSONB,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        return conn
    except Exception as e:
        print(f"Aviso: Não foi possível conectar ao PostgreSQL: {e}")
        return None

def log_event(pg_conn, component, event_type, details):
    """
    Registra um evento de auditoria na tabela `event_table` do PostgreSQL.

    Não faz nada se não houver conexão com o banco. Falhas na gravação são
    apenas impressas, para não interromper o processamento principal.

    Argumentos:
        pg_conn: Conexão com o PostgreSQL, ou None se o banco não estiver disponível.
        component (str): Nome do componente que gerou o evento (ex: "CONTROLADOR").
        event_type (str): Tipo do evento (ex: "START", "MACHINE_KILLED").
        details (dict): Dados adicionais do evento, gravados como JSON.
    """
    if not pg_conn:
        return
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO event_table (component_name, event_type, details)
                VALUES (%s, %s, %s)
            """, (component, event_type, json.dumps(details)))
    except Exception as e:
        print(f"Erro ao registrar evento: {e}")

def init_k8s():
    """Inicializa a conexão com a API do Kubernetes."""
    try:
        # Tenta carregar as credenciais de dentro do pod (via ServiceAccount)
        k8s_config.load_incluster_config()
        print("Autenticação In-Cluster configurada com sucesso.")
    except Exception as e:
        print(f"Erro ao carregar in-cluster config (pode estar rodando fora do cluster): {e}")
        try:
            # Fallback para desenvolvimento local
            k8s_config.load_kube_config()
            print("Autenticação local do KubeConfig carregada com sucesso.")
        except Exception as ex:
            print(f"Não foi possível autenticar no Kubernetes: {ex}")

def matar_pod(maquina_id, namespace, pg_conn):
    """
    Busca e deleta os pods associados à maquina_id utilizando a API do Kubernetes.
    A filtragem é feita assumindo que o pod tem um label `sensor-id=maquina_id`.
    """
    v1 = client.CoreV1Api()
    label_selector = f"sensor-id={maquina_id}"
    
    print(f"[*] Procurando pods com label: '{label_selector}' no namespace '{namespace}'...")
    try:
        pods = v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        if not pods.items:
            print(f"[-] Nenhum pod encontrado ativo para a máquina {maquina_id}.")
            log_event(pg_conn, "CONTROLADOR", "MACHINE_NOT_FOUND", {"maquina_id": maquina_id})
            return
            
        for pod in pods.items:
            pod_name = pod.metadata.name
            print(f"[*] Deletando pod problemático: {pod_name}...")
            v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
            print(f"[+] Pod {pod_name} deletado com sucesso!")
            log_event(pg_conn, "CONTROLADOR", "MACHINE_KILLED", {"maquina_id": maquina_id, "pod_name": pod_name})
    except Exception as e:
        print(f"[ERRO] Falha ao interagir com a API do K8s para a máquina {maquina_id}: {e}")
        log_event(pg_conn, "CONTROLADOR", "KILL_ERROR", {"maquina_id": maquina_id, "error": str(e)})

def main():
    """
    Função principal do controlador.

    Conecta ao banco e à API do Kubernetes e consome o tópico de comandos no grupo
    compartilhado pelas instâncias do controlador, de modo que cada comando é
    tratado por apenas uma instância. Para cada comando `KILL` com um
    `maquina_id`, remove os pods da máquina correspondente. Começa a ler do fim do
    tópico (`auto_offset_reset='latest'`), ignorando comandos antigos. Ao ser
    interrompido, fecha o consumidor e a conexão com o banco.
    """
    config = obter_configuracao()
    pg_conn = init_postgres(config)
    init_k8s()
    
    print(f"Iniciando Controlador. Conectando ao broker {config['broker']}...")
    log_event(pg_conn, "CONTROLADOR", "START", {"namespace": config["namespace"]})
    
    consumer = KafkaConsumer(
        config["topico"],
        bootstrap_servers=[config["broker"]],
        group_id=config["group_id"],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    print(f"Escutando comandos no tópico '{config['topico']}' (Namespace alvo: '{config['namespace']}')...")
    
    try:
        for mensagem in consumer:
            dado = mensagem.value
            comando = dado.get("comando")
            maquina_id = dado.get("maquina_id")
            
            if comando == "KILL" and maquina_id:
                print(f"\n>> Comando recebido: MATAR instância da máquina {maquina_id}")
                matar_pod(maquina_id, config["namespace"], pg_conn)
    except KeyboardInterrupt:
        print("Parando controlador...")
    finally:
        consumer.close()
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    main()
