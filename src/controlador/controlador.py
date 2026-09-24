"""
Microsserviço Controlador.

Consome mensagens do tópico de comandos e utiliza a API do Kubernetes
para deletar pods (sensores) problemáticos.
"""

import os
import json
from kafka import KafkaConsumer
from kubernetes import client, config as k8s_config

def obter_configuracao():
    return {
        "broker": os.environ.get("KAFKA_BROKER", "localhost:9092"),
        "topico": os.environ.get("KAFKA_TOPIC_COMANDOS", "comandos-fabrica"),
        "group_id": os.environ.get("KAFKA_GROUP_ID_CONTROLADOR", "controlador-instancias-group"),
        "namespace": os.environ.get("NAMESPACE", "default")
    }

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

def matar_pod(maquina_id, namespace):
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
            return
            
        for pod in pods.items:
            pod_name = pod.metadata.name
            print(f"[*] Deletando pod problemático: {pod_name}...")
            v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
            print(f"[+] Pod {pod_name} deletado com sucesso!")
    except Exception as e:
        print(f"[ERRO] Falha ao interagir com a API do K8s para a máquina {maquina_id}: {e}")

def main():
    config = obter_configuracao()
    init_k8s()
    
    print(f"Iniciando Controlador. Conectando ao broker {config['broker']}...")
    
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
                matar_pod(maquina_id, config["namespace"])
    except KeyboardInterrupt:
        print("Parando controlador...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
