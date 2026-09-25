"""
Módulo produtor para o sistema de monitoramento da fábrica inteligente.

Este script simula um sensor gerando dados periódicos (temperatura, vibração,
consumo de energia e emissão de CO2) e os envia para um tópico Kafka em formato JSON.
"""

import os
import json
import time
import random
import uuid
import psycopg2
from kafka import KafkaProducer

def obter_configuracao():
    """
    Recupera a configuração a partir de variáveis de ambiente.
    
    Retorna:
        dict: Um dicionário contendo os valores de configuração.
    """
    return {
        "broker": os.environ.get("KAFKA_BROKER", "localhost:9092"),
        "topico": os.environ.get("KAFKA_TOPIC", "dados-sensores"),
        "intervalo_base": float(os.environ.get("GENERATION_INTERVAL", "2.0")),
        "maquina_id": os.environ.get("MAQUINA_ID", str(uuid.uuid4())[:8]),
        "mult_temperatura": float(os.environ.get("MULTIPLICADOR_TEMPERATURA", "1.0")),
        "mult_vibracao": float(os.environ.get("MULTIPLICADOR_VIBRACAO", "1.5")),
        "mult_energia": float(os.environ.get("MULTIPLICADOR_ENERGIA", "2.0")),
        "mult_co2": float(os.environ.get("MULTIPLICADOR_CO2", "5.0")),
        "pg_host": os.environ.get("PG_HOST", ""),
        "pg_port": os.environ.get("PG_PORT", "5432"),
        "pg_db": os.environ.get("PG_DB", "fabrica"),
        "pg_user": os.environ.get("PG_USER", "postgres"),
        "pg_password": os.environ.get("PG_PASSWORD", "")
    }

def init_postgres(config):
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

def gerar_dados(tipo):
    """
    Gera dados simulados do sensor com base no tipo especificado.
    
    Argumentos:
        tipo (str): O tipo de dado a ser gerado.
    
    Retorna:
        float: O valor simulado.
    """
    if tipo == "temperatura":
        return round(random.uniform(20.0, 100.0), 2)
    elif tipo == "vibracao":
        return round(random.uniform(0.1, 5.0), 2)
    elif tipo == "energia":
        return round(random.uniform(100.0, 500.0), 2)
    elif tipo == "co2":
        return round(random.uniform(300.0, 1000.0), 2)
    return 0.0

def main():
    """
    Função principal de execução para o produtor.
    Inicializa o produtor Kafka e envia dados simulados continuamente.
    """
    config = obter_configuracao()
    pg_conn = init_postgres(config)
    
    print(f"Iniciando produtor. Conectando ao broker {config['broker']}...")
    print(f"Máquina ID: {config['maquina_id']}")
    
    log_event(pg_conn, "PRODUTOR", "START", {"maquina_id": config["maquina_id"], "topico": config["topico"]})
    
    produtor = KafkaProducer(
        bootstrap_servers=[config["broker"]],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Conectado! Enviando dados para o tópico '{config['topico']}'.")
    
    intervalo = config["intervalo_base"]
    
    sensores = {
        "temperatura": {"multiplicador": config["mult_temperatura"], "ultimo_envio": 0},
        "vibracao": {"multiplicador": config["mult_vibracao"], "ultimo_envio": 0},
        "energia": {"multiplicador": config["mult_energia"], "ultimo_envio": 0},
        "co2": {"multiplicador": config["mult_co2"], "ultimo_envio": 0}
    }
    
    try:
        while True:
            agora = time.time()
            for tipo, props in sensores.items():
                intervalo_sensor = intervalo * props["multiplicador"]
                if agora - props["ultimo_envio"] >= intervalo_sensor:
                    dado = {
                        "maquina_id": config["maquina_id"],
                        tipo: gerar_dados(tipo),
                        "timestamp": agora
                    }
                    produtor.send(
                        config["topico"], 
                        key=config["maquina_id"].encode('utf-8'),
                        value=dado
                    )
                    log_event(pg_conn, "PRODUTOR", "SEND_DATA", dado)
                    print(f"Enviado: {dado}")
                    props["ultimo_envio"] = agora
            
            # Pequena pausa para não consumir CPU em 100%
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("Parando produtor...")
    finally:
        produtor.close()
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    main()