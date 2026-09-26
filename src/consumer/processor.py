"""
Módulo consumidor para o sistema de monitoramento da fábrica inteligente.

Este script consome dados de sensores de um tópico Kafka e detecta
quando os valores ultrapassam limites predefinidos.
Também avalia a frequência de alertas por máquina.
"""

import os
import json
import time
import random
import redis
import psycopg2
from kafka import KafkaConsumer, KafkaProducer

# Conexão com Redis (estado compartilhado) ou dicionário local (fallback)
redis_host = os.environ.get("REDIS_HOST", "")
if redis_host:
    redis_client = redis.Redis(host=redis_host, port=int(os.environ.get("REDIS_PORT", "6379")), decode_responses=True)
else:
    redis_client = None
    historico_alertas = {}

def obter_limites_base(perfil):
    """
    Retorna os limites base dependendo do perfil escolhido.
    Opções de perfil: 'strict', 'normal', 'loose'
    """
    perfis = {
        "strict": {"temperatura": 60.0, "vibracao": 2.0, "energia": 200.0, "co2": 400.0},
        "normal": {"temperatura": 80.0, "vibracao": 3.5, "energia": 350.0, "co2": 600.0},
        "loose": {"temperatura": 95.0, "vibracao": 4.8, "energia": 480.0, "co2": 900.0}
    }
    # Caso não seja válido, assume 'normal'
    return perfis.get(perfil.lower(), perfis["normal"])

def obter_configuracao():
    """
    Recupera a configuração a partir de variáveis de ambiente.
    Permite sobrescrever configurações base por variável de ambiente.
    
    Retorna:
        dict: Um dicionário contendo os valores de configuração e limites.
    """
    perfil = os.environ.get("PERFIL_LIMITES", "normal")
    limites_base = obter_limites_base(perfil)
    
    # Substitui pelos valores específicos se estiverem definidos
    def obter_limite(chave_env, chave_base):
        val = os.environ.get(chave_env)
        return float(val) if val is not None else limites_base[chave_base]
        
    limites = {
        "temperatura": obter_limite("LIMITE_TEMPERATURA", "temperatura"),
        "vibracao": obter_limite("LIMITE_VIBRACAO", "vibracao"),
        "energia": obter_limite("LIMITE_ENERGIA", "energia"),
        "co2": obter_limite("LIMITE_CO2", "co2")
    }

    return {
        "broker": os.environ.get("KAFKA_BROKER", "localhost:9092"),
        "topico": os.environ.get("KAFKA_TOPIC", "dados-sensores"),
        "topico_comandos": os.environ.get("KAFKA_TOPIC_COMANDOS", "comandos-fabrica"),
        "group_id": os.environ.get("KAFKA_GROUP_ID", "sensor-group"),
        "janela_analise": float(os.environ.get("JANELA_ANALISE_SEG", "60.0")),
        "max_alertas": int(os.environ.get("MAX_ALERTAS_JANELA", "5")),
        "tempo_processamento_min": float(os.environ.get("TEMPO_PROCESSAMENTO_MIN_SEG", "0.125")),
        "tempo_processamento_max": float(os.environ.get("TEMPO_PROCESSAMENTO_MAX_SEG", "0.375")),
        "pg_host": os.environ.get("PG_HOST", ""),
        "pg_port": os.environ.get("PG_PORT", "5432"),
        "pg_db": os.environ.get("PG_DB", "fabrica"),
        "pg_user": os.environ.get("PG_USER", "postgres"),
        "pg_password": os.environ.get("PG_PASSWORD", ""),
        "perfil": perfil,
        "limites": limites
    }

def init_postgres(config):
    """Inicializa a conexão com o banco e garante a criação da tabela."""
    if not config["pg_host"]:
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
                CREATE TABLE IF NOT EXISTS leituras_sensores (
                    id SERIAL PRIMARY KEY,
                    maquina_id VARCHAR(50),
                    tipo_sensor VARCHAR(50),
                    valor NUMERIC(10, 2),
                    houve_alerta BOOLEAN,
                    timestamp TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS event_table (
                    id SERIAL PRIMARY KEY,
                    component_name VARCHAR(50),
                    event_type VARCHAR(50),
                    details JSONB,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        print("Conectado ao PostgreSQL com sucesso! Tabelas prontas.")
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

def verificar_estado_maquina(maquina_id, config, timestamp, produtor, pg_conn):
    """
    Verifica se a máquina ultrapassou o limite de alertas em um curto período.
    Indica necessidade de matar o cliente ou escalar os recursos.
    """
    agora = time.time()
    ts = timestamp if timestamp else agora
    
    if redis_client:
        chave_redis = f"alertas:{maquina_id}"
        # Adiciona o timestamp na lista
        redis_client.lpush(chave_redis, ts)
        # Define expiração para a chave para não poluir o redis
        redis_client.expire(chave_redis, int(config["janela_analise"]) * 2)
        
        # Pega todos os alertas no Redis
        alertas = redis_client.lrange(chave_redis, 0, -1)
        # Filtra os que estão dentro da janela (o Redis guarda como string)
        alertas_validos = [float(t) for t in alertas if agora - float(t) <= config["janela_analise"]]
        
        if len(alertas_validos) >= config["max_alertas"]:
            print(f"\n[CRÍTICO GLOBAL] MÁQUINA PROBLEMÁTICA DETECTADA: {maquina_id}")
            print(f"Motivo: {config['max_alertas']} alertas em menos de {config['janela_analise']}s (Compartilhado via Redis).")
            print(f"Enviando comando de KILL para o controlador...\n")
            
            produtor.send(
                config["topico_comandos"],
                value={"comando": "KILL", "maquina_id": maquina_id}
            )
            log_event(pg_conn, "CONSUMIDOR", "CRITICAL_ALERT_KILL_SENT", {"maquina_id": maquina_id, "via": "redis"})
            
            # Limpa para não floodar
            redis_client.delete(chave_redis)
    else:
        # Fallback para o estado local em memória caso Redis não seja configurado
        if maquina_id not in historico_alertas:
            historico_alertas[maquina_id] = []
            
        historico_alertas[maquina_id].append(ts)
        
        historico_alertas[maquina_id] = [
            t for t in historico_alertas[maquina_id] 
            if agora - t <= config["janela_analise"]
        ]
        
        if len(historico_alertas[maquina_id]) >= config["max_alertas"]:
            print(f"\n[CRÍTICO LOCAL] MÁQUINA PROBLEMÁTICA DETECTADA: {maquina_id}")
            print(f"Motivo: {config['max_alertas']} alertas em menos de {config['janela_analise']}s (Estado Local).")
            print(f"Enviando comando de KILL para o controlador...\n")
            
            produtor.send(
                config["topico_comandos"],
                value={"comando": "KILL", "maquina_id": maquina_id}
            )
            log_event(pg_conn, "CONSUMIDOR", "CRITICAL_ALERT_KILL_SENT", {"maquina_id": maquina_id, "via": "local"})
            
            historico_alertas[maquina_id] = []

def processar_mensagem(dados_mensagem, config, produtor, pg_conn):
    """
    Processa uma única mensagem de sensor e verifica se há alertas.
    
    Argumentos:
        dados_mensagem (dict): O conteúdo (payload) com os dados do sensor.
        config (dict): As configurações que contêm limites, janela de análise
            e o intervalo de tempo simulado de processamento.
    """
    # Simula um processamento analítico pesado (ex: IA/Machine Learning para anomalias).
    # A duração sorteada entre TEMPO_PROCESSAMENTO_MIN_SEG e TEMPO_PROCESSAMENTO_MAX_SEG
    # gera enfileiramento (backlog) e permite provar que escalar Consumidores resolve o gargalo.
    time.sleep(random.uniform(config["tempo_processamento_min"], config["tempo_processamento_max"]))

    limites = config["limites"]
    maquina_id = dados_mensagem.get("maquina_id", "desconhecida")
    timestamp = dados_mensagem.get("timestamp")
    
    houve_alerta = False
    
    for chave, limite in limites.items():
        if chave in dados_mensagem:
            valor = dados_mensagem[chave]
            alerta_disparado = valor > limite
            
            # Sempre loga o processamento dos dados
            print(f"Leitura recebida para {chave}: {valor} na máquina '{maquina_id}'")
            log_event(pg_conn, "CONSUMIDOR", "PROCESS_DATA", {"maquina_id": maquina_id, "sensor": chave, "valor": valor})
            
            if alerta_disparado:
                print(f"ALERTA! Alta {chave} detectada: {valor} (Limite: {limite}) na máquina '{maquina_id}' - Dados: {dados_mensagem}")
                log_event(pg_conn, "CONSUMIDOR", "ALERT_TRIGGERED", {"maquina_id": maquina_id, "sensor": chave, "valor": valor, "limite": limite})
                houve_alerta = True
                
            # Salvar no Postgres se disponível
            if pg_conn:
                try:
                    with pg_conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO leituras_sensores (maquina_id, tipo_sensor, valor, houve_alerta, timestamp)
                            VALUES (%s, %s, %s, %s, to_timestamp(%s))
                        """, (
                            maquina_id, 
                            chave, 
                            valor, 
                            alerta_disparado,
                            timestamp if timestamp else time.time()
                        ))
                except Exception as e:
                    print(f"Erro ao salvar no Postgres: {e}")
                
    if houve_alerta:
        verificar_estado_maquina(maquina_id, config, timestamp, produtor, pg_conn)

def main():
    """
    Função principal de execução para o consumidor.
    Inicializa o consumidor Kafka e escuta continuamente por novas mensagens.
    """
    config = obter_configuracao()
    pg_conn = init_postgres(config)
    
    log_event(pg_conn, "CONSUMIDOR", "START", {"group_id": config["group_id"], "perfil": config["perfil"]})
    
    print(f"Iniciando consumidor. Conectando ao broker {config['broker']}...")
    print(f"Tópico: {config['topico']}, Group ID: {config['group_id']}")
    print(f"Perfil de Limites: {config['perfil']}")
    print(f"Janela de análise p/ indisponibilidade: {config['max_alertas']} alertas a cada {config['janela_analise']}s")
    print(f"Limites configurados: {config['limites']}")
    
    consumidor = KafkaConsumer(
        config["topico"],
        bootstrap_servers=[config["broker"]],
        group_id=config["group_id"],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        max_poll_interval_ms=600000  # 10 minutos para não dar timeout em demoras de I/O
    )
    
    produtor_comandos = KafkaProducer(
        bootstrap_servers=[config["broker"]],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Conectado! Escutando por mensagens...")
    
    try:
        for mensagem in consumidor:
            try:
                processar_mensagem(mensagem.value, config, produtor_comandos, pg_conn)
            except Exception as e:
                print(f"Erro ao processar mensagem (pulando para a próxima): {e}")
    except KeyboardInterrupt:
        print("Parando consumidor...")
    finally:
        consumidor.close()
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    main()