"""
Módulo consumidor para o sistema de monitoramento da fábrica inteligente.

Este script consome dados de sensores de um tópico Kafka e detecta
quando os valores ultrapassam limites predefinidos.
Também avalia a frequência de alertas por máquina.
"""

import os
import json
import time
import redis
from kafka import KafkaConsumer

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
        "group_id": os.environ.get("KAFKA_GROUP_ID", "sensor-group"),
        "janela_analise": float(os.environ.get("JANELA_ANALISE_SEG", "60.0")),
        "max_alertas": int(os.environ.get("MAX_ALERTAS_JANELA", "5")),
        "perfil": perfil,
        "limites": limites
    }

def verificar_estado_maquina(maquina_id, config, timestamp):
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
            print(f"Recomendação: O cliente/sensor desta máquina deve ser reiniciado/morto!\n")
            
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
            print(f"Recomendação: O cliente/sensor desta máquina deve ser reiniciado/morto!\n")
            
            historico_alertas[maquina_id] = []

def processar_mensagem(dados_mensagem, config):
    """
    Processa uma única mensagem de sensor e verifica se há alertas.
    
    Argumentos:
        dados_mensagem (dict): O conteúdo (payload) com os dados do sensor.
        config (dict): As configurações que contêm limites e janela de análise.
    """
    limites = config["limites"]
    maquina_id = dados_mensagem.get("maquina_id", "desconhecida")
    timestamp = dados_mensagem.get("timestamp")
    
    houve_alerta = False
    
    for chave, limite in limites.items():
        if chave in dados_mensagem:
            valor = dados_mensagem[chave]
            if valor > limite:
                print(f"ALERTA! Alta {chave} detectada: {valor} (Limite: {limite}) na máquina '{maquina_id}' - Dados: {dados_mensagem}")
                houve_alerta = True
            else:
                print(f"Leitura normal para {chave}: {valor} na máquina '{maquina_id}'")
                
    if houve_alerta:
        verificar_estado_maquina(maquina_id, config, timestamp)

def main():
    """
    Função principal de execução para o consumidor.
    Inicializa o consumidor Kafka e escuta continuamente por novas mensagens.
    """
    config = obter_configuracao()
    
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
        auto_offset_reset='earliest'
    )
    
    print("Conectado! Escutando por mensagens...")
    
    try:
        for mensagem in consumidor:
            processar_mensagem(mensagem.value, config)
    except KeyboardInterrupt:
        print("Parando consumidor...")
    finally:
        consumidor.close()

if __name__ == "__main__":
    main()