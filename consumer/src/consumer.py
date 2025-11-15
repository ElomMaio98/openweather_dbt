from kafka import KafkaConsumer
import hashlib
import json
import logging

from src.config import KAFKA_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.database import open_database_conn, close_database_conn, execute_query
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def start_consumer(topic, group_id):
    try:
        configs = {
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,  # ← não 'kafka_topic'
        'group_id': KAFKA_GROUP_ID,
        'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
        'auto_offset_reset': 'latest'
        }
        consumer = KafkaConsumer(**configs, topic = KAFKA_TOPIC)
    except Exception as e:
        logger.error(f'Erro ao se inscrever em tópico Kafka: {e}')
        exit(1)
    return consumer

def check_data(dados):
    try:
        dados['name']
        dados['main']['temp']
        dados['main']['humidity']
        dados['dt']
        return True
    except KeyError:
        logger.error("Mensagem com campos faltando")
        return False

def hash_it(dados):
    string = f"{dados['main']['temp']}{dados['main']['humidity']}{dados['dt']}{dados['city_name']}"
    hash = hashlib.sha256(string.encode()).hexdigest()
    return hash

def start_loop_consumer(consumer):
    pool = open_database_conn()
    while True:
        try:
            message = consumer.poll(timeout_ms=1000)
            if not message:
                logger.info("Sem conteúdo para ler")
                continue
            for msg_lst in message.values():
                for msg in msg_lst:            
                    dados = msg.value
                    if not check_data(dados=dados):
                        continue
                    hashed = hash_it(dados=dados)
                    execute_query(dados=json.dumps(dados), hash_mensagem=hashed, connection_pool=pool)
        
        except psycopg2.OperationalError as e:
            logger.error(f"Erro de conexão com PostgreSQL: {e}")
            close_database_conn(pool)
            exit(1)
        
        except json.JSONDecodeError as e:
            logger.warning(f"Mensagem com JSON inválido (corrompida): {e}")
            continue
        
        except KeyError as e:
            logger.warning(f"Mensagem com campos faltando: {e}")
            continue
        
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            continue
