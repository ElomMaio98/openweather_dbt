from kafka import KafkaConsumer
import hashlib
import json
import logging
from datetime import datetime
from src.config import KAFKA_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
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
