import requests
import pandas as pd
from src.config import OPENWEATHER_API_KEY,OPENWEATHER_BASE_URL, CITIES_CSV_PATH, POLLING_INTERVAL_SECONDS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
import logging
import time
from kafka import KafkaProducer
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_weather_data(lat,lon):
    max_retries = 3
    for attempt in range (max_retries):
        try: 
            params = {
                'lat': lat,
                'lon': lon,
                'appid': OPENWEATHER_API_KEY,
                'units': 'metric'
            }

            response = requests.get(OPENWEATHER_BASE_URL, params= params, timeout= 10)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.Timeout:
            logger.warning(f'Timeout na tentativa {attempt+1}/{max_retries}')
            if attempt == max_retries-1:
                return None
            else:
                continue
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                if attempt < max_retries -1:
                    logger.info(f'Rate limit! Tentativa {attempt+1}/{max_retries}. Pausando 60s...')
                    time.sleep(60)
                    continue
                else:
                    logger.warning('Rate Limit após 3 tentativas, parando.')
                    return None
            elif response.status_code == 401:
                logger.warning('Chave de API inválida')
                exit(1)
            else:
                logger.warning(f'Erro desconhecido: {e}')
                return None
        except Exception as e:
            logger.warning(f'Erro HTTP: {e}')
            return None
    return None

def start_producer():
    df = pd.read_csv(CITIES_CSV_PATH)
    df_mg = df[df['UF']=='MG']

    configs = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,  
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),  
    'acks': 'all',  
    'retries': 5,
    'retry_backoff_ms': 100
    }
    
    try:
        producer = KafkaProducer(**configs)
        logger.info('Producer iniciado')
    except Exception as e:
        logger.error(f'Erro ao conectar no Kafka: {e}')
        exit(1)
    
    try:
        while True:
            for row in df_mg.itertuples():
                lat = row.Latitude
                lon = row.Longitude
                city = row.City

                weather_data = fetch_weather_data(lat= lat, lon= lon)
                weather_data['city_name'] = city
                time.sleep(1)
                try:
                    if weather_data is not None:

                        producer.send(KAFKA_TOPIC, key= city.encode(), value = weather_data)
                        logger.info(f'Dados de {city} enviados')
                    else:
                        logger.warning(f'Falha em coletar os dados de {city}')
                except Exception as e:
                    logger.error(f'Erro ao enviar {city} para o Kafka {e}')
            time.sleep(POLLING_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info('Encerrando producer...')
        producer.flush(timeout=30)
        producer.close()
        logger.info('Producer fechado')

if __name__ == "__main__":
    start_producer()