import requests
from src.config import OPENWEATHER_API_KEY,OPENWEATHER_BASE_URL
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_weather_data(lat,lon):
    max_retries = 3
    for attempt in range (0,max_retries):
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
        
        except requests.Timeout:
            if attempt == max_retries:
                return None
            else:
                continue
        
        except requests.HTTPError as e:
            if response.status_code == 429:
                if attempt < max_retries:
                    logger.info('Limite de requests atingido, pausando producer 60s')
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

