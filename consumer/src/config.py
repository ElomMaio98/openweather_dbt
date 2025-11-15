import os
from dotenv import load_dotenv

load_dotenv()
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
OPENWEATHER_BASE_URL =  os.getenv('OPENWEATHER_BASE_URL')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
KAFKA_TOPIC = 'weather-raw-data'
KAFKA_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP', 'weather-consumer-group')

CITIES_CSV_PATH = ('data/list_braziliancities.csv')
POLLING_INTERVAL_SECONDS = 60
