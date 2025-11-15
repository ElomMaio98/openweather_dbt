import psycopg2
import logging
#from src.config import PGHOST, PGDATABASE, PGPASSWORD,PGPORT,PGUSER
import os 
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def open_database_conn():
    db_configs= {
        'host': os.getenv('PGHOST'),
        'port': int(os.getenv('PGPORT')),
        'database': os.getenv('PGDATABASE'),
        'user': os.getenv('PGUSER'),
        'password': os.getenv('PGPASSWORD')
    }
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(minconn = 1, maxconn = 10, **db_configs)
        return connection_pool
    except Exception as e:
        raise ConnectionError (f'Erro ao se conectar com o banco de dados: {e}')
    
def get_connection(connection_pool):
    connection = connection_pool.getconn()
    return connection

def put_connection(connection_pool,connection):
    return connection_pool.putconn(connection)


def close_database_conn(connection_pool):
    try:
        connection_pool.closeall()
        logger.info('Interrompendo conexão com o banco de dados')
    except Exception as e:
        logger.error(f'Erro ao fechar pool: {e}')

def execute_query(connection_pool, dados, hash_mensagem):
    try:
        conn = get_connection(connection_pool)
        cursor = conn.cursor()
        query = '''INSERT INTO bronze_layer.weather_raw (raw_data, message_hash) 
        VALUES (%s, %s)'''
        cursor.execute(query, (dados, hash_mensagem))
        conn.commit()
        cursor.close()
        put_connection(connection_pool, conn)
    except Exception as e:
        logger.error(f"Erro ao inserir: {e}")
        raise 
