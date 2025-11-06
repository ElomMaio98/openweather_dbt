import psycopg2
from psycopg2 import pool
import logging
import time
from src.config import PGHOST, PGDATABASE, PGPASSWORD,PGPORT,PGUSER

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def open_database_conn():
    db_configs= {
        'host': PGHOST,
        'port': int(PGPORT),
        'database': PGDATABASE,
        'user': PGUSER,
        'password': PGPASSWORD
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

