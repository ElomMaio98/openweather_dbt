# Weather Streaming Pipeline ☁️⚡

Pipeline de dados em tempo real usando OpenWeather API, Kafka, DBT e PostgreSQL com arquitetura Medallion.

## 🏗️ Arquitetura
```
OpenWeather API → Producer → Kafka → Consumer → PostgreSQL (Bronze)
                                                       ↓
                                                   DBT (Railway)
                                                       ↓
                                           Bronze → Silver → Gold
                                                       ↓
                                               Superset/Power BI
```

## 📦 Componentes

### Producer (`/producer`)
- Coleta dados da OpenWeather API a cada X minutos
- Envia mensagens para tópico Kafka `weather-raw-data`
- Stack: Python, kafka-python, requests

### Consumer (`/consumer`)
- Consome mensagens do Kafka
- Salva dados brutos no PostgreSQL (schema: `bronze_layer`)
- Stack: Python, kafka-python, psycopg2

## 🛠️ Stack Tecnológica

- **Streaming:** Apache Kafka
- **Database:** PostgreSQL
- **Transformação:** DBT
- **Orquestração:** Railway
- **BI:** Superset
- **Linguagem:** Python 3.11

## 🚀 Deploy no Railway

Cada serviço (Producer e Consumer) é deployado separadamente:

**Producer:**
- Root Directory: `/producer`
- Start Command: `python main.py`

**Consumer:**
- Root Directory: `/consumer`
- Start Command: `python main.py`

## 📊 Modelagem Dimensional (Star Schema)

### Dimensões:
- `dim_city` - Cidades monitoradas
- `dim_date` - Dimensão temporal (dia)
- `dim_time` - Dimensão temporal (hora)
- `dim_weather_condition` - Condições climáticas

### Fato:
- `fact_weather_measurements` - Métricas de clima

## 🏃 Como Rodar Localmente

### Producer:
```bash
cd producer
pip install -r requirements.txt
python main.py
```

### Consumer:
```bash
cd consumer
pip install -r requirements.txt
python main.py
```

## 📝 Variáveis de Ambiente

### Producer:
- `OPENWEATHER_API_KEY`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `CITIES_LIST`
- `POLLING_INTERVAL_SECONDS`

### Consumer:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `KAFKA_GROUP_ID`
- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

