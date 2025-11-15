from src.consumer import start_consumer, start_loop_consumer
from src.config import KAFKA_TOPIC, KAFKA_GROUP_ID
if __name__ == "__main__":
    consumer = start_consumer(topic=KAFKA_TOPIC, group_id=KAFKA_GROUP_ID )
    start_loop_consumer(consumer=consumer)