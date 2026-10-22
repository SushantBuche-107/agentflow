
from confluent_kafka import Producer
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'byoa'
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def send_message(key, value):
    producer.produce(TOPIC, key=key, value=value)
    producer.flush()
