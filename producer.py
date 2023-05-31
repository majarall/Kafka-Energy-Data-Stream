import json
from kafka import KafkaProducer
from config import kafka_config

def produce_data(data,topic, bootstrap_servers):
    producer = KafkaProducer(bootstrap_servers=kafka_config['bootstrap_servers'], 
                             value_serializer=kafka_config['value_serializer'])
    producer.send(topic=kafka_config['topic'], value=data)
    producer.flush()