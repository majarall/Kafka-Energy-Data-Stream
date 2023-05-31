from kafka import KafkaConsumer
import json
import pandas as pd
import time
from utils import process_data
from config import kafka_config

"""
Add all consumers in this file
"""

topic = 'power_system'
bootstrap_servers = 'localhost:29092'
    
def consume_data():
    
    consumer = KafkaConsumer(topic, 
                             bootstrap_servers=bootstrap_servers, 
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        #print(message.value)
        data = message.value
        #df = process_data(data)
        print(data)
        # Use the DataFrame for machine learning training


if __name__ == "__main__":
    consume_data()