import requests
import logging
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from influxdb import InfluxDBClient
from utils import fetch_data, filter_records
from config import kafka_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filename='producer.log',
    filemode='a'
)

class Producer:
    def __init__(self, url, topic, bootstrap_servers):
        self.url = url
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.logger = logging.getLogger(__name__)
    
    # Create a Kafka producer with linger time of 500ms
    def produce_data(self, data):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            linger_ms=500
        )
        # Send the data to Kafka topic
        producer.send(self.topic, value=data)
        # Wait 1 min before fetching data again
        time.sleep(60)
        producer.flush()
        
        # Log the produced data 
        self.logger.info(f"Produced data to Kafka topic {self.topic}")
    
    def main(self):
        while True:
            # Fetch data from endpoint API
            data = fetch_data(self.url)
            #data = filter_records(data)
            self.produce_data(data)
            # Wait 1 min before fetching data again
            # time.sleep(60)

if __name__ == "__main__":
    url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?'
    topic = 'power_system'
    bootstrap_servers = ['localhost:29092']

    # Create a producer object
    producer = Producer(url, topic, bootstrap_servers)
    # Start producing data
    producer.main()