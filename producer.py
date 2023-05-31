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

    def produce_data(self, data):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        producer.send(self.topic, value=data)
        producer.flush()
        self.logger.info(f"Produced data to Kafka topic {self.topic}")
    
    def main(self):
        while True:
            data = fetch_data(self.url)
            #data = filter_records(data)
            self.produce_data(data)
            time.sleep(60)

if __name__ == "__main__":
    url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?'
    topic = 'power_system'
    bootstrap_servers = ['localhost:29092']


    producer = Producer(url, topic, bootstrap_servers)
    producer.main()