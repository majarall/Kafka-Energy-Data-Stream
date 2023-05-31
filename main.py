import requests
import logging
import time
import json 
import pandas as pd 
from datetime import datetime, timedelta
from kafka import KafkaProducer

from utils import fetch_data, process_data
from producer import produce_data
from config import kafka_config

# Kafka configuration 
#bootstrap_servers = ['localhost:29092']
#topic = 'power_system_data'
#url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?'


"""
Read from the producers in this file
"""
def main():
    url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?'
    #topic = 'power_system'
    #bootstrap_servers = ['localhost:29092']
    while True:
        data = fetch_data(url)
        data = process_data(data)
        produce_data(data, 
                     topic=kafka_config['topic'], 
                     bootstrap_servers=kafka_config['bootstrap_servers'])

        #time.sleep(1)
        #print(consume_data(topic, bootstrap_servers))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
    time.sleep(10)
