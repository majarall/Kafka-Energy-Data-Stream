import requests
import logging
import time
import json 
import pandas as pd 
from datetime import datetime, timedelta
from kafka import KafkaProducer


# Kafka configuration 
#bootstrap_servers = ['localhost:29092']
#topic = 'power_system_data'
#url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?'

def fetch_data(url, topic, bootstrap_servers):
    logging.info("START")
    try:
        #url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?'
        response = requests.get(url)
        response.raise_for_status()
        
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        producer.send(topic, value=response.json())
        producer.flush()

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.ConnectionError as err: 
        print(f"Connection error occurred: {err}")
    except requests.exceptions.Timeout as err: 
        print(f"Timeout error occurred: {err}")
    except requests.exceptions.RequestException as err: 
        print(f"Error occurred: {err}")


def produce_data(data, topic, bootstrap_servers):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    producer.send(topic, value=data)
    producer.flush()

def main():
    url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?'
    topic = 'power_system'
    bootstrap_servers = ['localhost:29092']
    while True:
        fetch_data(url, topic, bootstrap_servers)
    #produce_data(data, topic, bootstrap_servers)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
    time.sleep(10)
