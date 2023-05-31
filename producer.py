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
    """"
    def records_to_df(self, filtered_records):
        rows = []
        for record in filtered_records:
            timestamp = datetime.fromisoformat(record["Minutes1UTC"])
            co2_emission = record["CO2Emission"]
            production_ge100mw = record["ProductionGe100MW"]
            production_lt100mw = record["ProductionLt100MW"]
            solar_power = record["SolarPower"]
            offshore_wind_power = record["OffshoreWindPower"]
            onshore_wind_power = record["OnshoreWindPower"]
            exchange_sum = record["Exchange_Sum"]
            
            # Perform aggregation
            aggregated_values = {
                "Timestamp": timestamp,
                "CO2Emission": sum(co2_emission),
                "ProductionGe100MW": sum(production_ge100mw),
                "ProductionLt100MW": sum(production_lt100mw),
                "SolarPower": sum(solar_power),
                "OffshoreWindPower": sum(offshore_wind_power),
                "OnshoreWindPower": sum(onshore_wind_power),
                "ExchangeSum": sum(exchange_sum)
            }
            
            rows.append(aggregated_values)
        
        df = pd.DataFrame(rows)
        return df
    """
    
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