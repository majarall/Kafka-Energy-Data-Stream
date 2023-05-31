from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
import pandas as pd
from utils import filter_records, records_to_df, aggregate_data

class Consumer:
    def __init__(self, topic, bootstrap_servers):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    def consume_and_print_df(self):
        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers=self.bootstrap_servers,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        for message in consumer:
            #data = self.filter_records(message.value)
            #df = self.records_to_df(data)
            data = filter_records(message.value)
            df = records_to_df(data)
            agg_df = aggregate_data(df)
            print(message.value)
            print(df.head())
            print(agg_df.head())


if __name__ == "__main__":
    topic = 'power_system'
    bootstrap_servers = ['localhost:29092']

    consumer = Consumer(topic, bootstrap_servers)
    consumer.consume_and_print_df()
