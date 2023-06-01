from kafka import KafkaConsumer
import json
from tabulate import tabulate
from datetime import datetime, timedelta
import pandas as pd
from utils import DataProcessor

class Consumer:

    def __init__(self, topic, bootstrap_servers):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.data_processor = DataProcessor()
        self.consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers=self.bootstrap_servers,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    def consume_and_print_df(self):
        for message in self.consumer:
            #data = self.filter_records(message.value)
            #df = self.records_to_df(data)
            
            # Filter the records 
            data = self.data_processor.filter_records(message.value)
            # Convert records to Pandas DataFrame
            df = self.data_processor.records_to_df(data)
            # Aggregate the data in the DataFrame
            agg_df = self.data_processor.aggregate_data(df.copy())
            #print(message.value)
            print(df.to_markdown())
            print(agg_df.head())


if __name__ == "__main__":
    topic = 'power_system'
    bootstrap_servers = ['localhost:29092']

    # Create a Consumer object
    consumer = Consumer(topic, bootstrap_servers)
    # Consume messages from the Kafka topic and print as a DataFrame
    consumer.consume_and_print_df()
