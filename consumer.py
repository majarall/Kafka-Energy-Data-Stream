from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
import pandas as pd
from utils import filter_records, records_to_df, aggregate_data

class Consumer:
    def __init__(self, topic, bootstrap_servers):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
    """
    def filter_records(self, data):
        now = datetime.utcnow()
        records = data.get("records", [])
        filtered_records = [record for record in records
                            if datetime.fromisoformat(record.get('Minutes1UTC')) >= now - timedelta(minutes=6)]
        return filtered_records

    def records_to_df(self, filtered_records):
        rows = []
        for record in filtered_records:
            timestamp = datetime.fromisoformat((record["Minutes1UTC"]))
            co2_emission = record["CO2Emission"]
            production_ge100mw = record["ProductionGe100MW"]
            production_lt100mw = record["ProductionLt100MW"]
            solar_power = record["SolarPower"]
            offshore_wind_power = record["OffshoreWindPower"]
            onshore_wind_power = record["OnshoreWindPower"]
            exchange_sum = record["Exchange_Sum"]
            rows.append({"Timestamp": timestamp, "CO2Emission": co2_emission, "ProductionGe100MW": production_ge100mw, "ProductionLt100MW": production_lt100mw, "SolarPower": solar_power, "OffshoreWindPower": offshore_wind_power, "OnshoreWindPower": onshore_wind_power, "ExchangeSum": exchange_sum})
        
        df = pd.DataFrame(rows)
        return df
    """
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
