from kafka import KafkaConsumer
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import time

bootstrap_servers = 'localhost:29092'
topic = 'power_system' 
    
def consume_data():
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:29092',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        #print(message.value)
        data = message.value
        df = process_data(data)
        print(df.head())
        # Use the DataFrame for machine learning training

def process_data(data):
    filtered_records = []
    now = datetime.utcnow()
    records = data.get("records", [])
    for record in records:
        timestamp = datetime.fromisoformat(record['Minutes1UTC'])
        if now - timestamp <= timedelta(minutes=5):
            filtered_records.append(record)

    
    # Create a list of dictionaries to store the data
    rows = []
    
    # Process the filtered records and store in the list
    for record in filtered_records:
        # Access the required fields from the record
        timestamp = datetime.fromisoformat(record["Minutes1UTC"])
        co2_emission = record["CO2Emission"]
        production_ge100mw = record["ProductionGe100MW"]
        production_lt100mw = record["ProductionLt100MW"]
        solar_power = record["SolarPower"]
        offshore_wind_power = record["OffshoreWindPower"]
        onshore_wind_power = record["OnshoreWindPower"]
        exchange_sum = record["Exchange_Sum"]
        
        # Append the record as a dictionary to the list
        rows.append({"Timestamp": timestamp, "CO2Emission": co2_emission,
                        "ProductionGe100MW": production_ge100mw, "ProductionLt100MW": production_lt100mw,
                        "SolarPower": solar_power, "OffshoreWindPower": offshore_wind_power,
                        "OnshoreWindPower": onshore_wind_power, "ExchangeSum": exchange_sum})
    
    # Create the DataFrame from the list of dictionaries
    df = pd.DataFrame(rows)
    
    return df

if __name__ == "__main__":
    consume_data()