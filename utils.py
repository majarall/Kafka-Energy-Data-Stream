import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import logging

def fetch_data(url):

    logging.info("START")
    try:
        response = requests.get(url=url, params={})
        response.raise_for_status()
        payload = response.json()
        return payload
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error occurred: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")

def process_data(data):
    now = datetime.utcnow()
    records = data.get("records", [])
    filtered_records = [record for record in records
                        if datetime.fromisoformat(record.get('Minutes1UTC')) >= now - timedelta(minutes=6)]
    """
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
        rows.append({"Timestamp": timestamp, "CO2Emission": co2_emission, "ProductionGe100MW": production_ge100mw, "ProductionLt100MW": production_lt100mw, "SolarPower": solar_power, "OffshoreWindPower": offshore_wind_power, "OnshoreWindPower": onshore_wind_power, "ExchangeSum": exchange_sum})
    df = pd.DataFrame(rows)
    """
    return filtered_records