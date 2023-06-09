

# Event Based Data Processing System

This is a data processing system that fetches power system data from an external API, performs data filtering, conversion, and aggregation, and then publishes the processed data to a Kafka broker for further consumption or analysis.

## Features

- Fetches power system data from an external API.
- Filters the data based on a specified time window (last 5 minutes every minute).
- Converts the filtered data into a structured format (DataFrame).
- Performs data aggregation on selected fields.
- Publishes the processed data to a Kafka broker for further consumption.
- Provides a consumer module to consume and process the data from the Kafka broker for furthur downstream tasks.

## System Components

The system consists of the following components:


1.  utils.py: A module that contains the `DataProcessor` class responsible for data filtering, conversion, and aggregation.
2.  producer.py: A module that contains the `KafkaProducer` class for publishing the processed data to a Kafka broker.
3.  consumer.py: A module that contains the `Consumer` class for consuming and processing data from the Kafka broker.
4.  config.py: A configuration file that stores Kafka broker settings and other system configurations.
5.  utils.py: Utility functions used within the system, such as fetching data from the API and logging.
6.  docker-compose.yml: To run the Kafka broker container and Zookeeper container
7.  requirements.txt: All necessary libraries  

## How to run the system

To run the power system data processing system, follow these steps:

1. Install the required dependencies by running `pip install -r requirements.txt`.
2. Configure the Kafka broker settings and other system configurations in the `config.py` file.
   Start the kafka broker and the Zookeeper: `docker-compose up -d`
3. Run the producer.py: `python producer.py > producer.log &`
4. Run the consumer.py: `python consumer.py`

The system will start fetching data from the external API, process it, and publish the processed data to the Kafka broker. The processed data can be consumed or further analyzed by other components or applications.








