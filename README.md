# api_energydataservice.dk

# Power System Data Processing System

This is a data processing system that fetches power system data from an external API, performs data filtering, conversion, and aggregation, and then publishes the processed data to a Kafka broker for further consumption or analysis.

## Features

- Fetches power system data from an external API.
- Filters the data based on a specified time window (last 5 minutes every minute).
- Converts the filtered data into a structured format (DataFrame).
- Performs data aggregation on selected fields.
- Publishes the processed data to a Kafka broker for further consumption.

## System Components

The system consists of the following components:


1.  utils.py: A module that contains the `DataProcessor` class responsible for data filtering, conversion, and aggregation.
2.  producer.py: A module that contains the `KafkaProducer` class for publishing the processed data to a Kafka broker.
3.  config.py: A configuration file that stores Kafka broker settings and other system configurations.
4.  utils.py: Utility functions used within the system, such as fetching data from the API and logging.
5.  requirements.txt (need to add??): A file that lists the required Python dependencies for the system.

## How to run the system

To run the power system data processing system, follow these steps:

1. Install the required dependencies by running `pip install -r requirements.txt`.
2. Configure the Kafka broker settings and other system configurations in the `config.py` file.
   Start the kafka broker and the Zookeeper: docker-compose up -d
3. Run the producer.py: nohup python producer.py > producer.log &

The system will start fetching data from the external API, process it, and publish the processed data to the Kafka broker. The processed data can be consumed or further analyzed by other components or applications.
4. Run the consumer.py: python consumer.py

## Customization for furthur downstream tasks

The system can be customized to fit your specific requirements. You can modify the data processing logic in the `DataProcessor` class, adjust the Kafka broker settings, or extend the system functionality as needed.





