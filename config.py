import json

kafka_config = {
    'topic':'power_system',
    'bootstrap_servers': ['localhost:29092'],
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'value_serializer': lambda x: json.dumps(x).encode('utf-8')
}