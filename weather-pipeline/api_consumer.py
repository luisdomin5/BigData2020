from kafka import KafkaConsumer
import json

# Setup kafka consumer, subscribes to the topic 'uk'. Auto offset only set for testing
consumer = KafkaConsumer('uk',bootstrap_servers=['localhost:9093'],
	value_deserializer=lambda x:json.loads(x.decode('ascii')),
	auto_offset_reset='earliest')

for msg in consumer:
	print(msg)

