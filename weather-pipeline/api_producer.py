from weather-api import *
from kafka import KafkaProducer
import json

def send_response(response,key):
	# Exit if the request failed
	if response.status_code != 200:
		print('Bad Response')
		return
	# Send the message to the queue in kafka
	producer.send(key,response.json())



producer = KafkaProducer(bootstrap_servers=['broker1':'localhost:9093'],
		value_serializer=lambda x:json.dumps(x).encode('ascii'))

# TODO: Create function to produce and send data to kafka on timer

