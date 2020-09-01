from weather_api import *
from kafka import KafkaProducer
import json

def send_response(key,response):
	# Exit if the request failed
	if response.status_code != 200:
		print('Bad Response')
		return
	# Send the message to the queue in kafka
	producer.send(key,response.json())
	producer.flush()



producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
		value_serializer=lambda x:json.dumps(x).encode('ascii'))

topic='uk'
send_response(topic,get_current_weather('London'))
send_response(topic,get_current_weather('Bristol,uk'))
send_response('us',get_current_weather('New York'))
producer.flush()

# TODO: Create function to produce and send data to kafka on timer

