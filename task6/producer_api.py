from weather_api import *
from kafka import KafkaProducer
import json

def send_response(key,response):
	if response==None:
		return
	# Exit if the request failed
	if response.status_code != 200:
		print('Bad Response')
		print(response.text)
		return
	# Send the message to the queue in kafka
	producer.send(key,response.json())
	producer.flush()



producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
		value_serializer=lambda x:json.dumps(x).encode('utf-8'))

topic='realtime'
send_response(topic,get_weather_data('realtime','london'))
send_response(topic,get_weather_data('realtime','bristol'))
send_response(topic,get_weather_data('realtime','new york'))
producer.flush()

# TODO: Create function to produce and send data to kafka on timer

