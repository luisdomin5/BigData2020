from weathermap_api import *
from kafka import KafkaProducer
import json
import time

def send_response(topic,response):
	if response==None:
		return
	# Exit if the request failed
	if response.status_code != 200:
		print('Bad Response')
		print(response.text)
		return
	# Send the message to the queue in kafka
	producer.send(topic,response.json(),partition=0)
	producer.flush()

producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],value_serializer=lambda x:json.dumps(x).encode('utf-8'))


topic = "weathermap"

i=0
while(True):
	locations = ['bristol','london','manchester']
	for l in locations:
		send_response(topic,current_location(l))
	print('Send Successful: '+str(i))
	i+=1
	time.sleep(60)

	
