from kafka import KafkaConsumer

consumer = KafkaConsumer('shakespeare',bootstrap_servers='localhost:9093')

for msg in consumer:
	print(msg)
