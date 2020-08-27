from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9093')

with open('/home/hadoop/BigData2020/Shakespeare.txt') as f:
	for line in f:
		producer.send('shakespeare',b'{}'.format(line))
		
producer.flush()
producer.close()
