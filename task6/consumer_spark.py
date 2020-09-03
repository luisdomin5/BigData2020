from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


if __name__=='__main__':
	sc = SparkContext(appName='Weather Pipeline')
	ssc = StreamingContext(sc,2)
	
	brokers,topic = 'localhost:9093','realtime'
	kvs = KafkaUtils.createDirectStream(ssc,[topic],{'metadata.broker.list':brokers,'auto.offset.reset':'smallest'})
	
	order = ['lat','lon','observation_time','weather_code']	
	
	records = kvs.map(lambda x: json.loads(x[1]))
	transform = records.map(lambda r: [r[i] for i in order])
	transform.pprint()


	ssc.start()
	ssc.awaitTermination()
