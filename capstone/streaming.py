from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json 
from pyspark.sql.functions import *
#spark-submit --master yarn --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0  --repositories http://repo.hortonworks.com/content/groups/public/ --files /usr/hdp/current/spark2-client/conf/hbase-site.xml streaming.py 


spark = SparkSession.builder.appName('Capstone').getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc,5)
spark.sparkContext.setLogLevel("WARN")

catalog = "".join("""{"table":{"namespace":"default","name":"weatherdata"},"rowkey":"key","columns":{"key":{"cf":"rowkey","col":"key","type":"string"},"lat":{"cf":"coord","col":"lat","type":"double"},"lon":{"cf":"coord","col":"lon","type":"double"},"name":{"cf":"info","col":"name","type":"string"},"dt":{"cf":"info","col":"dt","type":"string"},"temp":{"cf":"temperature","col":"temp","type":"double"},"temp_max":{"cf":"temperature","col":"temp_max","type":"double"},"temp_min":{"cf":"temperature","col":"temp_min","type":"double"},"feels_like":{"cf":"temperature","col":"feels_like","type":"double"},"desc":{"cf":"weather","col":"desc","type":"string"},"typ":{"cf":"weather","col":"typ","type":"string"},"wind_speed":{"cf":"weather","col":"wind_speed","type":"double"},"humidity":{"cf":"weather","col":"humidity","type":"bigint"},"pressure":{"cf":"weather","col":"pressure","type":"bigint"}}}""".split())

def handle_rdd(rdd):
	if(not rdd.isEmpty()):
		df = spark.read.json(rdd)
		#df.printSchema()
		df_out=df.selectExpr("dt","coord.lat","coord.lon","lower(name) AS name","main.*","lower(weather.description[0]) AS desc","lower(weather.main[0]) AS typ","wind.speed AS wind_speed").withColumn("key",concat(col("name"),col("dt").cast("string"))).withColumn("dt",to_timestamp(col("dt")).cast("string"))
		df_out.show()
		df_out.write.options(catalog=catalog,newTable=5).format("org.apache.spark.sql.execution.datasources.hbase").save()

ks = KafkaUtils.createDirectStream(ssc,['weathermap'],{'metadata.broker.list':'sandbox-hdp.hortonworks.com:6667'})

record = ks.map(lambda x: x[1])
#record.pprint()
record.foreachRDD(handle_rdd)

ssc.start()

ssc.awaitTermination()
