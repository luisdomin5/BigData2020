from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4

spark = SparkSession.builder.appName('Weather-api').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
#read json schema from a test file
json_schema = spark.read.format('json').load('file:////home/nhobbs/BigData2020/task6/data.txt').schema

df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers','localhost:9093')\
    .option('subscribe','realtime')\
    .load()

df_data = df.select(from_json(col("value")\
              .cast("string"), json_schema)\
                    .alias('weather')).selectExpr('weather.*')

out = df_data.writeStream.format('console').outputMode('append').start()

out.awaitTermination()

