from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4

spark = SparkSession.builder.appName('Weather-api').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
#read json schema from a test file
spotify_schema = spark.read.format('json').load('file:////home/maria_dev/BigData2020/task9/spotify_example.txt').schema

df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers','sandbox-hdp.hortonworks.com:6667')\
    .option('subscribe','spotify_tracks')\
    .option('startingOffsets','earliest')\
    .load()

df_data = df.select(from_json(col("value")\
              .cast("string"), spotify_schema).alias('parsed_value')).selectExpr('parsed_value.*')

#df_final = df_data.withColumn('album name',col('album.name')).withColumn('release date',col('album.release_date')).select('album name','release date')

out = df_data.writeStream.format('console').outputMode('append').start()

out.awaitTermination()
