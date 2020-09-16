from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0

spark = SparkSession.builder.appName('Weather-api').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
#read json schema from a test file
spotify_schema = spark.read.format('json').load('file:////home/maria_dev/BigData2020/task9/spotify_example.txt').schema

df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers','sandbox-hdp.hortonworks.com:6667')\
    .option('subscribe','spotify_tracks')\
    .option('startingOffsets','earliest')\
    .load()

""" Select data of use:
track_name,artist, album, release_date,popularity,track_id
"""
df_data = df.select(from_json(col("value")\
              .cast("string"), spotify_schema).alias('parsed_value')).selectExpr('parsed_value.*')

df_final = df_data \
	.withColumn('album_name',col('album.name')) \
	.withColumn('release_date',col('album.release_date')) \
	.selectExpr('name as track_name','artists[0].name as artist','album_name','release_date','popularity','id as track_id')

out = df_final.writeStream.format('console').outputMode('append').start()

out.awaitTermination()
