from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import os
import sys
from pyspark.sql.functions import *
import re

def rdd_transform(rdd):
    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd,schema=['hashtag','count'])
        SQLWrite(df)
    
def SQLWrite(df):
    df.show()
    df.write.format('jdbc').mode('overwrite').option('driver','com.mysql.jdbc.Driver').option('url','jdbc:mysql://localhost:3306/bigdata2020').option('dbtable','Tweets').option('user','user').option('password','root').save()
    

spark = SparkSession.builder.appName('twitter-reader').getOrCreate()
ssc = StreamingContext(spark.sparkContext,5)

lines = ssc.textFileStream('hdfs://localhost:9000/user/twitter-data')

counts = lines.flatMap(lambda l:l.split(' ')) \
    .filter(lambda w:w.lower().startswith('#')) \
    .map(lambda w: w.replace('#','')) \
    .map(lambda w: w.lower()) \
    .filter(lambda w: re.sub(r'[^a-z]+','',w)) \
    .filter(lambda w: re.sub(r'[^\x00-\x7F]+','',w)) \
    .map(lambda x: (x,1)) \
    .reduceByKey(lambda x,y:x+y) \
    .map(lambda r: (r[0],r[1])) \
    .foreachRDD(rdd_transform)

ssc.start()
ssc.awaitTermination()

#spark-submit --jars /home/nhobbs/opt/mysql-connector-java-8.0.21/mysql-connector-java-8.0.21.jar twitter-streaming.py 
