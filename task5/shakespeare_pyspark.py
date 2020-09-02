from pyspark.sql import SparkSession

# create a spark context
ss = SparkSession.builder.appName('Shakespeare').getOrCreate()

# create rdd from text file, stored in lines
lines = ss.read.text('../Shakespeare.txt').rdd.map(lambda x:x[0])

# show the first 20 lines
print('First 20 lines: ', lines.take(20))

# display the count of the first 20 lines
lineCount = lines.map(lambda x:len(x))
print('Line Count : ' + ' '.join(map(str,lineCount.take(20))))







