from pyspark import SparkContext

# create a spark context
sc = SparkContext(appName='shakespeare')

# create rdd from text file, stored in lines
txt = sc.textFile('../Shakespeare.txt')

# show the first 20 lines
print('First 10 lines: ', txt.take(20))

# display the count of the first 20 lines
lineCount = txt.map(lambda x:len(x))
print('Line Count : ' + ' '.join(map(str,lineCount.take(10))))







