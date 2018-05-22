from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

conf = SparkConf().setAppName("myFirstApp").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

textFile = sc.textFile("hdfs://172.31.12.123:9000/flight.csv")
print textFile.take(10)
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('hdfs://172.31.12.123:9000/flight.csv')
#df.describe().show()
#csvFile = textFile.map(lambda line: (line.split(',')[0], line.split(',')[1])).collect()
df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="kv", keyspace="test")\
    .save()

