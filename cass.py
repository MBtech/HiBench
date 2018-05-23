from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
#import pyspark_cassandra
import sys

masterip = sys.argv[1]
cassip = sys.argv[2]
conf = SparkConf() \
	.setAppName("PySpark Cassandra Test") \
	.setMaster("spark://"+masterip+":7077") \
	.set("spark.cassandra.connection.host", cassip)
sc = SparkContext(conf=conf)
#sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

sqlContext = SQLContext(sc)

#textFile = sc.textFile("hdfs://172.31.12.123:9000/flight.csv")
#print textFile.take(10)
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferSchema = 'true').load('hdfs://172.31.12.123:9000/flight.csv')
df.printSchema()
lowerdf = df.toDF(*[c.lower() for c in df.columns])
lowerdf.describe('ARR_DELAY').show()
newdf = lowerdf.select('year', 'month', 'day_of_month', 'unique_carrier','fl_num','arr_delay')
newdf.show(5)
#df.describe().show()
#csvFile = textFile.map(lambda line: (line.split(',')[0], line.split(',')[1])).collect()
#df.saveToCassandra(
#	"kv",
#	"test",
#	ttl=timedelta(hours=1),
#)

newdf.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="kv", keyspace="test")\
    .save()

