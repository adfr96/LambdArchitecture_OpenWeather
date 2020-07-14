
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'
my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db_meteo.result_temp").getOrCreate()

def somma_temp(t1,t2):
    return t1+t2

#conf = SparkConf().setAppName("test_spark_hadoop2")
#sc = SparkContext(conf=conf)

dati_meteo = my_spark.sparkContext.textFile("hdfs://localhost:9000/user/giacomo/input/data_aws_without_header.csv")

rdd_meteo = dati_meteo.map(lambda line: line.split(','))
rdd_meteo = rdd_meteo.map(lambda a: (a[0],float(a[7])))
count_per_provincia = rdd_meteo.groupByKey().map(lambda a: (a[0], len(list(a[1]))))
rdd_meteo = rdd_meteo.reduceByKey(somma_temp)
unione = rdd_meteo.join(count_per_provincia)
unione = unione.map(lambda a: (a[0],a[1][0]/a[1][1]))
unione.foreach(lambda a: print(a))
#a = unione.collect()
#for elem in a:
#    print(elem)
temp = my_spark.createDataFrame(unione.collect(),['citta','temperatura'])
temp.write.format("mongo").mode("append").save()

#conta = dati_meteo.count()
#print(f"Line: {conta}\n")