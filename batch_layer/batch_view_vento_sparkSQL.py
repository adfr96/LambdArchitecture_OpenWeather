from pyspark.sql import SparkSession
import os
#os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'
my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db_meteo.meteo_provincie")\
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db_meteo.result_wind_speed_SQL")\
    .getOrCreate()
df = my_spark.read.format("mongo").option("uri","mongodb://127.0.0.1/db_meteo.meteo_provincie").load()
#df.show()
df.createOrReplaceTempView("meteo")
sqlDF = my_spark.sql("SELECT citta, max(wind_speed) AS wind_speed FROM meteo GROUP BY citta")
sqlDF.write.format("mongo").mode("append").save()