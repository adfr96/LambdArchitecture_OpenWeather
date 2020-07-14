from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from confluent_kafka import Consumer
import json
import datetime
import sys

#import os
#os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'

#PROJ_DIR = '/home/giacomo/Documenti/progetto-2_big_data/'
PROJ_DIR = sys.argv[1]

WIND_THRESHOLD = 4


def show_wind_dataframe(rdd):
        if(rdd.count() > 0 ):
            spark_session = SparkSession.builder().getOrCreate()
            #spark_session = SparkSession.builder.config("spark.mongodb.output.uri",
            #                                            "mongodb://127.0.0.1/db_meteo.real_view_venti").getOrCreate()
            rowRdd = rdd.map(lambda w: Row(city=w['citta'],wind_speed = w['wind_speed'], wind_deg = w['wind_deg'], date = w['date']))
            wind_dataframe = spark_session.createDataFrame(rowRdd)
            #wind_dataframe.write.format("mongo").mode("append").save()

            #print("stampa del dataframe")
            wind_dataframe.show()
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingRecieverKafkaWordCount")
    ssc = StreamingContext(sc, 5) # 5 second window

    d_stream = ssc.socketTextStream('localhost',2020)
    ssc.checkpoint(PROJ_DIR+'data/checkpoint/')
    d_stream = d_stream.flatMap(lambda row: row.split('\n'))
    d_stream = d_stream.map(lambda row: json.loads(row))

    """
    ELABORAZIONE DATI
    """
    def sum_func(a,b):
        return (a[0]+b[0],a[1]+b[1])
    def diff_func(a,b):
        return (a[0]-b[0],a[1]-b[1])
    
    #MEDIA TEMPERATURA PER REGIONE

    temp_stream = d_stream.map(lambda row: (row['regione'],row['temp']))
    sum_temp = temp_stream.map(lambda r_t: (r_t[0],(r_t[1],1))).reduceByKeyAndWindow(sum_func,invFunc=diff_func,windowDuration=30,slideDuration=5)
    #sum_temp.saveAsTextFiles(PROJ_DIR+'data/output/test/')
    avg_temp = sum_temp.filter(lambda a: a[1][1]>0).map(lambda a: {'regione':a[0],'media_temp':a[1][0]/a[1][1]})
    avg_temp.pprint()
    #avg_temp.saveAsTextFiles(PROJ_DIR+'data/output/test/')

    avg_temp.pprint(num=1000)
    avg_temp.saveAsTextFiles(PROJ_DIR+'data/output/avg_temp/')
    
    #CONTROLLO VENTO FORTE PER CITTA
    #TODO convertire data
    wind_stream = d_stream.map(lambda row: {'citta':row['citta'],'wind_speed':row['wind_speed'],'wind_deg':row['wind_deg'],'date': row['datetime']})
    wind_stream = wind_stream.filter(lambda c_w: c_w['wind_speed']>WIND_THRESHOLD)
    #wind_stream.pprint(num=1000)
    #wind_stream.saveAsTextFiles(PROJ_DIR+'data/output/wind/')
   
    wind_stream.foreachRDD(show_wind_dataframe)
        

    ssc.start()
    ssc.awaitTermination(40)