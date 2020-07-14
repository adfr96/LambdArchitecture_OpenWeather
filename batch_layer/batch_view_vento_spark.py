from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import datetime
import sys
import os
os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'

inizio = datetime.datetime.now().time()
print(inizio)
ss = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db_meteo.batch_view_venti").getOrCreate()

def max_wind_speed(ws1,ws2):
    ws_max = {}
    if(ws1['wind_speed']>ws2['wind_speed']):
        ws_max['wind_speed']=ws1['wind_speed']
        ws_max['wind_deg']=ws1['wind_deg']
        ws_max['datetime']=ws1['datetime']
    else:
        ws_max['wind_speed'] = ws2['wind_speed']
        ws_max['wind_deg'] = ws2['wind_deg']
        ws_max['datetime'] = ws2['datetime']
    return ws_max

def con_str_to_datetime(str_date):
    date_time_obj = datetime.datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S')
    return date_time_obj.date()

'''
data_odierna e ultima_data_utile servono ad individuare il periodo temporale su cui si vuole fare l'analisi.
In particolare ultima_data_utile viene ricavata a partire dalla data odierna e sottrendovi i giorni passati
come argomento a batch_view_pioggia.py (il parametro che si passa all'avvio dell'esecuzione indica il periodo
temporale a partire dalla data odierna su cui si vuole fare l'analisi
'''
data_odierna = datetime.date.today()
period = int(sys.argv[1])
ultima_data_utile = datetime.date.today() - datetime.timedelta(days=period)

dati_meteo = ss.sparkContext.textFile("hdfs://localhost:9000/user/giacomo/input/data_aws_without_header.csv")

rdd_meteo = dati_meteo.map(lambda line: line.split(','))
rdd_meteo = rdd_meteo.filter(lambda line: con_str_to_datetime(line[20])>=ultima_data_utile)
rdd_meteo = rdd_meteo.map(lambda a: (a[0],{'wind_speed':float(a[13]),'wind_deg':float(a[14]), 'datetime':a[20]}))

rdd_meteo = rdd_meteo.reduceByKey(max_wind_speed)

rdd_meteo.foreach(lambda a: print(a))

wind_dataFrame = ss.createDataFrame(rdd_meteo.collect(), ['max_wind_speed', 'wind_deg'])
wind_dataFrame.write.format("mongo").mode("append").save()
fine = datetime.datetime.now().time()
print(fine)
