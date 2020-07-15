from pyspark.sql import SparkSession
from pyspark.sql import SparkSession,Row
import datetime
import sys
import os
os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'

#inizio = datetime.datetime.now().time()
#print(inizio)
ss = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.output.uri", f"mongodb://127.0.0.1/db_meteo.batch_view_max_vento_{sys.argv[1]}").getOrCreate()

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

def wind_to_row(w):
    return Row(citta=w['citta'],regione=w['regione'], wind_speed=w['wind_speed'], wind_deg=w['wind_deg'], date=w['datetime'])

def convert_str_to_date(str_date):
    date_obj = datetime.datetime.strptime(str_date, '%Y-%m-%d')
    return date_obj.date()

def filtro_data_with_fine_periodo(line):
    data = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S').date()
    return (data >= inizio_periodo) and (data <= fine_periodo)

def filtro_data_without_fine_periodo(line):
    ultima_data_utile = datetime.date.today()- datetime.timedelta(days=days_before)
    data = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S').date()
    return (data >= ultima_data_utile)

flag_periodo = 0
if(sys.argv == 2):
    inizio_periodo = convert_str_to_date(sys.argv[1])
    fine_periodo = convert_str_to_date(sys.argv[2])
    flag_periodo = 1
else:
    days_before = int(sys.argv[1])
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

if(flag_periodo):
    rdd_meteo = rdd_meteo.filter(filtro_data_with_fine_periodo)
else:
    rdd_meteo = rdd_meteo.filter(filtro_data_without_fine_periodo)

rdd_meteo = rdd_meteo.map(lambda a: ((a[0],a[2]),{'wind_speed':float(a[13]),'wind_deg':float(a[14]), 'datetime':a[20]}))

rdd_meteo = rdd_meteo.reduceByKey(max_wind_speed)

#rdd_meteo.foreach(lambda a: print(a))
rdd_meteo = rdd_meteo.map(lambda a: {'citta':a[0][0],'regione':a[0][1],'wind_speed':a[1]['wind_speed'],'wind_deg':a[1]['wind_deg'],\
                                     'datetime':a[1]['datetime']})
row_rdd_meteo = rdd_meteo.map(wind_to_row)
wind_dataFrame = ss.createDataFrame(row_rdd_meteo)
wind_dataFrame.write.format("mongo").mode("append").save()

#fine = datetime.datetime.now().time()
#print(fine)
