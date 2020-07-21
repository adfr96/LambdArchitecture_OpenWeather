from pyspark.sql import SparkSession
from pyspark.sql import SparkSession,Row
import datetime
import sys
import os
#os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'

#inizio = datetime.datetime.now().time()
#print(inizio)
ss = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.output.uri", f"mongodb://127.0.0.1/db_meteo.batch_view_venti_{sys.argv[1]}").getOrCreate()

def map_k_v(line):
    date_time_obj = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S')
    return ((line[0],line[1],line[2],date_time_obj.year,date_time_obj.month,date_time_obj.day,date_time_obj.hour),\
           {'wind_speed_max':float(line[13]),'wind_deg_max':float(line[14]),\
            'wind_speed_min':float(line[13]),'wind_deg_min':float(line[14]),'datetime_max':line[20]})

def reduce_wind(ws1, ws2):
    ws_max = {}
    #max
    if(ws1['wind_speed_max']>ws2['wind_speed_max']):
        ws_max['wind_speed_max']=ws1['wind_speed_max']
        ws_max['wind_deg_max']=ws1['wind_deg_max']
        ws_max['datetime_max']=ws1['datetime_max']
    else:
        ws_max['wind_speed_max'] = ws2['wind_speed_max']
        ws_max['wind_deg_max'] = ws2['wind_deg_max']
        ws_max['datetime_max'] = ws2['datetime_max']
    #min
    if (ws1['wind_speed_min'] < ws2['wind_speed_min']):
        ws_max['wind_speed_min'] = ws1['wind_speed_min']
        ws_max['wind_deg_min'] = ws1['wind_deg_min']
    else:
        ws_max['wind_speed_min'] = ws2['wind_speed_min']
        ws_max['wind_deg_min'] = ws2['wind_deg_min']
    return ws_max

def wind_to_row(w):
    return Row(citta=w['citta'],provincia=w['provincia'],regione=w['regione'], \
               anno=w['anno'], mese = w['mese'], giorno = w['giorno'], ora = w['ora'],\
               wind_speed_max=w['wind_speed_max'], wind_deg_max=w['wind_deg_max'], date_max=w['datetime_max'],\
               wind_speed_min=w['wind_speed_min'],wind_deg_min=w['wind_deg_min'])

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

dati_meteo = ss.sparkContext.textFile("hdfs://localhost:9000/user/giacomo/input/dati_meteo_without_header.csv")

rdd_meteo = dati_meteo.map(lambda line: line.split(','))

if(flag_periodo):
    rdd_meteo = rdd_meteo.filter(filtro_data_with_fine_periodo)
else:
    rdd_meteo = rdd_meteo.filter(filtro_data_without_fine_periodo)

rdd_meteo = rdd_meteo.map(map_k_v)

rdd_meteo = rdd_meteo.reduceByKey(reduce_wind)

#rdd_meteo.foreach(lambda a: print(a))
rdd_meteo = rdd_meteo.map(lambda a: {'citta':a[0][0],'provincia':a[0][1],'regione':a[0][2],\
                                     'anno':a[0][3],'mese':a[0][4],'giorno':a[0][5],'ora':a[0][6],\
                                     'wind_speed_max':a[1]['wind_speed_max'],'wind_deg_max':a[1]['wind_deg_max'],\
                                     'datetime_max':a[1]['datetime_max'],\
                                     'wind_speed_min':a[1]['wind_speed_min'],'wind_deg_min':a[1]['wind_deg_min']})
row_rdd_meteo = rdd_meteo.map(wind_to_row)
wind_dataFrame = ss.createDataFrame(row_rdd_meteo)
wind_dataFrame.write.format("mongo").mode("append").save()

#fine = datetime.datetime.now().time()
#print(fine)
