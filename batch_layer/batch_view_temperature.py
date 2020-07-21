from pyspark.sql import SparkSession,Row
import datetime
import sys
import os
os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'

def toCelsius(temp):
    if temp>100:
        return (float(temp) - 273.15)
    else:
        return temp

def reduce_temp(t1, t2):
    t = {}
    t['sum_temp']=  t1['sum_temp'] + t2['sum_temp']
    t['samples'] =  t1['samples'] + t2['samples']
    t['temp_max'] = max(t1['temp_max'],t2['temp_max'])
    t['temp_min'] = min(t1['temp_min'], t2['temp_min'])
    return t

def convert_str_to_date(str_date):
    date_obj = datetime.datetime.strptime(str_date, '%Y-%m-%d')
    return date_obj.date()

def map_k_v(line):
    date_time_obj = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S')
    return ((line[0],line[1],line[2],date_time_obj.year,date_time_obj.month,date_time_obj.day,date_time_obj.hour),\
           {'sum_temp':toCelsius(float(line[7])),'samples':1,'temp_max':toCelsius(float(line[10])),'temp_min':toCelsius(float(line[9]))})

#verifica dei parametri passati in input se argv==2 allora i due parametri indicano rispettivamente,
#data di inizio e data di fine periodo di osservazione, altrimenti ci si aspetta un solo parametro in input,
#che rappresenta il numero di giorni di osservazione andando a ritroso a partire dalla data odierna
flag_periodo = 0
if(sys.argv == 2):
    inizio_periodo = convert_str_to_date(sys.argv[1])
    fine_periodo = convert_str_to_date(sys.argv[2])
    flag_periodo = 1
else:
    days_before = int(sys.argv[1])

def temp_to_row(w):
    return Row(citta=w['citta'], provincia=w['provincia'], regione=w['regione'],anno=w['anno'],mese=w['mese'],giorno=w['giorno'],ora=w['ora'],\
               temp_avg =w['temp_avg'], temp_max = w['temp_max'], temp_min = w['temp_min'])

def filtro_data_with_fine_periodo(line):
    data = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S').date()
    return (data >= inizio_periodo) and (data <= fine_periodo)

def filtro_data_without_fine_periodo(line):
    ultima_data_utile = datetime.date.today()- datetime.timedelta(days=days_before)
    data = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S').date()
    return (data >= ultima_data_utile)

ss = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.output.uri", f"mongodb://127.0.0.1/db_meteo.batch_view_temp_{sys.argv[1]}").getOrCreate()

dati_meteo = ss.sparkContext.textFile("hdfs://localhost:9000/user/giacomo/input/dati_meteo_without_header.csv")

if flag_periodo:
    data  = fine_periodo
    i = 0
    while data>= inizio_periodo:
        data = datetime.date.today() - datetime.timedelta(days=i)
        try:
            dati_meteo_un_giorno = ss.sparkContext.textFile(f'hdfs://localhost:9000/user/giacomo/input/dati_meteo_{data}.csv')
            dati_meteo = dati_meteo.union(dati_meteo_un_giorno)
            break
        except Exception:
            print("file Not Found\n")
        i += 1

else:
    for i in range(days_before):
        data =datetime.date.today() - datetime.timedelta(days=days_before)
        try:
            dati_meteo_un_giorno  = ss.sparkContext.textFile(f'hdfs://localhost:9000/user/giacomo/input/dati_meteo_{data}.csv')
            dati_meteo = dati_meteo.union(dati_meteo_un_giorno)
            break
        except Exception:
            print("file Not Found\n")

rdd_meteo = dati_meteo.map(lambda line: line.split(','))
#verifica se in input è stato passato un intervallo temporale oppure il numero di giorni a partire dalla data odierna su
# cui si vuole fare l'analisi
if(flag_periodo):
    rdd_meteo = rdd_meteo.filter(filtro_data_with_fine_periodo)
else:
    rdd_meteo = rdd_meteo.filter(filtro_data_without_fine_periodo)
rdd_meteo = rdd_meteo.map(map_k_v)

rdd_meteo = rdd_meteo.reduceByKey(reduce_temp)
rdd_meteo = rdd_meteo.map(lambda a: (a[0],{'temp_avg':a[1]['sum_temp']/a[1]['samples'],'temp_max':a[1]['temp_max'],'temp_min':a[1]['temp_min']}))
rdd_meteo = rdd_meteo.map(lambda a:{'citta':a[0][0],'provincia':a[0][1],'regione':a[0][2],'anno':a[0][3],'mese':a[0][4],'giorno':a[0][5],'ora':a[0][6],\
                                    'temp_avg':a[1]['temp_avg'],'temp_max':a[1]['temp_max'],'temp_min':a[1]['temp_min']})
row_rdd_meteo = rdd_meteo.map(temp_to_row)
#rdd_meteo = rdd_meteo.map(lambda a: (a[0][0],{'anno':a[0][1],'mese':a[0][2],'giorno':a[0][3],'ora':a[0][4],'temp':a[1]}))
#rdd_meteo.foreach(lambda a: print(a))
"""
result = result.collect()
for a in result:
    print(a)
"""
temp_dataFrame = ss.createDataFrame(row_rdd_meteo)#,['citta','anno','mese','giorno','ora','temp'])
temp_dataFrame.write.format("mongo").mode("append").save()
#data = datetime.datetime.now()
#print(f'anno:{data.year}    mese:{data.month}   giorno:{data.day}   ora:{data.hour}')