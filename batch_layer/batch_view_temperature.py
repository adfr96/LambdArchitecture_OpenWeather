from pyspark.sql import SparkSession,Row
import datetime
import sys
import os

os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'


def sum_temp(t1,t2):
    return {'sum_temp':t1['sum_temp']+t2['sum_temp'],'samples':t1['samples']+t2['samples']}

def convert_str_to_date(str_date):
    date_obj = datetime.datetime.strptime(str_date, '%Y-%m-%d')
    return date_obj.date()

def map_k_v(line):
    date_time_obj = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S')
    return ((line[0],line[2],date_time_obj.year,date_time_obj.month,date_time_obj.day,date_time_obj.hour),{'sum_temp':float(line[7]),'samples':1})

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
    return Row(citta=w['citta'], regione=w['regione'],anno=w['anno'],mese=w['mese'],giorno=w['giorno'],ora=w['ora'],temp=w['temp'])

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

dati_meteo = ss.sparkContext.textFile("hdfs://localhost:9000/user/giacomo/input/data_aws_without_header.csv")

rdd_meteo = dati_meteo.map(lambda line: line.split(','))
#verifica se in input è stato passato un intervallo temporale oppure il numero di giorni a partire dalla data odierna su
# cui si vuole fare l'analisi
if(flag_periodo):
    rdd_meteo = rdd_meteo.filter(filtro_data_with_fine_periodo)
else:
    rdd_meteo = rdd_meteo.filter(filtro_data_without_fine_periodo)
rdd_meteo = rdd_meteo.map(map_k_v)

rdd_meteo = rdd_meteo.reduceByKey(sum_temp)
rdd_meteo = rdd_meteo.map(lambda a: (a[0],a[1]['sum_temp']/a[1]['samples']))
rdd_meteo = rdd_meteo.map(lambda a:{'citta':a[0][0],'regione':a[0][1],'anno':a[0][2],'mese':a[0][3],'giorno':a[0][4],'ora':a[0][5],'temp':a[1]})
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