from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import datetime
import sys
import os

os.environ['PYSPARK_SUBMIT_ARGS']= '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'


def sum_temp(t1,t2):
    return {'sum_temp':t1['sum_temp']+t2['sum_temp'],'samples':t1['samples']+t2['samples']}

def con_str_to_date(str_date):
    date_obj = datetime.datetime.strptime(str_date, '%Y-%m-%d')
    return date_obj.date()

def map_k_v(line):
    date_time_obj = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S')
    return ((line[0],date_time_obj.year,date_time_obj.month,date_time_obj.day,date_time_obj.hour),{'sum_temp':float(line[7]),'samples':1})

inizio  = con_str_to_date(sys.argv[1])
fine = con_str_to_date(sys.argv[2])

def filtro_data(line):
    data = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S').date()
    return (data >= inizio) and (data<=fine)

#data_odierna = datetime.date.today()

ss = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.output.uri", f"mongodb://127.0.0.1/db_meteo.batch_view_temp_1").getOrCreate()

dati_meteo = ss.sparkContext.textFile("hdfs://localhost:9000/user/giacomo/input/data_aws_without_header.csv")

rdd_meteo = dati_meteo.map(lambda line: line.split(','))
rdd_meteo = rdd_meteo.filter(filtro_data)
rdd_meteo = rdd_meteo.map(map_k_v)

rdd_meteo = rdd_meteo.reduceByKey(sum_temp)
rdd_meteo = rdd_meteo.map(lambda a: (a[0],a[1]['sum_temp']/a[1]['samples']))
result = rdd_meteo.map(lambda a:{'citta':a[0][0],'anno':a[0][1],'mese':a[0][2],'giorno':a[0][3],'ora':a[0][4],'temp':a[1]})

#rdd_meteo = rdd_meteo.map(lambda a: (a[0][0],{'anno':a[0][1],'mese':a[0][2],'giorno':a[0][3],'ora':a[0][4],'temp':a[1]}))
#rdd_meteo.foreach(lambda a: print(a))
"""
result = result.collect()
for a in result:
    print(a)
"""
temp_dataFrame = ss.createDataFrame(result.collect())#,['citta','anno','mese','giorno','ora','temp'])
temp_dataFrame.write.format("mongo").mode("append").save()
#data = datetime.datetime.now()
#print(f'anno:{data.year}    mese:{data.month}   giorno:{data.day}   ora:{data.hour}')