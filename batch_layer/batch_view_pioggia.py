from pyspark.sql import SparkSession,Row
import datetime
import sys

def rain_to_row(w):
    return Row(citta=w['citta'], provincia=w['provincia'], regione=w['regione'],anno=w['anno'],mese=w['mese'],giorno=w['giorno'],ora=w['ora'],\
               rain = w['rain_1h'])

def convert_str_to_date(str_date):
    date_obj = datetime.datetime.strptime(str_date, '%Y-%m-%d')
    return date_obj.date()

def map_k_v(line):
    date_time_obj = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S')
    return ((line[0],line[1],line[2],date_time_obj.year,date_time_obj.month,date_time_obj.day,date_time_obj.hour),\
            {'rain_1h':float(line[16]),'minuti':date_time_obj.minute})

flag_periodo = 0
if(sys.argv == 2):
    inizio_periodo = convert_str_to_date(sys.argv[1])
    fine_periodo = convert_str_to_date(sys.argv[2])
    flag_periodo = 1
else:
    days_before = int(sys.argv[1])


def filtro_data_with_fine_periodo(line):
    data = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S').date()
    return (data >= inizio_periodo) and (data <= fine_periodo)


def filtro_data_without_fine_periodo(line):
    ultima_data_utile = datetime.date.today() - datetime.timedelta(days=days_before)
    data = datetime.datetime.strptime(line[20], '%Y-%m-%d %H:%M:%S').date()
    return (data >= ultima_data_utile)

def pioggia_per_ora(v1,v2):
    if v1['minuti']>v2['minuti']:
        return {'rain_1h':v1['rain_1h'],'minuti':v1['minuti']}
    else:
        return {'rain_1h': v2['rain_1h'], 'minuti': v2['minuti']}

ss = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.output.uri", f"mongodb://127.0.0.1/db_meteo.batch_view_rain_{sys.argv[1]}").getOrCreate()

#dati_meteo = ss.sparkContext.textFile("hdfs://localhost:9000/user/giacomo/input/data_aws_duplicato_02_without_header.csv")
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
rdd_meteo = rdd_meteo.reduceByKey(pioggia_per_ora)
rdd_meteo = rdd_meteo.map(lambda a: {'citta':a[0][0], 'provincia':a[0][1],'regione':a[0][2],\
                                     'anno':a[0][3],'mese':a[0][4],'giorno':a[0][5],'ora':a[0][6],'rain_1h':a[1]['rain_1h']})
row_rdd_meteo = rdd_meteo.map(rain_to_row)
temp_dataFrame = ss.createDataFrame(row_rdd_meteo)
temp_dataFrame.write.format("mongo").mode("append").save()