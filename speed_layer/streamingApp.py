from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from LambdArchitecture_OpenWeather.properties import PROJ_DIR, TTL, BATCH_DURATION, SLIDE_DURATION, WINDOW_DURATION
import json

"""import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 pyspark-shell'
"""
# PROJ_DIR = '/home/giacomo/Documenti/progetto-2_big_data/'
# PROJ_DIR = sys.argv[1]


WIND_THRESHOLD = 7


def wind_to_row(w):
    return Row(city=w['citta'], wind_speed=w['wind_speed'], wind_deg=w['wind_deg'], date=w['date'])


def rain_to_row(r):
    return Row(city=r['citta'], weather_main=r['main'], rain_1h=r['rain_1h'], rain_3h=r['rain_3h'], date=r['date'])


def rain_now_to_row(r):
    return Row(city=r['citta'], weather_main=r['main'], date=r['date'])


def temp_to_row(t):
    return Row(city=t['citta'], provincia=t['provincia'], regione=t['regione'], media_temp=t['media_temp'],
               anno=t['anno'], mese=t['mese'], giorno=t['giorno'], ora=t['ora'], samples=t['samples'], time=t['time'])


def show_wind_dataframe(rdd):
    if rdd.count() > 0:
        print("print wind dataframe")
    show_dataframe(rdd, wind_to_row)


def show_rain_dataframe(rdd):
    if rdd.count() > 0:
        print("print rain dataframe")
    show_dataframe(rdd, rain_to_row)


def show_rain_now_dataframe(rdd):
    if rdd.count() > 0:
        print("print now rain dataframe")
    show_dataframe(rdd, rain_now_to_row)


def show_temp_dataframe(rdd):
    if rdd.count() > 0:
        print("print temp dataframe")
    show_dataframe(rdd, temp_to_row)


# prende un rdd e una funzione 'to_row' che trasforma una riga dell'rdd in formato Row,
# crea un dataframe dall'rdd e lo printa
# viene chiamata da una 'show_pippo_dataframe(rdd)' 
def show_dataframe(rdd, to_row):
    if rdd.count() > 0:
        spark = SparkSession.builder.getOrCreate()

        rowRdd = rdd.map(to_row)
        dataframe = spark.createDataFrame(rowRdd)
        print(datetime.now())
        dataframe.show(n=1000)


def wind_to_mongo(rdd):
    to_mongo(rdd, wind_to_row, "real_view_venti")


def temp_to_mongo(rdd):
    to_mongo(rdd, temp_to_row, "real_view_temp")


def rain_to_mongo(rdd):
    to_mongo(rdd, rain_to_row, "real_view_rain")


def to_mongo(rdd, to_row, view_name):
    if rdd.count() > 0:
        spark_session = SparkSession.builder.config("spark.mongodb.output.uri",
                                                    "mongodb://127.0.0.1/db_meteo." + view_name).getOrCreate()
        rowRdd = rdd.map(to_row)
        dataframe = spark_session.createDataFrame(rowRdd)

        dataframe.write.format("mongo").mode("append").save()


def reduce_func(a, b):
    return (a[0] + b[0], a[1] + b[1])


def diff_func(a, b):
    return (a[0] - b[0], a[1] - b[1])


def map_temp(row):
    date = datetime.strptime(row['datetime'][:19], '%Y-%m-%dT%H:%M:%S')
    return (row['citta'], row['provincia'], row['regione'], date.year, date.month, date.day, date.hour), (
        row['temp'], 1)


if __name__ == "__main__":
    print(f'start time: {datetime.now()}')

    sc = SparkContext(appName="PythonStreaming_SpeedLayer4LambdArchitecture")
    ssc = StreamingContext(sc, batchDuration=BATCH_DURATION)  # 10 second window

    meteo_stream = ssc.socketTextStream('localhost', 2020)
    ssc.checkpoint(PROJ_DIR + 'data/checkpoint/')
    meteo_stream = meteo_stream.flatMap(lambda row: row.split('\n'))
    meteo_stream = meteo_stream.map(lambda row: json.loads(row))

    """
    ELABORAZIONE DATI
    """

    # MEDIA TEMPERATURA

    # temp_stream = meteo_stream.map(lambda row: (row['citta'],row['provincia'],row['regione'],row['temp']))
    temp_stream = meteo_stream.map(map_temp)
    # filtrare quelle dell'ora attuale
    temp_stream = temp_stream.window(windowDuration=WINDOW_DURATION, slideDuration=SLIDE_DURATION).filter(
        lambda row: row[0][6] == datetime.now().hour)
    temp_stream = temp_stream.reduceByKey(reduce_func)
    """temp_stream = temp_stream.reduceByKeyAndWindow(reduce_func, invFunc=diff_func,
                                                                  windowDuration=WINDOW_DURATION,
                                                                  slideDuration=SLIDE_DURATION)
    """
    # reduce_temp.saveAsTextFiles(PROJ_DIR+'data/output/test/')
    avg_temp = temp_stream.filter(lambda a: a[1][1] > 0).map(lambda a: {'citta': a[0][0],
                                                                        'provincia': a[0][1], 'regione': a[0][2],
                                                                        'anno': a[0][3], 'mese': a[0][4],
                                                                        'giorno': a[0][5], 'ora': a[0][6],
                                                                        'media_temp': a[1][0] / a[1][1],
                                                                        'samples': a[1][1],
                                                                        'time': datetime.now()})

    # avg_temp.pprint(num=1000)
    # avg_temp.saveAsTextFiles(PROJ_DIR+'data/output/avg_temp/')
    avg_temp.foreachRDD(show_temp_dataframe)
    avg_temp.foreachRDD(temp_to_mongo)

    # CONTROLLO VENTO FORTE PER CITTA

    wind_stream = meteo_stream.map(
        lambda row: {'citta': row['citta'], 'wind_speed': row['wind_speed'], 'wind_deg': row['wind_deg'],
                     'date': row['datetime']})
    wind_stream = wind_stream.filter(lambda c_w: c_w['wind_speed'] > WIND_THRESHOLD)
    # wind_stream.pprint(num=1000)
    # wind_stream.saveAsTextFiles(PROJ_DIR+'data/output/wind/')

    wind_stream.foreachRDD(show_wind_dataframe)
    wind_stream.foreachRDD(wind_to_mongo)

    # PIOGGIA A BREVE

    rain_stream = meteo_stream.map(
        lambda row: {'citta': row['citta'], 'main': row['weather_main'], 'rain_1h': row['rain_1h'],
                     'rain_3h': row['rain_3h'],
                     'date': row['datetime']})
    rain_stream = rain_stream.filter(
        lambda rain: rain['main'] == 'Rain' or rain['rain_1h'] != 0 or rain['rain_3h'] != 0)
    # rain_stream = rain_stream.filter(lambda rain: rain['rain_1h'] != 0 or rain['rain_3h'] != 0)

    rain_stream.foreachRDD(show_rain_dataframe)
    rain_stream.foreachRDD(rain_to_mongo)

    ssc.start()
    ssc.awaitTermination(TTL + 20)
