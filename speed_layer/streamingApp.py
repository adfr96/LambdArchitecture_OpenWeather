from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from confluent_kafka import Consumer
import json
import sys


PROJ_DIR = '/home/giacomo/Documenti/progetto-2_big_data/'
PROJ_DIR = sys.argv[1]

WIND_THRESHOLD = 4

def toCelsius(temp):
    return float(temp)-273.15

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

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

    temp_stream = d_stream.map(lambda row: (row['regione'],float(toCelsius(row['temp']))))
    #sum_temp = temp_stream.map(lambda a: (a,1)).reduceByWindow(lambda a,b: (a[0]+b[0],a[1]+b[1]),invReduceFunc=None,windowDuration=30,slideDuration=5)
    sum_temp = temp_stream.map(lambda r_t: (r_t[0],(r_t[1],1))).reduceByKeyAndWindow(sum_func,invFunc=diff_func,windowDuration=30,slideDuration=5)
    #sum_temp.saveAsTextFiles(PROJ_DIR+'data/output/test/')
    avg_temp = sum_temp.filter(lambda a: a[1][1]>0).map(lambda a: (a[0],a[1][0]/a[1][1]))

    avg_temp.pprint()
    #avg_temp.saveAsTextFiles(PROJ_DIR+'data/output/test/')
    #d_stream.pprint()
    #d_stream.saveAsTextFiles('output/'))

    avg_temp.pprint(num=1000)
    avg_temp.saveAsTextFiles(PROJ_DIR+'data/output/avg_temp/')
    
    #CONTROLLO VENTO FORTE PER CITTA
    wind_stream = d_stream.map(lambda row: (row['citta'],(row['wind_speed'],row['wind_deg'])))
    wind_stream = wind_stream.filter(lambda c_w: c_w[1][0]>WIND_THRESHOLD)
    #wind_stream.pprint(num=1000)
    #wind_stream.saveAsTextFiles(PROJ_DIR+'data/output/wind/')

    def process(rdd):
        #spark = getSparkSessionInstance(rdd.contex.getConf())
        if(rdd.count() > 0 ):
            spark = SparkSession.builder.getOrCreate()

            rowRdd = rdd.map(lambda c_w: Row(city=c_w[0],wind_speed = c_w[1][0], wind_deg = c_w[1][1]))
            
            wind_dataframe = spark.createDataFrame(rowRdd)

            #wind_dataframe.createOrReplaceTempView("wind")

            #wind_query = spark.sql("select * from wind")
            print("stampa del dataframe")
            wind_dataframe.show()
    
    wind_stream.foreachRDD(process)
        

    ssc.start()
    ssc.awaitTermination(40)