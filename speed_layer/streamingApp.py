from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from confluent_kafka import Consumer
import json

PROJ_DIR = '/home/adfr/Documenti/python-BigData/progetto2/'

def toCelsius(temp):
    return float(temp)-273.15

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
    
    
    temp_stream = d_stream.map(lambda row: toCelsius(row['temp']))
    sum_temp = temp_stream.map(lambda a: (a,1)).reduceByWindow(lambda a,b: (a[0]+b[0],a[1]+b[1]),invReduceFunc=None,windowDuration=30,slideDuration=5)
    
    avg_temp = sum_temp.map(lambda a: a[0]/a[1])
    
    avg_temp.pprint()
    #avg_temp.saveAsTextFiles(PROJ_DIR+'data/output/test/')
    
    ssc.start()
    ssc.awaitTermination(40)