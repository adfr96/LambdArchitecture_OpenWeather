from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from confluent_kafka import Consumer
import json

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingRecieverKafkaWordCount")
    ssc = StreamingContext(sc, 3) # 2 second window    

    d_stream = ssc.socketTextStream('localhost',2020)
    #d_stream = d_stream.flatMap(lambda row: row.split('\n'))
    #d_stream = d_stream.map(lambda row: json.loads(row))
    """
    ELABORAZIONE DATI
    """
    #d_stream = d_stream.map(lambda row: row['citta'])


    d_stream.pprint()
    #d_stream.saveAsTextFiles('output/')
    
    ssc.start()
    ssc.awaitTermination(40)