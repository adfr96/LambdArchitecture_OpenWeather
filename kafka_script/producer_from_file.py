from confluent_kafka import Producer
import time
import json
import jsons
import sys
import datetime

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def toCelsius(temp):
    return float(temp)-273.15

def conv_date(date):
    return datetime.datetime.fromtimestamp(date)

path_file = sys.argv[1]
p = Producer({'bootstrap.servers': 'localhost:9092'})
file_name = path_file+'dati_meteo.json'
#file_name = path_file+'mini_data.json'

file_open = open(file_name,'r')

for line in file_open:
    row = json.loads(line)
    row["temp"]=toCelsius(row["temp"])
    row["temp_min"]=toCelsius(row["temp_min"])
    row["temp_max"]=toCelsius(row["temp_max"])
    row["datetime"] = conv_date(row["datetime"])	
    row_string = jsons.dumps(row)
    print(f'Linea letta: {row_string}\n')
    p.poll(0)
    p.produce('test', row_string.encode('utf-8'), callback=delivery_report)

    time.sleep(1)

p.flush()