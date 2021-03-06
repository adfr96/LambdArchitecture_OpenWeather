from datetime import datetime
from confluent_kafka import Consumer
import sys
import time
import json
import pydoop.hdfs as hdfs
import csv

from LambdArchitecture_OpenWeather.properties import TTL,TOPIC

name = sys.argv[1]


def save_to_file(row_list):
    """
    per il partion veritcal, i dati vengono partizionati giornalmente, tutti i dati dello stesso giorno,
    sono raccolti nello stesso file con nome: dati_meteo_data_del_giorno
    """
    #inizialmente prima di realizzare il partition vertical i dati vengono caricati all'interno dello stesso file
    # with hdfs.open("data/data_openWeather.csv", mode='at') as csv_file:
    today = data = datetime.date.today()
    with hdfs.open(f'data/dati_meteo_{today}', mode='at') as csv_file:
        fieldnames = row_list[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writerows(row_list)

        csv_file.close()


print(f'Start time: {datetime.now()}')
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': name,
    'auto.offset.reset': 'earliest'
})

c.subscribe([TOPIC])

start_time = time.time()
now = start_time
row_list = []
SAVE_TIME = 10  # second
last_save = now
while now < start_time + TTL:

    now = time.time()
    msg = c.poll(1.0)

    if msg is None:
        time.sleep(0.5)
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    row = json.loads(msg.value().decode('utf-8'))  # dict

    row_list.append(row)
    if now > last_save + SAVE_TIME:
        save_to_file(row_list)
        row_list.clear()
        last_save = time.time()

c.close()
print(f'End time: {datetime.now()}')
