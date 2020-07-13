from confluent_kafka import Producer
import time
import json

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


p = Producer({'bootstrap.servers': 'localhost:9092'})
file_name = '/home/giacomo/Documenti/dati_meteo.json'

file_open = open(file_name,'r')

for line in file_open:
    #row = json.load(line)
    print(f'Linea letta: {line}\n')
    p.poll(0)
    p.produce('test', line.encode('utf-8'), callback=delivery_report)

    time.sleep(1)

p.flush()