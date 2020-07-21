from datetime import datetime
from confluent_kafka import Consumer
import socket
import sys
import time
import json

from LambdArchitecture_OpenWeather.properties import PORT_CONSUMER_TO_STREAMING, TTL


HOST = 'localhost'
address = (HOST, PORT_CONSUMER_TO_STREAMING)
name = sys.argv[1]
topic = sys.argv[2]

print(f'Start time: {datetime.now()}')
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': name,
    'auto.offset.reset': 'earliest'
})

c.subscribe([topic])
s = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
s.bind(address)
s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
print("In attesa della connessione")

s.listen()
conn, addr = s.accept()
print(f'Connesso a {conn.getpeername()}')

start_time = time.time()
now = start_time

while now < start_time + TTL:

    now = time.time()
    msg = c.poll(1.0)

    if msg is None:
        time.sleep(0.5)
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    row = json.loads(msg.value().decode('utf-8'))
    message = json.dumps(row) + '\n'
    sended = conn.send(message.encode())
    print(f'sended message to socket: {conn.getpeername()}')
    #print('Received message: {}'.format(msg.value().decode('utf-8')))

s.close()
c.close()
print(f'End time: {datetime.now()}')
