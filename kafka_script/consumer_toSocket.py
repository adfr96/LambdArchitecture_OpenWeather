from confluent_kafka import Consumer
import socket
import sys
import time
import json

from LambdArchitecture_OpenWeather.propertis import PORT_CONSUMER_TO_STREAMING

HOST = 'localhost'
address = (HOST, PORT_CONSUMER_TO_STREAMING)
name = sys.argv[1]

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': name,
    'auto.offset.reset': 'earliest'
})

c.subscribe(['test'])
s = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
s.bind(address)
s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
print("prima della connessione")
s.listen()
conn, addr = s.accept()
print("dopo la connessione")

start_time = time.time()
now = start_time

while now < start_time + 30:

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
    print("sended message to socket")
    # print('Received message: {}'.format(msg.value().decode('utf-8')))

s.close()
c.close()
