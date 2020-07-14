from confluent_kafka import Producer
import time
import requests
import json
import jsons
import pandas as pd
import sys
import datetime

PROJ_DIR = sys.argv[1]
appId = sys.argv[2]

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

def getLineFromWebSocket(provincia):
    param_request = f'http://api.openweathermap.org/data/2.5/weather?id={provincia[3]}&appid={appId}&lang=it'
    r = requests.get(param_request)
    risp = r.text

    data = json.loads(risp)

    row = {}

    row["citta"]=data.get("name",0)
        
    row["provincia"]=provincia[1]
        
    row["regione"]= provincia[2]

    row["coord"] = data["coord"]

    if data.get("weather",0):
        row["weather_main"]=data["weather"][0]["main"]
        row["weather_description"]=data["weather"][0]["description"]
    else:
        row["weather_main"]="Not Defined"
        row["weather_description"]="Not Defined"
            
    row["temp"]=toCelsius(data["main"]["temp"])
    row["feels_like"]=data["main"]["feels_like"]
    row["temp_min"]=toCelsius(data["main"]["temp_min"])
    row["temp_max"]=toCelsius(data["main"]["temp_max"])
    row["pressure"]=data["main"]["pressure"]
    row["humidity"]=data["main"]["humidity"]

    if(data.get("wind",0)):
        row["wind_speed"]=data["wind"]["speed"]
        row["wind_deg"] = data["wind"]["deg"]
    else:
        row["wind_speed"]=0
        row["wind_deg"] = 0

    if(data.get("clouds",0)):
        row["cloudiness"]=data["clouds"]["all"]
    else:
        row["cloudiness"] = 0

    if(data.get("rain",0)):
        if(data.get("rain").get("1h",0)):
            row["rain_1h"]=data["rain"]["1h"]
        else:
            row["rain_1h"]=0
        if(data.get("rain").get("3h",0)):
            row["rain_3h"]=data["rain"]["3h"]
        else:	
            row["rain_3h"]=0
    else:
        row["rain_1h"]=0
        row["rain_3h"]=0

    if(data.get("snow",0)):
        if(data.get("snow").get("1h",0)):
            row["snow_1h"]=data["snow"]["1h"]
        else:
            row["snow_1h"]=0
        if(data.get("snow").get("3h",0)):
            row["snow_3h"]=data["snow"]["3h"]
        else:	
            row["snow_3h"]=0
    else:
        row["snow_1h"]=0
        row["snow_3h"]=0

    #row["datetime"] = data["dt"]
    row["datetime"] = conv_date(data["dt"])	

    return (jsons.dumps(row))

def getProvincia(id_provincie):
    if(INDEX == len(id_provincie)):
        INDEX = 0
    tmp = id_provincie[INDEX]
    INDEX+=1
    return tmp


INDEX = 0


id_provincie = pd.read_csv(PROJ_DIR+'data/id_provincie.csv', sep=',')
id_provincie = id_provincie.values

p = Producer({'bootstrap.servers': 'localhost:9092'})

start_time = time.time()
now = start_time

while now<start_time+30:
    now = time.time()
    line  = getLineFromWebSocket(getProvincia(id_provincie))
    
    p.poll(0)
    p.produce('test', line.encode('utf-8'), callback=delivery_report)

    time.sleep(1)

p.flush()