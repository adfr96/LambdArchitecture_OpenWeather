import requests
import json
import csv
import time
import datetime
import pandas as pd



id_provincie = pd.read_csv("/home/giacomo/Documenti/script_progetto-2_big_data/id_provincie.csv", sep=',')
id_provincie = id_provincie.values

with open('/home/giacomo/Documenti/dati_meteo.csv', 'w') as file:
	writer = csv.writer(file)
	writer.writerow(["citta","provincia","regione","coord","weather_main","weather_description","temp","feels_like","temp_min","temp_max","pressure","humidity",
					 "wind_speed","wind_deg","cloudiness","rain_1h","rain_3h","snow_1h","snow_3h","datetime"])

	for provincia in id_provincie:
		#param_request = f"http://api.openweathermap.org/data/2.5/weather?id={provincia[3]}&appid=mettereID&lang=it"
		r = requests.get(param_request)
		risp = r.text
		#print(risp)

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
			
		row["temp"]=data["main"]["temp"]
		row["feels_like"]=data["main"]["feels_like"]
		row["temp_min"]=data["main"]["temp_min"]
		row["temp_max"]=data["main"]["temp_max"]
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
		
		row["datetime"] = datetime.datetime.fromtimestamp(data["dt"])+datetime.timedelta(hours=2)

		
		writer.writerow([row["citta"],row["provincia"],row["regione"],row["coord"],row["weather_main"],row["weather_description"],
			row["temp"],row["feels_like"],row["temp_min"],row["temp_max"],row["pressure"],row["humidity"],
			row["wind_speed"],row["wind_deg"],row["cloudiness"],row["rain_1h"],row["rain_3h"],row["snow_1h"],row["snow_3h"],
			row["datetime"]])
		
		time.sleep(1)
	file.close()