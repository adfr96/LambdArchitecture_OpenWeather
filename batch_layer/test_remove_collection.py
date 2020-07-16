import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["db_meteo"]
"""
mycol = mydb["batch_view_avg_temp_1"]
mycol = mydb["batch_view_avg_temp_3"]
mycol = mydb["batch_view_avg_temp_7"]
mycol = mydb["batch_view_avg_temp_15"]
mycol = mydb["batch_view_avg_temp_30"]

mycol = mydb["batch_view_wind_max_1"]
mycol = mydb["batch_view_wind_max_3"]
mycol = mydb["batch_view_wind_max_7"]
mycol = mydb["batch_view_wind_max_15"]
mycol = mydb["batch_view_wind_max_30"]
"""
mycol = mydb["batch_view_temp_3"]
l = mycol.find({"ora":10})
for a in l:
    print(a)

#mycol.drop()