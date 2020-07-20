import pymongo
from bson.son import SON
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["db_meteo"]