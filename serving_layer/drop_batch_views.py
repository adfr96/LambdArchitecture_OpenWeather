import pymongo
from bson.son import SON
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["db_meteo"]
collections = mydb.collection_names()

prefix_view = "batch"
for c in collections:
    mycol = mydb[c]
    if prefix_view in c:
        print(f'cancellazione view: {mycol}')
        #print(mycol)
        mycol.drop()

