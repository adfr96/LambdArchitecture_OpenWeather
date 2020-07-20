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

"""
l = mycol.find({"ora":10})
for a in l:
    print(a)
"""
#cursor = mycol.aggregate([{"$unwind": "$regione"},{ "$group : { "_id": "$regione"}, "avg_temp": { $avg:"$temp"}}},])
#pipeline = [{"$unwind": "$regione"}, {"$group": {"_id": {"regione":"$regione","anno":"$anno"}, "avg_temp": {"$avg": "$temp"}}},{"$sort": SON([("avg_temp", -1), ("_id", -1)])}]
#pipeline = [{"$unwind": "$regione"}, {"$group": {"_id": {"regione":"$regione","anno":"$anno","mese":"$mese","giorno":"$giorno","ora":"$ora"}, "avg_temp": {"$avg": "$temp"}}}]
#l = mycol.aggregate(pipeline)
#for a in l:
#    print(a)
#mycol.drop()