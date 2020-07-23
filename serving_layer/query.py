import pymongo
import sys
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["db_meteo"]

print("1.andamento venti ultimi 3 giorni \n2.andamento venti ultimi 7 giorni\n"
      "3.andamento venti ultimi 15 giorni\n4.andamento venti ultimi 30 giorni\n"
      "5.andamento temperature ultimi 3 giorni\n6.andamento temperature ultimi 7 giorni\n"
      "7.andamento temperature ultimi 15 giorni\n8.andamento temperature ultimi 30 giorni\n"
      "9.real_view_temp\n")
opt = input()


"""
Realizzazione di alcune query per consultare i risultati ottenuti
"""
if opt == '1':
    col = mydb['batch_view_venti_3']
    print("Velocità massima dei venti negli utlimi 3 giorni:\n")
else:
    if opt == '2':
        col = mydb['batch_view_venti_7']
    else:
        if opt == '3':
            col = mydb['batch_view_venti_15']
            print("ok\n")
        else:
            if opt == '4':
                col = mydb['batch_view_venti_30']
            else:
                if opt == '5':
                    col = mydb['batch_view_temp_3']
                    print("*********** 5 *****************\n")
                else:
                    if opt == '6':
                        col = mydb['batch_view_temp_7']
                    else:
                        if opt == '7':
                            col = mydb['batch_view_temp_15']
                            print("Temperature massime negli ultimi 15 giorni:\n")
                        else:
                            if opt == '8':
                                col = mydb['batch_view_temp_30']
                                print("no\n")
                            else:
                                col = mydb['real_view_temp']
                                print("**************** 9 ********\n")

if opt=='1' or opt =='2' or opt =='3' or opt =='4':
    pipeline = [ { "$match": { "regione": "EMR" } },{"$group": {"_id": {"provincia":"$provincia","regione":"$regione"}, "wind_speed_max": {"$max": "$wind_speed_max"}}}]
    l = col.aggregate(pipeline)
    l = sorted(l, key=lambda a: a['wind_speed_max'], reverse=True)
    for a in l:
        print(f"{a['_id']} wind_speed_max: {a['wind_speed_max']} m/sec")
else:
    if opt=='5' or opt =='6' or opt =='7' or opt =='8':
        pipeline = [ { "$match": { "giorno": 22, "citta":"Roma" } },{"$group": {"_id": {"citta":"$citta", "giorno":"$giorno","ora":"$ora"}, "media_temp": {"$avg": "$temp_max"}}}]
        l = col.aggregate(pipeline)
        #l = sorted(l, key=lambda a: a['temp_max'], reverse=True)
        l = sorted(l, key=lambda a: a['_id']['ora'])
        for a in l:
            print(f"{a['_id']} temperatura media: {a['media_temp']:.2f} °C")
    else:
        print("****** 9 *******\n")
        pipeline = [{"$match": {"giorno": 22, "city": "Roma"}}, {"$group": {"_id": {"citta": "$city", "giorno": "$giorno", "ora": "$ora"},
                       "media_temp": {"$avg": "$media_temp"}}}]
        l = col.aggregate(pipeline)
        l = sorted(l, key=lambda a: a['_id']['ora'])
        for a in l:
            print(f"{a['_id']} temperatura media: {a['media_temp']:.2f} °C")

