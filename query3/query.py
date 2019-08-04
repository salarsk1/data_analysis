import pymongo
client = pymongo.MongoClient("mongodb://localhost:27017/") 
db = client["air"]
from pymongo import cursor
import pprint


outputOrder = [("DAY_OF_WEEK", pymongo.DESCENDING), ("ARR_DELAY", pymongo.ASCENDING), ("TAIL_NUM", pymongo.DESCENDING), ("AIR_TIME", pymongo.ASCENDING)]
print("QUESTION 1")
p1 = db.flights.find({"$and": [{ "OP_UNIQUE_CARRIER": "MQ"  }, 
                               {"DAY_OF_MONTH":1}]}, {"_id":0} ).sort(outputOrder)
p1.limit(5)
for i in p1:
    print(i)

print("QUESTION 2")
p2 = db.flights.find({"$and": [{ "OP_UNIQUE_CARRIER": "MQ"  }, 
                               {"DAY_OF_MONTH":5}, {"MONTH":5}]}, {"_id":0} ).sort(outputOrder).limit(15)
for i in p2:
    print(i)

db.flights.insert_one({ "YEAR" : 2020, "MONTH" : 9, "DAY_OF_MONTH" : 1, "DAY_OF_WEEK" : 7, "No schema" : "Yes" })
db.flights.insert_many([{ "YEAR" : 2020, "MONTH" : 10, "DAY_OF_MONTH" : 1, "DAY_OF_WEEK" : 3 },
                        {"YEAR" : 2020, "MONTH" : 10, "DAY_OF_MONTH" : 2, "DAY_OF_WEEK" : 4 }])
print("QUESTION 5")
p5 = db.flights.find({"YEAR":2020}, {"_id":0}).sort(outputOrder)
for i in p5:
    print(i)

db.flights.update_many({"YEAR":2020}, {"$set":{"OP_UNIQUE_CARRIER":"CS"}})

print("QUESTION 7")
p7 = db.flights.find({"OP_UNIQUE_CARRIER":"CS"}, {"_id":0}).sort(outputOrder)
for i in p7:
    print(i)

db.flights.delete_many({"OP_UNIQUE_CARRIER":"CS"})

print("QUESTION 9")
p9 = db.flights.find({"OP_UNIQUE_CARRIER":"CS"}, {"_id":0}).sort(outputOrder)
for i in p9:
    print(i)
p10 = db.flights.aggregate([{
                            "$lookup":
                            {"from":"airlines",
                            "localField":"OP_UNIQUE_CARRIER",
                            "foreignField":"OP_UNIQUE_CARRIER",
                            "as":"airline"
                            }
                            },
                            {"$out":"temp1"}])


p11 = db.temp1.find({},{"_id":0}).sort(outputOrder).limit(10)
print("QUESTION 11")

for i in p11:
    print(i)

p12 = db.flights.aggregate([{
                            "$lookup":
                            {"from":"airlines",
                            "localField":"OP_UNIQUE_CARRIER",
                            "foreignField":"OP_UNIQUE_CARRIER",
                            "as":"airline"
                            }
                            },
                           {
                           "$lookup":
                           {"from":"airports",
                           "localField":"ORIGIN_AIRPORT_ID",
                           "foreignField":"AIRPORT_ID",
                           "as":"origin"
                           }
                           },
                           {
                           "$lookup":
                           {"from":"airports",
                           "localField": "DEST_AIRPORT_ID",
                           "foreignField":"AIRPORT_ID",
                           "as":"dest"
                           }
                           },
                           {"$out":"temp2"}])
print("QUESTION 13")

p13 = db.temp2.find({},{"_id":0}).sort(outputOrder).limit(10)

for i in p13:
    print(i)


