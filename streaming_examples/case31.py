import pymongo as pm

mongo = pm.MongoClient("mongodb://127.0.0.1:27017")
db = mongo.get_database("stream_test")

pipe_3_1 = [
    {
        "$match": {
            "$and": [
                {"$expr": {"$eq": ["111.", {"$substr": ["$fullDocument.ip", 0, 4]}]}},
                {"$expr": {"$eq": ["200", "$fullDocument.status"]}},
                {"$expr": {"$ne": [None, "$fullDocument.userid"]}},
            ]
        }
    }
]

s_3_1 = db.test.watch(pipe_3_1)

for r in s_3_1:
    print(r)
