import pymongo as pm

mongo = pm.MongoClient("mongodb://127.0.0.1:27017")
db = mongo.get_database("stream_test")

pipe_3_2 = [
    {
        "$addFields": {
            "fullDocument.sizeInKB": {
                "$divide": [
                    { "$toDouble": "$fullDocument.bytes"},
                    1000
                ]
            }
        }
    }
]

s_3_2 = db.test.watch(pipe_3_2)

for r in s_3_2:
    print(r)
