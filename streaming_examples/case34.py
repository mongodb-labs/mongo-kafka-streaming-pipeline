import pymongo as pm
from collections import Counter

mongo = pm.MongoClient("mongodb://127.0.0.1:27017")
db = mongo.get_database("stream_test")

# a tumbling window per second
s_3_4 = db.test.watch([{
                          "$project": {
                              "timestamp": {
                                  "$dateTrunc": {
                                      "date": "$clusterTime",
                                      "unit": "second"
                                  }
                              },
                              "fullDocument": 1
                          }
                      }])


cur_timestamp = None
counter = Counter()
for r in s_3_4:
    print(r)
    if r['timestamp'] != cur_timestamp:
        if cur_timestamp:
            db.occurrences.insert_one({ 'timestamp': cur_timestamp, **counter })
        counter = Counter()
        cur_timestamp = r['timestamp']
    counter[r['fullDocument']['feature']] += 1
    print('View')
    view = db.occurrences.find()
    for v in view:
        print(v)
