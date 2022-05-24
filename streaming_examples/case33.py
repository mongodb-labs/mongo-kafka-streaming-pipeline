import pymongo as pm

mongo = pm.MongoClient("mongodb://127.0.0.1:27017")
db = mongo.get_database("stream_test")

db.score_max.create_index( [("player", 1)], unique=True )

s_3_3 = db.test.watch()

for r in s_3_3:
    doc = r['fullDocument']
    print(doc)
    player = doc['player']
    score = int(doc['score'])

    # a very stupid approach, could be better?
    # do we have something like, match_or_default?
    curr_max = db.score_max.find_one({ "player": player })
    print(curr_max)
    if not curr_max:
        db.score_max.insert_one( { "player": player, "score": score })
    else:
        if curr_max['score'] < score:
            db.score_max.update_one( { "player": player }, { "$set": { "score": score } } )
    print('View:')
    view = db.score_max.find()
    for v in view:
        print(v)

