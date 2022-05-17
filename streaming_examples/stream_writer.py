import pymongo as pm
import csv
import sys

if __name__ == "__main__":
    file_name = sys.argv[-1]
    mongo = pm.MongoClient("mongodb://127.0.0.1:27017")
    db = mongo.get_database("stream_test")
    with open(file_name) as fin:
        content = csv.DictReader(fin)
        for row in content:
            print(f"Writing: {row = }")
            db.test.insert_one(row)
