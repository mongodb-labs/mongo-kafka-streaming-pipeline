import pymongo as pm
import csv
import sys
import random
import time

if __name__ == "__main__":
    file_name = sys.argv[-1]
    mongo = pm.MongoClient("mongodb://127.0.0.1:27017")
    db = mongo.get_database("stream_test")
    with open(file_name) as fin:
        content = csv.DictReader(fin)
        for row in content:
            sleep_time = random.randint(100, 800)
            print(f"{sleep_time = }")
            time.sleep(sleep_time / 1000)
            print(f"Writing: {row = }")
            db.test.insert_one(row)
