from multiprocessing import cpu_count
from multiprocessing import Pool

import pymongo 
import json



# def insertMongo(cpf):

#     client = pymongo.MongoClient("localhost", 27017, connect=False)
#     db     = client.analiseRenach

#     doc = {"_id": cpf}        

#     try:
#         db.cpfs.insert_one(doc)
#     except pymongo.errors.DuplicateKeyError:
#         pass


client = pymongo.MongoClient("localhost", 27017)
db     = client.analiseRenach


if __name__ == '__main__':    

    mongoData = []
    with open('data/cpfs.txt', 'r') as f:
        data = json.load(f)

        for cpf in data["cpf"]:
            mongoData.append({"_id":cpf})

        print("Inserindo...")
        db.cpfs.insert_many(mongoData)