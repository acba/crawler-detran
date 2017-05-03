import time
import pymongo 
from time import sleep


client = pymongo.MongoClient("localhost", 27017)
db     = client.analiseRenach


valorInicial = db.renach.count()

while(True):
    sleep(5)

    valorFinal = db.renach.count()
    diff = valorFinal - valorInicial
    valorInicial = valorFinal
    print(diff/5, "inserts/s")
