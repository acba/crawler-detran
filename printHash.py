import pymongo 
import shutil

from os import listdir
from os.path import isfile, join

def xorThis(string, chave="12345678910"):

    novaString = ""
    for i in range(len(string)):
        novaString += '{0:x}'.format(int(string[i], 16) ^ int(chave[i%len(chave)], 16)) 

    return novaString


client = pymongo.MongoClient("localhost", 27017)
db     = client.analiseRenach

path = 'data/fotosRenach/'
onlyfiles = [f[:-6] for f in listdir(path) if isfile(join(path, f))]
setFileName = set(onlyfiles)
listFileName = list(setFileName)
print("Diferentes RENACHS nas fotos: ", len(setFileName))

# print(setFileName)
indexaveis = [r for r in db.renach.find({"renach": {"$in": listFileName}})]

print("Indexaveis: ", len(indexaveis))

with open('cpfsFotos.txt', 'w') as w:
    for obj in indexaveis:
        print("RENACH: ", obj["renach"], " CPF: ", obj["cpf"], " --> ", xorThis(obj["cpf"]), file=w)