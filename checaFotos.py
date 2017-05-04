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

for obj in indexaveis:
    obj["_id"] = obj["cpf"]
    try:
        db.indexaveis.insert_one(obj)
    except pymongo.errors.DuplicateKeyError:
        pass

cnt = 0
cntTotal = len(indexaveis)
outPath = 'data/fotosFormatadas/'
for doc in db.indexaveis.find():
    nomeFoto       = path+doc["renach"]+"-F.jpg"
    assinaturaFoto = path+doc["renach"]+"-A.jpg"

    # print("Copiando ", doc["renach"])

    if isfile(nomeFoto):
        outNomeFoto = outPath+xorThis(doc["cpf"])+"-F.jpg"
        shutil.copy(nomeFoto, outNomeFoto)

        print(nomeFoto, " ---> ", outNomeFoto)

    if isfile(assinaturaFoto):
        outAssinaturaFoto = outPath+xorThis(doc["cpf"])+"-A.jpg"
        shutil.copy(assinaturaFoto, outAssinaturaFoto)

        print(assinaturaFoto, " ---> ", outAssinaturaFoto)

    if cnt%50 == 0:
        print(format(cnt/cntTotal, ".4f"), "%", cnt)
    cnt +=1

# for doc in db.renach.find({"renach": {"$in": listFileName}}):
