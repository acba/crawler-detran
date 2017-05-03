import requests
import time
from time import sleep
import os  
import random
import pymongo 
import json
import datetime


url = "http://wsdetran.pb.gov.br/DT_SITUACAOCNH_CLIENTE/SituacaoCNH"
data = {
    "p_cpf": "57588392487"
}

def formataDados(res):
    # print()
    # print(res)
    # print()

    if len(res) > 0:
        mongoData = {
                    "prontuarioGeral": str(res["numPgu"]),
                    "cpf"            : str(res["num_Cpf"]),
                    "nome"           : str(res["ds_Nomeco"]),
                    "registro"       : str(res["numReg"]),
                    "renach"         : str(res["numFor"]),
                    "dataValidade"   : str(res["valCnh"]),
                    "msgErro"        : str(res["msg_Erro"]),
                    "situacaoCNH"    : str(res["ds_Sitcnh"]),
                    "diaNumCpf"      : str(res["dia_NumCpf"])
                } 

        mongoData["cpf"]          = "0"*(11 - len(mongoData["cpf"])) + mongoData["cpf"]
        mongoData["_id"]          = mongoData["cpf"]

        if len(res["msg_Erro"].strip()) == 0:
            
            mongoData["diaNumCpf"]    = "0"*(11 - len(mongoData["diaNumCpf"])) + mongoData["diaNumCpf"]
            mongoData["nome"]         = mongoData["nome"].strip()
            mongoData["situacaoCNH"]  = mongoData["situacaoCNH"].strip()
            if mongoData["situacaoCNH"] == "EMISSAO CONFIRMADA":
                mongoData["dataValidade"] = datetime.datetime.strptime(mongoData["dataValidade"].strip(), "%d/%m/%Y")
            else:
                mongoData["dataValidade"] = None
            mongoData["msgErro"]      = mongoData["msgErro"].strip()
            mongoData["renach"]       = mongoData["renach"].strip()
            mongoData["registro"]     = mongoData["registro"].strip()
            
        else:

            mongoData["msgErro"]      = mongoData["msgErro"].strip()
            mongoData["diaNumCpf"]    = None
            mongoData["nome"]         = None
            mongoData["dataValidade"] = None
            mongoData["situacaoCNH"]  = None
            mongoData["renach"]       = None
            mongoData["registro"]     = None
            
        return mongoData
    else:
        return False

client = pymongo.MongoClient("localhost", 27017)
db     = client.analiseRenach

numTotalCpfs = 3806076
cpfsEncontrados = [r["cpf"] for r in db.renach.find({}, {"cpf":1, "_id":0})]

cnt = len(cpfsEncontrados)

for doc in db.cpfs.find({"_id": {"$nin": cpfsEncontrados}}):
    cpf = doc["_id"]
    start = time.time()

    data = {"p_cpf": cpf}
    response = requests.post(url, data)
    if response.status_code == 200:
        res = json.loads(response.text)
        
        mongoData = formataDados(res)
        
        if mongoData:
            try:
                db.renach.insert_one(mongoData)
            except pymongo.errors.DuplicateKeyError:
                pass

    sleep(random.random()/10)
    end = time.time()
    print(cnt/numTotalCpfs, "% finalizado", end-start, "tempo")
    cnt+=1


