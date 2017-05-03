from multiprocessing import cpu_count
from multiprocessing import Pool

import requests
import time
from time import sleep
import os  
import random
import pymongo 
import json
import datetime


def formataDados(res):
    # print()
    # print(res)
    # print()

    if res:
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
            if mongoData["situacaoCNH"] == "EMISSAO CONFIRMADA" and len(mongoData["dataValidade"].strip())>0:
                print(mongoData["dataValidade"].strip())
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

def fazRequisicao(cpf):

    client = pymongo.MongoClient("localhost", 27017, connect=False)
    db     = client.analiseRenach
    url = "http://wsdetran.pb.gov.br/DT_SITUACAOCNH_CLIENTE/SituacaoCNH"

    data = {"p_cpf": cpf}

    try:
        response = requests.post(url, data)
        if response.status_code == 200:
            res = json.loads(response.text)
            
            mongoData = formataDados(res)
            
            if mongoData:
                try:
                    db.renach.insert_one(mongoData)
                except pymongo.errors.DuplicateKeyError:
                    pass
    except requests.exceptions.ConnectionError:
        sleep(60)
        fazRequisicao(cpf)


if __name__ == '__main__':    

    client = pymongo.MongoClient("localhost", 27017)
    db     = client.analiseRenach

    numTotalCpfs = 3806076
    cpfsEncontrados = [r["cpf"] for r in db.renach.find({}, {"cpf":1, "_id":0})]
    for i in range(0, len(cpfsEncontrados), 1000):        
        ce = cpfsEncontrados[i:i+1000]     
        db.cpfs.delete_many({"_id": {"$in": ce}})
    cnt = len(cpfsEncontrados)
    print("CPFs Encontrados: ", cnt)

    cpfsRestantes = [doc["_id"] for doc in db.cpfs.find({})]
    print("CPFs Restantes: ", len(cpfsRestantes))

    with Pool(8) as p:
        for result in p.imap(fazRequisicao, cpfsRestantes):
            pass
