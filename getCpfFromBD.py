import pymssql  
import pymongo 
import json


conn = pymssql.connect(server='10.128.24.10', user='sispesquisa', password='@$1$P3$Qu1$@', database='sispesquisa')  

client = pymongo.MongoClient("localhost", 27017)
db     = client.analiseRenach

dados = {"cpf": []}

with open('data/cpfs.txt', 'w') as w:
    cursor = conn.cursor()  
    cursor.execute('SELECT CPF FROM pessoas;')  
    row = cursor.fetchone()  
    while row:  
        dados["cpf"].append(row[0])
        # print(row[0], file=w)
        row = cursor.fetchone()  

    json.dump(dados, w)
    db.cpfs.insert_one(dados)