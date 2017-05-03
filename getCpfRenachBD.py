import pymssql  
import pymongo 
import json

conn = pymssql.connect(server='10.128.24.10', user='sispesquisa', password='@$1$P3$Qu1$@', database='sispesquisa')  

client = pymongo.MongoClient("localhost", 27017)
db     = client.analiseRenach

dados = []

cursor = conn.cursor()  
cursor.execute('SELECT CPF, Renach FROM dbo.cnh;')  
row = cursor.fetchone()  

while row:
    obj = {"_id": row[0], "cpf": row[0], "renach": row[1]}
    try:
        db.renach.insert_one(obj)
    except pymongo.errors.DuplicateKeyError:
        pass

    # print(row[0], row[1])
    row = cursor.fetchone()  