def xorThis(string, chave):

    novaString = ""
    for i in range(len(string)):
        novaString += '{0:x}'.format(int(string[i], 16) ^ int(chave[i%len(chave)], 16)) 
    return novaString


string = "06529813489"
chave  = "12345678910"

a = xorThis(string, chave)

print(a)
print(xorThis(a, chave))

