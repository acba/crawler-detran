def xorThis(string, chave):

    novaString = ""
    for i in range(len(string)):
        novaString += '{0:x}'.format(int(string[i], 16) ^ int(chave[i%len(chave)], 16)) 
    return novaString


string = "97630049124"
chave  = "12345678910"

a = xorThis(string, chave)

print(string, "XORCrypt --> ", a)
print(a, "XORCrypt --> ", xorThis(a, chave))

