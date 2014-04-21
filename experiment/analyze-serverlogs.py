from os import listdir
from sys import argv

d = argv[1]
tofind = argv[2]
var = argv[3]

for f in listdir(argv[1]):
    if f.find("IT") == -1:
        continue
    print f
    f = argv[1]+'/'+f
    serverresults = {}
    for g in listdir(f):
        if g.find("S") == -1:
            continue
        g = f+'/'+g
        s = g.split("/S")[1]
        lookout = False
        for line in open(g+"/server.log"):
            if line.find("edu.berkeley.kaiju") != -1:
                
                if line.find(tofind) != -1:
                    lookout=True
                else:
                    lookout=False
            if lookout and line.find(tofind) == -1 and line.find(var) != -1:
                serverresults[s] = line.split("=")[1]

    for s in serverresults:
        print s, serverresults[s],
                
