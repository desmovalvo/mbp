#requirement
import sys
import time
from lib.SIBLib import SibLib
from smart_m3.m3_kp import *
import random
import string
from termcolor import colored

# define hosts data
sib_ip = sys.argv[1]
sib_port = int(sys.argv[2])
num_ins = int(sys.argv[3])
num_iter = int(sys.argv[4])

ns= "http://smartM3Lab/Ontology.owl#"
chars = string.ascii_letters + string.digits

triples = {}

def random_generator(size, chars, ns):
    a = ns
    for i in range(size):
        a = a + random.choice(chars)
    return a


kp1 = SibLib(sib_ip, sib_port)

###########################
#                         #
#         JOIN            #
#                         #
###########################

print colored("JOIN...", "blue", attrs=["bold"])
tot = []
for it in range(num_iter):
    start = time.time()
    kp1.join_sib()
    end = time.time() - start
    print "Time: " + str(end)
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print


###########################
#                         #
#         INSERT          #
#                         #
###########################

# inserimento singolo
print colored("INSERT SINGLE TRIPLE...", "blue", attrs=["bold"])
triples[0] = {}
soggetto = random_generator(6, chars, ns)
predicato = random_generator(6, chars, ns)
oggetto = random_generator(6, chars, ns)
triples[0]["soggetto"] = soggetto
triples[0]["predicato"] = predicato
triples[0]["oggetto"] = oggetto

tot = []
for it in range(num_iter):
    start = time.time()
    t = Triple(URI(soggetto), URI(predicato), URI(oggetto))
    kp1.insert(t)
    end = time.time() - start
    print "Time: " + str(end)
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print


# num_ins inserimenti singoli
print colored(str(num_ins) + " SINGLE INSERTIONS...", "blue", attrs=["bold"])
print str(num_ins) + " inserimenti singoli"
tot = []
for it in range(num_iter):
    start = time.time()
    for i in range(1,num_ins+1):
        soggetto = random_generator(6, chars, ns)
        predicato = random_generator(6, chars, ns)
        oggetto = random_generator(6, chars, ns)
        triples[i] = {}
        triples[i]["soggetto"] = soggetto
        triples[i]["predicato"] = predicato
        triples[i]["oggetto"] = oggetto

        t = Triple(URI(soggetto), URI(predicato), URI(oggetto))
        kp1.insert(t)
    end = time.time() - start
    print "Time: " + str(end)
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print

# inserimento di num_ins triple
print colored("INSERT " + str(num_ins) + " TRIPLES...", "blue", attrs=["bold"])
tot = []
for it in range(num_iter):
    start = time.time()
    t = []
    for i in range(num_ins+1, num_ins*2+1):
        soggetto = random_generator(6, chars, ns)
        predicato = random_generator(6, chars, ns)
        oggetto = random_generator(6, chars, ns)
        triples[i] = {}
        triples[i]["soggetto"] = soggetto
        triples[i]["predicato"] = predicato
        triples[i]["oggetto"] = oggetto
        t.append(Triple(URI(soggetto), URI(predicato), URI(oggetto)))
    kp1.insert(t)
    end = time.time() - start
    print "Time: " + str(end)
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print

###########################
#                         #
#         QUERY           #
#                         #
###########################

#query sparql generica
print colored("GENERAL SPARQL QUERY...", "blue", attrs=["bold"])
tot = []
for it in range(num_iter):
    start = time.time()
    query = """SELECT ?s ?p ?o WHERE{?s ?p ?o}"""	
    kp1.execute_sparql_query(query)
    end = time.time() - start
    print "Time: " + str(end) 
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print

#query rdf generica
print colored("GENERAL RDF QUERY...", "blue", attrs=["bold"])
tot = []
for it in range(num_iter):
    start = time.time()
    t = Triple(URI(None), URI(None), URI(None))
    kp1.execute_rdf_query(t)
    end = time.time() - start
    print "Time: " + str(end) 
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print



#query specifiche
key = random.choice(triples.keys())
oggetto = triples[key]["oggetto"]

#query sparql specifica
print colored("SPARQL QUERY: triples with object " + oggetto , "blue", attrs=["bold"])
tot = []
for it in range(num_iter):
    start = time.time()
    query = '''SELECT ?s ?p  WHERE{?s ?p <''' + oggetto  + '''>}'''
    result = kp1.execute_sparql_query(query)
    print result
    end = time.time() - start
    print "Time: " + str(end) 
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print


#query rdf specifica
print colored("RDF QUERY: triples with object " + oggetto , "blue", attrs=["bold"])
tot = []
for it in range(num_iter):
    start = time.time()
    t = Triple(None, None, URI(oggetto))
    result = kp1.execute_rdf_query(t)
    print result
    end = time.time() - start
    print "Time: " + str(end) 
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print


###########################
#                         #
#         LEAVE           #
#                         #
###########################

print colored("LEAVE...", "blue", attrs=["bold"])
tot = []
for it in range(num_iter):
    start = time.time()
    kp1.leave_sib()
    end = time.time() - start
    print "Time: " + str(end)
    tot.append(end)
print "____________________"
tot.sort()
if len(tot)%2 == 0:
    median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
else:
    median = tot[len(tot)/2] 

print colored("Mediana: " + str(median), "blue", attrs=["bold"])
print


