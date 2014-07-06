#!/usr/bin/python

# requirements
import sys
import rdflib
import logging
from SIBLib import *
from smart_m3.m3_kp_api import *

# logger configuration
logging.basicConfig(filename="logfile", level=logging.DEBUG)
logger = logging.getLogger("logger")

# parse command line arguments
n3file = sys.argv[1]
sib_ip = sys.argv[2]
sib_port = int(sys.argv[3])

# join the SIB
a = SibLib(sib_ip, sib_port)

# creating a Graph instance
print "Creating a graph instance...",
g = rdflib.Graph()
print "OK"

# parsing the graph
print "Parsing the turtle file...",
res = g.parse('sp2b.n3', format='n3')
print "OK"

# Printing all the triples:
counter = 0
for triple in res:

    s = []
    for t in triple:

        if type(t).__name__  == "URIRef":
            s.append( URI(t.toPython()) )
            
        elif type(t).__name__  == "Literal":
            s.append( Literal(t.toPython()) )

        elif type(t).__name__  == "BNode":
            s.append( bNode(t.toPython()) )

    if len(s) == 3:
        counter += 1
        tr = Triple(s[0], s[1], s[2])
        ins_ok = False
        while ins_ok != True:
            try:
                print "Inserting the triple " + str(counter) + ":",
                a.insert(tr)
                ins_ok = True
            except:
                print "Exception... we'll try again in a few seconds"
                print sys.exc_info()
        print "ok!"

        # except socket.error:
        #     print "SOCKET ERROR"
        #     sys.exit()

        # except:
        #     print "ERROR!"
        #     print sys.exc_info()
        #     logger.error(sys.exc_info())
