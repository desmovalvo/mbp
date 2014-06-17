#!/usr/bin/python

# requirements
import sys
import rdflib
import logging
from SIBLib import *
from smart_m3.m3_kp_api import *
import timeit

# logger configuration
logging.basicConfig(filename="logfile", level=logging.DEBUG)
logger = logging.getLogger("logger")

# parse command line arguments
n3file = sys.argv[1]
sib_ip = sys.argv[2]
sib_port = int(sys.argv[3])
virt_sib_ip = sys.argv[4]
virt_sib_port = int(sys.argv[5])
num_iter = int(sys.argv[6])
triple_list = []
kps = [] 

# join the SIB
rkp = SibLib(sib_ip, sib_port)

#vkp = SibLib(virt_sib_ip, virt_sib_port)
#kps.append(vkp)
kps.append(rkp)

#############################################################
#
# CLEAN
#
#############################################################

print "\nCleaning the sib...\n"
rkp.remove(Triple(None, None, None))
print "Triples expected at this step: 0"
print "Triples at this step: " + str(len(rkp.execute_rdf_query(Triple(None, None, None))))

# creating a Graph instance
print "Creating a graph instance...",
g = rdflib.Graph()
print "OK"

# parsing the graph
print "Parsing the turtle file...",
res = g.parse('sp2b.n3', format='n3')
print "OK"


# building triple_list:
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
        tr = Triple(s[0], s[1], s[2])
        triple_list.append(tr)


median_arrays = []
for client in kps:

    tot = []
    # insert
    for it in range(num_iter):   

        print "Iteration: " + str(it)
    
        # insert the triples
        end = timeit.timeit(lambda: client.insert(triple_list), number=1)
        # append the time
        tot.append(end)

        # clean sib
#        client.remove(Triple(None, None, None))

    # sort the array and pick the median value
    tot.sort()
    if len(tot)%2 == 0:
        median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
    else:
        median = tot[len(tot)/2]     
            
    median_arrays.append([median * 1000])


for i in median_arrays:
    print "MEDIAN ARRAY: " + str(i)
bar_chart = pygal.Bar()
# chart = pygal.StackedLine(fill=True, interpolate='cubic', style=LightGreenStyle)
bar_chart.add('Real SIB', median_arrays[0])
bar_chart.add('Virtual SIB', median_arrays[1])
bar_chart.render_to_file('multiple_sub_insert.svg')



    
        # except socket.error:
        #     print "SOCKET ERROR"
        #     sys.exit()

        # except:
        #     print "ERROR!"
        #     print sys.exc_info()
        #     logger.error(sys.exc_info())
