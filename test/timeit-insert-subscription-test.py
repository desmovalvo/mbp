#!/usr/bin/python

# requirements
import time
import sys
import timeit
import pygal
from SIBLib import *
from termcolor import *
from smart_m3.m3_kp_api import *

# namespace
ns = "http://ns#"

# cli args
sib_ip = sys.argv[1]
sib_port = int(sys.argv[2])
rem_sib_ip = sys.argv[3]
rem_sib_port = int(sys.argv[4])
num_iter = int(sys.argv[5])

# variables
step = 50
min = 0
max = int(sys.argv[6])
median_arrays = []

# times
join_time = None
single_insert_time = None
single_remove_time = None
multi_insert_time = None
multi_remove_time = None
single_rdf_query_time = None
multi_rdf_query_time = None
whole_rdf_query_time = None
single_sparql_query_time = None
multi_sparql_query_time = None
whole_sparql_query_time = None
leave_time = None

# Handler class
class TestHandler:
     def __init__(self):	
         pass
             # print "handle init"
     def handle(self, added, removed):
             # print "handle"
             # print "ADDED: " + str(added)
	     # print "REMOVED: " + str(removed)
         pass

# triples to insert
ti  = []
for k in range(5):
    ti.append(Triple(URI(ns + "subject" + str(k)), URI(ns + "predicate" + str(k)), URI(ns + "object" + str(k))))
    
print "Testing the time elapsed by a INSERT REQUEST related to 5 triples, varying the number of subscriptions"

#############################################################
#
# JOIN and CLEAN
#
#############################################################

kp = []
#kp.append(SibLib(sib_ip, sib_port))
#kp[0].remove(Triple(None, None, None))
kp.append(SibLib(rem_sib_ip, rem_sib_port))
kp[0].remove(Triple(None, None, None))

#############################################################
#
# SUBSCRIBE and INSERT
#
#############################################################

for client in kp:

    median_array = []

    # step iteration
    for i in range(0, max/step):
        print "Inserting " + str(len(ti)) + " triples at once"
    
        tot = []

        subscriptions = []
        print "\tSubscribing to [" + str((i+1) * step) + '] triples'
        
        for k in range(((i+1)*step)):
             sub = client.create_rdf_subscription(Triple(None,None,None), TestHandler())
             subscriptions.append(sub)
             print k
             
        for it in range(num_iter):   

            print '* [' + str((i+1) * step) + '] Iteration: ' + str(it)
    
            # generate (i+1)*step triples
    
            # insert the triples
            print "\tInsert of the 5 triples"
            end = timeit.timeit(lambda: client.insert(ti), number=1)
            # append the time
            tot.append(end)

            # clean sib
            client.remove(Triple(None, None, None))

        # unsubscribe
        print "\tUnsubscribing to [" + str((i+1) * step) + '] triples'
        for s in subscriptions:
            client.unsubscribe(s)
    
        # sort the array and pick the median value
        tot.sort()
        if len(tot)%2 == 0:
            median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
        else:
            median = tot[len(tot)/2]     
            
        median_array.append(median * 1000)

    median_arrays.append(median_array)

for i in median_arrays:
    print "MEDIAN ARRAY: " + str(i)
bar_chart = pygal.Bar()
# chart = pygal.StackedLine(fill=True, interpolate='cubic', style=LightGreenStyle)
bar_chart.add('Local SIB', median_arrays[0])
bar_chart.add('Remote SIB', median_arrays[1])
bar_chart.render_to_file('multiple_sub_insert.svg')


#############################################################
#
# CLEAN
#
#############################################################

print "\nCleaning the sib...\n"
kp1 = SibLib(sib_ip, sib_port)
kp1.remove(Triple(None, None, None))
print "Triples expected at this step: 0"
print "Triples at this step: " + str(len(kp1.execute_rdf_query(Triple(None, None, None))))
kp1.leave_sib()
