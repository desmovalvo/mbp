#!/usr/bin/python

# requirements
import sys
import timeit
import pygal
from SIBLib import *
from termcolor import *
from smart_m3.m3_kp_api import *

# namespace
ns = "http://ns#"

# cli args
real_sib_ip = sys.argv[1]
real_sib_port = int(sys.argv[2])
virtual_sib_ip = sys.argv[3]
virtual_sib_port = int(sys.argv[4])
multi_sib_ip = sys.argv[5]
multi_sib_port = int(sys.argv[6])
num_iter = int(sys.argv[7])
maxn = int(sys.argv[8])
sibtype = sys.argv[9]

# variables
step = 25
min = 0
median_arrays = []

# filename
filename = "insert_test_" + str(num_iter) + "iter_" + str(step) + "step_" + str(maxn) + "max_" + sibtype + ".svg"


#############################################################
#
# JOIN and CLEAN
#
#############################################################

kp = []
# kp connected to the real sib
kp.append(SibLib(real_sib_ip, real_sib_port))
#clear the real sib
kp[0].remove(Triple(None, None, None))
# kp connected to the virtual sib
kp.append(SibLib(virtual_sib_ip, virtual_sib_port))
# kp connected to the multi sib
kp.append(SibLib(multi_sib_ip, multi_sib_port))

print "Sibs joined"

#############################################################
#
# INSERT
#
#############################################################

for client in kp:

    median_array = []

    # step iteration
    for i in range(0, maxn/step):
        print "Inserting " + str((i+1) * step) + " triples at once"
    
        tot = []
        for it in range(num_iter):   
    
            print '* [' + str((i+1) * step) + '] Iteration: ' + str(it)
    
            # generate (i+1)*step triples
            t = []
            for k in range(((i+1)*step)):
                tt = Triple(URI(ns + "subject" + str(it) + "_" + str(k)), URI(ns + "predicate" + str(it)), URI(ns + "object" + str(it)))
                # print tt
                t.append(tt)
    
            # insert the triples
            end = timeit.timeit(lambda: client.insert(t), number=1)
             # print "\tTriples at this step: " + str(len(client.execute_rdf_query(Triple(None, None, None))))
            tot.append(end)

            # clean sib
            client.remove(Triple(None, None, None))
    
        # sort the array and pick the median value
        tot.sort()
        if len(tot)%2 == 0:
            median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
        else:
            median = tot[len(tot)/2]     
            
        median_array.append(round(median * 1000, 2))

    median_arrays.append(median_array)

for i in median_arrays:
    print "MEDIAN ARRAY: " + str(i)
bar_chart = pygal.Bar()
# chart = pygal.StackedLine(fill=True, interpolate='cubic', style=LightGreenStyle)
bar_chart.title = 'Insertion time increasing the number of triples inserted'
bar_chart.add('Real SIB (' + sibtype + ')', median_arrays[0])
bar_chart.add('Virtual SIB', median_arrays[1])
bar_chart.add('Multi SIB', median_arrays[2])
bar_chart.render_to_file(filename)


#############################################################
#
# CLEAN
#
#############################################################

print "\nCleaning the sib...\n"
kp1 = SibLib(real_sib_ip, real_sib_port)
kp1.remove(Triple(None, None, None))
print "Triples expected at this step: 0"
print "Triples at this step: " + str(len(kp1.execute_rdf_query(Triple(None, None, None))))
kp1.leave_sib()
