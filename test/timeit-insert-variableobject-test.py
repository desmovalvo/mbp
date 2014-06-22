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
step = 500
min = 0
maxn = int(sys.argv[6])
median_arrays = []
sibtype = sys.argv[7]
filename = "timeit_insert_variable_object_" + str(num_iter) + "iter_" + str(maxn) + "max_" + sibtype + ".svg"

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

#############################################################
#
# JOIN and CLEAN
#
#############################################################

kp = []
kp.append(SibLib(sib_ip, sib_port))
kp[0].remove(Triple(None, None, None))
kp.append(SibLib(rem_sib_ip, rem_sib_port))
kp[1].remove(Triple(None, None, None))

#############################################################
#
# INSERT
#
#############################################################

for client in kp:
    
    median_array = []
    obj = ""

    # cycle
    for i in range(0, maxn):
         tot = []
         obj += "a" * step

         # Insertion
         print "Inserting a triple with object lenght = " + str(len(obj))
         
         # step iteration
         for iter in range(0,num_iter):
             t = Triple(URI("http://ns#a"), URI("http://ns#a"), Literal(obj))
             end = timeit.timeit(lambda: client.insert(t), number=1)
             client.remove(t)
             tot.append(end)
         
         # Median calculation
         tot.sort()
         if len(tot)%2 == 0:
             median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
         else:
             median = tot[len(tot)/2]
         median_array.append(median * 1000)
             
    # median array update
    median_arrays.append(median_array)

    # clean the sib
    client.remove(Triple(None, None, None))

for i in median_arrays:
    print "MEDIAN ARRAY: " + str(i)

bar_chart = pygal.Bar()
# chart = pygal.StackedLine(fill=True, interpolate='cubic', style=LightGreenStyle)
bar_chart.add('Real SIB (' + sibtype + ')', median_arrays[0])
bar_chart.add('Virtual SIB', median_arrays[1])
bar_chart.render_to_file(filename)


#############################################################
#
# CLEAN
#
#############################################################

print "\nCleaning the sib...\n"
kp1 = SibLib(sib_ip, sib_port)
kp1.remove(Triple(None, None, None))
kp1.leave_sib()
