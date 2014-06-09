import sys
import timeit
import pygal
from SIBLib import *
from termcolor import *
from smart_m3.m3_kp_api import *

# namespace
ns = "http://ns#"

# variables
step = 5
min = 0
median_arrays = []
kp_sub = []
subs = []

# define hosts data
sib_ip = sys.argv[1]
sib_port = int(sys.argv[2])
virtual_sib_ip = sys.argv[3]
virtual_sib_port = int(sys.argv[4])
num_sub = int(sys.argv[5])
num_iter = int(sys.argv[6])
max_ins = int(sys.argv[7])
sub_at = sys.argv[8]

class TestHandler:
     def __init__(self):
         #print "Kp sottoscritto"
          pass
     def handle(self, added, removed):
             #print "Ricevuta indication"
             #print
         pass


t_sub = Triple(None,None,None)

if sub_at == "virtualsib":
     for i in range(0, num_sub):
          kp_sub.append(SibLib(virtual_sib_ip, virtual_sib_port))
          subs.append(kp_sub[i].create_rdf_subscription(t_sub, TestHandler()))
elif sub_at == "realsib":
     for i in range(0, num_sub):
          kp_sub.append(SibLib(sib_ip, sib_port))
          subs.append(kp_sub[i].create_rdf_subscription(t_sub, TestHandler()))


#############################################################
#
# JOIN and CLEAN
#
#############################################################

kp = []
kp.append(SibLib(sib_ip, sib_port))
kp[0].remove(Triple(None, None, None))
kp.append(SibLib(virtual_sib_ip, virtual_sib_port))
kp[1].remove(Triple(None, None, None))

#############################################################
#
# INSERT
#
#############################################################

for client in kp:

    median_array = []
    
    # step iteration
    for i in range(0, max_ins/step):
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
            
        median_array.append(median * 1000)
    
    median_arrays.append(median_array)
    
    
for i in median_arrays:
    print "MEDIAN ARRAY: " + str(i)

print "grafico..."
bar_chart = pygal.Bar()
# chart = pygal.StackedLine(fill=True, interpolate='cubic', style=LightGreenStyle)
print "grafico..."
bar_chart.add('Local SIB', median_arrays[0])
print "grafico..."
bar_chart.add('Remote SIB', median_arrays[1])
print "grafico..."
bar_chart.render_to_file('multiple_insert.svg')
print "grafico..."

#############################################################
#
# CLEAN
#
#############################################################

print "NUM SUB: " + str(num_sub)

for i in range(0, num_sub):
    print "sub in chiusura"
    kp_sub[i].unsubscribe(subs[i])
    print "sub chiusa"


print "\nCleaning the sib...\n"
kp1 = SibLib(sib_ip, sib_port)
kp1.remove(Triple(None, None, None))
print "Triples expected at this step: 0"
print "Triples at this step: " + str(len(kp1.execute_rdf_query(Triple(None, None, None))))
kp1.leave_sib()
