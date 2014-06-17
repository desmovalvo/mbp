import sys
import timeit
import pygal
from lib.SIBLib import *
from termcolor import *
from smart_m3.m3_kp_api import *
import time

# namespace
ns = "http://ns#"

# variables
step = 100
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
none = int(sys.argv[9])

class TestHandler:
     def __init__(self):
          pass
     def handle(self, added, removed):
         pass


# create num_sub kps subscribed at the triple t_sub
if none == 1:
    t_sub = Triple(URI("http://ns#non-voglio-scattare"),URI("http://ns#mai"),None)
elif none == 2:
    t_sub = Triple(URI("http://ns#non-voglio-scattare"),None,None)
elif none == 0:
    t_sub = Triple(URI("http://ns#non-voglio-scattare"),URI("http://ns#mai"),URI("http://ns#piu"))
elif none == 3:
    if num_sub > 10:
        print "Too many subscriptions!"
        sys.exit()
    else:
        t_sub = Triple(None, None, None)
else:
    print "Invalid argument " + argv[9]

print "Creating the subscriptions..."
if sub_at == "virtualsib":
     ss = 0
     for i in range(0, num_sub):
          try:
               kp_sub.append(SibLib(virtual_sib_ip, virtual_sib_port))
               subs.append(kp_sub[i].create_rdf_subscription(t_sub, TestHandler()))
          except:
               print sys.exc_info()
               
          ss += 1
     print "Started " + str(ss) + " subscriptions"

elif sub_at == "realsib":
     ss = 0
     for i in range(0, num_sub):
          try:
               kp_sub.append(SibLib(sib_ip, sib_port))
               subs.append(kp_sub[i].create_rdf_subscription(t_sub, TestHandler()))
          except:
               print sys.exc_info()
          ss += 1
     print "Started " + str(ss) + " subscriptions"

#############################################################
#
# JOIN and CLEAN
#
#############################################################

kp = []
try:
     rskp = SibLib(sib_ip, sib_port)
     print "Created a kp for the insertions into the real sib"
     kp.append(rskp)
     lt = rskp.execute_rdf_query(Triple(None, None, None))
     for t in lt:
          rskp.remove(t)
     print "Cleaned the real sib"

     vskp = SibLib(virtual_sib_ip, virtual_sib_port)
     print "Created a kp for the insertions into the virtual sib"
     kp.append(vskp)

except:
     print sys.exc_info()
#############################################################
#
# INSERT
#
#############################################################

for client in [ rskp, vskp ]:
    
    if client == rskp:
         print "Starting the insert into the real sib..."
    else:
         print "Starting the insert into the virtual sib..."
    time.sleep(2)
    
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
                t.append(tt)
    
            # insert the triples
            try:
                 end = timeit.timeit(lambda: client.insert(t), number=1)
            except:
                 print sys.exc_info()
            
            tot.append(end)
    
            # clean sib
            try:
                 lt = client.execute_rdf_query(Triple(None, None, None))
                 for t in lt:
                      client.remove(t)
            except:
                 print sys.exc_info()
    
        # sort the array and pick the median value
        tot.sort()
        if len(tot)%2 == 0:
            median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
        else:
            median = tot[len(tot)/2]     
            
        median_array.append(round(median * 1000, 3))
    
    median_arrays.append(median_array)
    
    
for i in median_arrays:
    print "MEDIAN ARRAY: " + str(i)


bar_chart = pygal.Bar(range=(0.0, 50.0))
bar_chart.title = 'Insertion time with constant subscriptions varying the number of triples inserted'
bar_chart.add('Local SIB', median_arrays[0])
bar_chart.add('Remote SIB', median_arrays[1])
bar_chart.render_to_file('multiple_insert.svg')


#############################################################
#
# CLEAN
#
#############################################################

print "NUM SUB: " + str(num_sub)

ss = 0
for i in range(0, num_sub):
     try:
          kp_sub[i].unsubscribe(subs[i])
     except:
          print sys.exc_info()
     ss += 1
    
print str(ss) + " subscriptions closed"

print "\nCleaning the sib...\n"
try:
    lt = rskp.execute_rdf_query(Triple(None, None, None))
    for t in lt:
         rskp.remove(t)
except:
     print sys.exc_info()

print "Triples expected at this step: 0"
print "Triples at this step: " + str(len(rskp.execute_rdf_query(Triple(None, None, None))))
#vskp.leave_sib()

try:
     rskp.leave_sib()
except:
     print sys.exc_info()
