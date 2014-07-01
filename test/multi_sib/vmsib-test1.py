#!/usr/bin/python

# This test does insertions with increasing number of triples on a
# virtual multi sib and increasing the number of component SIBs.

# Arguments:
# real_sib_ip_port
# sib_list (id_sib1:id_sib2:...:id_sibN)

# requirements
import sys
import pygal
import timeit
import getopt
from termcolor import *
from lib.SIBLib import *
from lib.connection_helpers import *

# initial config
sib_list_string = None
sib_list = []
real_sib_ip = None
real_sib_port = None
manager_ip = None
manager_port = None
iterations = None
step = None
maxn = None

# read the parameters
try: 
    opts, args = getopt.getopt(sys.argv[1:], "r:l:M:s:m:i:", ["realsib=", "sib_list=", "max=", "step=", "manager=", "iterations="])
    for opt, arg in opts:
        if opt in ("-r", "--realsib"):
            real_sib_ip = arg.split(":")[0]
            real_sib_port = int(arg.split(":")[1])
        elif opt in ("-m", "--manager"):
            manager_ip = arg.split(":")[0]
            manager_port = int(arg.split(":")[1])
        elif opt in ("-s", "--step"):
            step = int(arg)
        elif opt in ("-M", "--max"):
            maxn = int(arg)
        elif opt in ("-i", "--iterations"):
            iterations = int(arg)
        elif opt in ("-l", "--sib_list"):
            sib_list_string = arg
        else:
            print "Unrecognized option " + str(opt)
            
    if not(real_sib_ip and real_sib_port and sib_list_string and maxn and step and manager_ip and manager_port and iterations):
        print 'Usage: python vmsib-test1.py -r realsib_ip:port -l idsib1:idsib2:...:idsibN -s STEP -M MAX -m manager_ip:port -i iterations'
        sys.exit()
        
except getopt.GetoptError:
    print 'Usage: python vmsib-test1.py -r realsib_ip:port -l idsib1:idsib2:...:idsibN -s STEP -M MAX -m manager_ip:port -i iterations'
    sys.exit()


# Ready to begin
kp = []
for sib in sib_list_string.split(":"):
    sib_list.append(sib)


################################################################
#                                                              #
# Test on the real SIB                                         #
#                                                              #
################################################################

# KP to the real sib
real_sib_kp = SibLib(real_sib_ip, real_sib_port)

# Clean the SIB
real_sib_kp.remove(Triple(None, None, None))

# Test on the real SIB
real_sib_results = []

# step loop
for i in range(0, maxn/step):
    
    print "Inserting " + str((i+1) * step) + " triples at once (Real SIB)"

    # iteration loop
    tot = []
    for it in range(iterations):   
    
        print '* [' + str((i+1) * step) + '] Iteration: ' + str(it)
        
        # generate (i+1)*step triples
        t = []
        for k in range(((i+1)*step)):
            tt = Triple(URI(ns + "subject" + str(it) + "_" + str(k)), URI(ns + "predicate" + str(it)), URI(ns + "object" + str(it)))
            # print tt
            t.append(tt)
    
        # insert the triples
        end = timeit.timeit(lambda: real_sib_kp.insert(t), number=1)
        tot.append(end)

        # clean sib
        real_sib_kp.remove(Triple(None, None, None))

    # sort the array and pick the median value
    tot.sort()
    if len(tot)%2 == 0:
        median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
    else:
        median = tot[len(tot)/2]             
    real_sib_results.append(round(median * 1000, 2))


################################################################
#                                                              #
# Test on the virtual multi SIB                                #
#                                                              #
################################################################

# debug print
print colored("\nRunning tests on the virtual multi SIBs...\n", "red", attrs=["bold"])

vmsib_results = []
for s in xrange(len(sib_list)):

    # Create the and connect to vmsib
    vmsib_list = []
    k = 0
    print colored("creo una multisib con " + str(s+1) + " SIBs", "red", attrs=["bold"])
    while (k < s+1):
        vmsib_list.append(sib_list[k])
        k += 1
    msg = {"command" : "NewVirtualMultiSIB", "sib_list" : sib_list}
    confirm = manager_request(manager_ip, manager_port, msg)
    vmsib_id = confirm["virtual_multi_sib_info"]["virtual_multi_sib_id"]
    vmsib_ip = confirm["virtual_multi_sib_info"]["virtual_multi_sib_ip"]
    vmsib_port = int(confirm["virtual_multi_sib_info"]["virtual_multi_sib_kp_port"])
    vmsib_kp = SibLib(vmsib_ip, vmsib_port)
    
    # create an array to store results of the virtualmulti SIB i
    vmsib_results.append([])

    # step loop
    for i in range(0, maxn/step):

        print "Inserting " + str((i+1) * step) + " triples at once (VMSIB " + str(s) + ")"
    
        # iteration loop
        tot = []
        for it in range(iterations):   
        
            print '* [' + str((i+1) * step) + '] Iteration: ' + str(it)
            
            # generate (i+1)*step triples
            t = []
            for k in range(((i+1)*step)):
                tt = Triple(URI(ns + "subject" + str(it) + "_" + str(k)), URI(ns + "predicate" + str(it)), URI(ns + "object" + str(it)))
                t.append(tt)
        
            # insert the triples
            end = timeit.timeit(lambda: vmsib_kp.insert(t), number=1)
            tot.append(end)
    
            # clean sib
            vmsib_kp.remove(Triple(None, None, None))
    
        # sort the array and pick the median value
        tot.sort()
        if len(tot)%2 == 0:
            median = (tot[len(tot)/2] + tot[len(tot)/2-1]) / 2
        else:
            median = tot[len(tot)/2]             
        vmsib_results[s].append(round(median * 1000, 2))
    
    # Delete the virtual multi SIB
    msg = {"command":"DeleteRemoteSIB", "virtual_sib_id":vmsib_id}
    manager_request(manager_ip, manager_port, msg)


# Creating the graph
print colored("Drawing the graph...", "green", attrs=["bold"])
bar_chart = pygal.Bar()
bar_chart.title = 'Insertion time increasing the number of triples'
bar_chart.add('Real SIB', real_sib_results)
for i in xrange(len(sib_list)):
    bar_chart.add('VMSIB (size' + str(i+1) + ')', vmsib_results[i])

filename_template = """test1-%s step_%s max_%s iter_%s vmsibs.svg"""
filename = filename_template % (step, maxn, iterations, len(sib_list))
bar_chart.render_to_file(filename)
