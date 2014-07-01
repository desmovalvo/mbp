#requirements
import sys
import json
import random
import uuid
from termcolor import *
import socket, select, string, sys
from lib.SIBLib import SibLib
from smart_m3.m3_kp import *

ns = "http://smartM3Lab/Ontology.owl#"

ancillary_ip = '10.143.250.250'
ancillary_port = '10088'
manager_ip = '10.143.250.250'
manager_port = 17714


#main function
try:
    # socket to the manager process
    manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    manager.settimeout(2)
     
    # connect to the manager
    try :
        manager.connect((manager_ip, manager_port))
    except :
        print colored("client_process> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
        sys.exit()        

    if(len(sys.argv) < 2) :
        print colored("client_process> ", "blue", attrs=["bold"]) + 'Connect to the manager. Sending DiscoveryAll request: searching all reachable sibs...'

        # build and send discovery request to the manager
        msg = {"command":"DiscoveryAll"}
        request = json.dumps(msg)
        manager.send(request)


        
    elif(len(sys.argv) == 3) :
        print colored("client_process> ", "blue", attrs=["bold"]) + 'Sending DiscoveryWhere request: searching all reachable sibs with ' + str(sys.argv[1]) + ' = ' + str(sys.argv[2])
        
        # build and send discovery request to the manager
        sib_profile =  str(sys.argv[1]) + ":" + str(sys.argv[2])
        msg = {"command":"DiscoveryWhere", "sib_profile":sib_profile}
        request = json.dumps(msg)
        manager.send(request)


    # wait for reply from manager
    while 1:
        msg = manager.recv(4096)
        if msg:
            print colored("client_process> ", "blue", attrs=["bold"]) + 'Received the following message:'
            print msg
            manager.close()
            break

    # parse the reply
    parsed_msg = json.loads(msg)
    if parsed_msg["return"] == "fail":
        print colored("client_process> ", "red", attrs=["bold"]) + 'Request failed!' + parsed_msg["cause"]
            
    elif parsed_msg["return"] == "ok":
        virtual_sib_list = parsed_msg["virtual_sib_list"]
      
        print colored("client_process> ", "blue", attrs=["bold"]) + "Select a virtual sib to connect:" 

        i = 1
        vsib = []
        for vs in virtual_sib_list:
            print "[" + str(i) + "] sib_id: " + vs + " (ip: " + virtual_sib_list[vs]['ip'] + ", port: " + virtual_sib_list[vs]['port'] + ")" 
            vsib.append(vs)
            i += 1

        print colored("client_process> ", "blue", attrs=["bold"]) + "or type [0] to create new virtual multi sib."

        choice = raw_input("> ")
        if choice == "0":
            vm_sib = []
            # create new virtual multi sib
#           while 1:
#                 print colored("client_process> ", "blue", attrs=["bold"]) + "Select a sib ([0] to break): "
            print colored("client_process> ", "blue", attrs=["bold"]) + "Type the list of desired sibs, separated by commas"
            sib_list = raw_input("> ")
            sib_list = sib_list.split(",")
            for sib in sib_list:
                 if not vsib[int(sib)-1] in vm_sib:
                      vm_sib.append(vsib[int(sib)-1])

                 # if sib == "0": 
                 #      break
                 # else:
                 #      if not vsib[int(sib)-1] in vm_sib:
                 #           vm_sib.append(vsib[int(sib)-1])

            print colored("client_process> ", "blue", attrs=['bold']) + 'Sending NewVirtualMultiSIB request to the manager!'
                           
            # socket to the manager process
            manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            manager.settimeout(2)
            
            # connect to the manager
            try :
                manager.connect((manager_ip, manager_port))
            except :
                print colored("client_process> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
                sys.exit()        

            # build and send NewVirtualMultiSIB request to the manager
            msg = {'command':'NewVirtualMultiSIB','sib_list':vm_sib}
            request = json.dumps(msg)
            manager.send(request)
    
            # wait for reply from manager
            while 1:
                msg = manager.recv(4096)
                if msg:
                    print colored("client_process> ", "blue", attrs=["bold"]) + 'Received the following message:'
                    print msg
                    break

            parsed_msg = json.loads(msg)
            vmsib_id = parsed_msg["virtual_multi_sib_info"]["virtual_multi_sib_id"]
            # print vmsib_id

            # # subscribe to the ancillary sib
            # t = Triple(URI(ns + str(vmsib_id)), URI(ns + "hasKpIpPort"), None)
            # a = SibLib('127.0.0.1', 10088)
            # a.join_sib()
            # sub = a.CreateSubscribeTransaction(a.ss_handle)
            # initial_results = sub.subscribe_rdf(t, AncillaryHandler(a))
            # if initial_results != []:
            #     for i in initial_results:
            #         print i
            #         print i[2]
            #         vm_sib_ip = str(i[2]).split("-")[0].split("#")[1]
            #         vm_sib_port = int(str(i[2]).split("-")[1])
            #         print vm_sib_ip
            #         print vm_sib_port




            
        else:
            sib_id = vsib[int(choice)-1]
            sib_ip = virtual_sib_list[sib_id]["ip"]
            sib_port = virtual_sib_list[sib_id]["port"]
            print colored("client_process> ", "blue", attrs=["bold"]) + "Type the commands: "
            print "a = SibLib(" + sib_ip + ", " + sib_port + ")"
            print "a.join_sib()"
            print "to connect to the sib"


except KeyboardInterrupt:
    print colored("client_process> ", "blue", attrs=["bold"]) + "Goodbye!"
