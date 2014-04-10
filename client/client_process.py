#requirements
import sys
import json
import random
import uuid
from termcolor import *
import socket, select, string, sys
from lib.SIBLib import SibLib
from smart_m3.m3_kp import *

ancillary_ip = '127.0.0.1'
ancillary_port = '10088'
manager_ip = '127.0.0.1'
manager_port = 17714

class AncillaryHandler:
     def __init__(self, a):
         self.a = a
         print "handle init"
     def handle(self, added, removed):
         for i in added:
             self.information = str(i[2])
             print "handle"
             print self.information
             virtual_sib_ip = self.information.split("-")[0]
             virtual_sib_port = self.information.split("-")[1]
             print virtual_sib_ip
             print virtual_sib_port
             # TODO: lancio tpublisher2 
             # close subscription
             self.a.CloseSubscribeTransaction(sub)
             print "Subscription closed!"
             
                 


#main function
if __name__ == "__main__":
    try:
        # if(len(sys.argv) < 2) :
        #     print colored("newpublisher> ", "red", attrs=["bold"]) + 'Usage : python newpublisher.py owner'
        #     sys.exit()
        
        # real sib information
        # owner = sys.argv[1]   

        # socket to the manager process
        manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        manager.settimeout(2)
         
        # connect to the manager
        try :
            manager.connect((manager_ip, manager_port))
        except :
            print colored("client_process> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
            sys.exit()        

        print colored("client_process> ", "blue", attrs=['bold']) + 'Connected to the manager. Sending discovery request!'

        # build and send discovery request to the manager
        msg = {"command":"Discovery"}
        request = json.dumps(msg)
        manager.send(request)
        
        # wait for reply from manager
        while 1:
            msg = manager.recv(4096)
            if msg:
                print colored("client_process> ", "red", attrs=["bold"]) + 'Received the following message:'
                print msg
                break

        parsed_msg = json.loads(msg)
        # if parsed_msg["return"] == "fail":
        #     print colored("newpublisher> ", "red", attrs=["bold"]) + 'Registration failed!' + confirm["cause"]
                
        # elif parsed_msg["return"] == "ok":
        #     print colored("newpublisher> ", "red", attrs=["bold"]) + 'Ready to subscribe to the ancillary sib'
        #     virtual_sib_id = confirm["virtual_sib_id"]
            
        #     # subscribe to the ancillary sib
        #     t = Triple(URI(virtual_sib_id), URI("hasPubIpPort"), None)
        #     a = SibLib('127.0.0.1', 10088)
        #     a.join_sib()
        #     sub = a.CreateSubscribeTransaction(a.ss_handle)
        #     initial_results = sub.subscribe_rdf(t, AncillaryHandler(a))
        #     if initial_results != []:
        #         for i in initial_results:
        #             print i
        #             print i[2]
        #             virtual_sib_ip = str(i[2]).split("-")[0]
        #             virtual_sib_port = int(str(i[2]).split("-")[1])
        #             print virtual_sib_ip
        #             print virtual_sib_port
                    
        #             # lancio publisher
        #             StartConnection(virtual_sib_ip, virtual_sib_port, a, sub)

                
        #         # print "Subscription closed!"
        #         # a.CloseSubscribeTransaction(sub)
                                
    except KeyboardInterrupt:
        print colored("Publisher> ", "blue", attrs=["bold"]) + "Goodbye!"
