#requirements
import sys
import json
import random
import uuid
from termcolor import *
import socket, select, string, sys
from lib.SIBLib import SibLib
from smart_m3.m3_kp import *
from lib.publisher3 import *
import time
import datetime

ns = "http://smartM3Lab/Ontology.owl#"

ancillary_ip = "localhost"
ancillary_port = '10088'
manager_ip = "localhost"
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
        if(len(sys.argv) < 6) :
            print colored("publisher> ", "red", attrs=["bold"]) + 'Usage : python newpublisher.py owner ancillary_ip ancillary_port manager_ip manager_port realsib_port'
            sys.exit()
        
        # ancillary sib informations
        ancillary_ip = sys.argv[2]
        ancillary_port = sys.argv[3]
        manager_ip = sys.argv[4]
        manager_port = int(sys.argv[5])     
            
        # real sib information
        owner = sys.argv[1]
        realsib_port = sys.argv[6]

        # socket to the manager process
        manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        manager.settimeout(2)
         
        # connect to the manager
        try:
             manager.connect((manager_ip, manager_port))
        except :
             print colored("publisher> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
             sys.exit()        

        print colored("publisher> ", "blue", attrs=['bold']) + 'Connected to the manager. Sending register request!'

        # build and send the request message 
        register_msg = {"command":"NewRemoteSIB", "owner":owner}
        request = json.dumps(register_msg)
        try:
             manager.send(request)
             timer = datetime.datetime.now()
        except:
             print colored("publisher> ", "red", attrs=["bold"]) + 'Registration failed! Try again!'  

        
        while 1:
             if (datetime.datetime.now() - timer).total_seconds() > 15:
                  print colored("publisher> ", "red", attrs=["bold"]) + 'No reply received. Try again!'
                  sys.exit(0)

             confirm_msg = manager.recv(4096)
             if confirm_msg:
                  print colored("publisher> ", "blue", attrs=["bold"]) + 'Received the following message:'
                  print confirm_msg
                  break

        confirm = json.loads(confirm_msg)
        if confirm["return"] == "fail":
            print colored("publisher> ", "red", attrs=["bold"]) + 'Registration failed!' + confirm["cause"]
            sys.exit(0)

        elif confirm["return"] == "ok":
            print colored("publisher> ", "blue", attrs=["bold"]) + 'Virtual Sib Created!'
    
            virtual_sib_id = confirm["virtual_sib_info"]["virtual_sib_id"]
            virtual_sib_ip = confirm["virtual_sib_info"]["virtual_sib_ip"]
            virtual_sib_pub_port = confirm["virtual_sib_info"]["virtual_sib_pub_port"]
            
            # lancio publisher
            timer = datetime.datetime.now()
            StartConnection(virtual_sib_id, virtual_sib_ip, virtual_sib_pub_port, timer, realsib_port)

############################################################
###
### Il seguente pezzo serve se dobbiamo fare la 
### sottoscrizione all'ancillary sib per 
### conoscere le info relative alla virtual sib 
### creata. Per ora Le riceviamo direttamente dal manager!
###
############################################################
                
        # elif confirm["return"] == "ok":
        #     print colored("publisher> ", "red", attrs=["bold"]) + 'Ready to subscribe to the ancillary sib'
        #     virtual_sib_id = confirm["virtual_sib_id"]
            
        #     # subscribe to the ancillary sib
        #     t = Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIpPort"), None)
        #     a = SibLib('127.0.0.1', 10088)
        #     a.join_sib()
        #     sub = a.CreateSubscribeTransaction(a.ss_handle)
        #     initial_results = sub.subscribe_rdf(t, AncillaryHandler(a))
        #     if initial_results != []:
        #         for i in initial_results:
        #             print i
        #             print i[2]
        #             virtual_sib_ip = str(i[2]).split("-")[0].split("#")[1]
        #             virtual_sib_port = int(str(i[2]).split("-")[1])
        #             print virtual_sib_ip
        #             print virtual_sib_port
                    
        #             # lancio publisher
        #             StartConnection(virtual_sib_ip, virtual_sib_port, a, sub)

                
        #         # print "Subscription closed!"
        #         # a.CloseSubscribeTransaction(sub)

############################################################################################
############################################################################################

                                
    except KeyboardInterrupt:
         print colored("publisher> ", "blue", attrs=["bold"]) + "Detected keyboard interrupt, sending DeleteRemoteSIB request"         
         
         # build json message
         manager.close()
         manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
         delete_msg = {"command":"DeleteRemoteSIB", "virtual_sib_id":virtual_sib_id}         
         request = json.dumps(delete_msg)
         
         # send message
         try:
              manager.connect((manager_ip, manager_port))
              manager.send(request)
              confirm_msg = manager.recv(4096)
              if confirm_msg:
                   print colored("publisher> ", "blue", attrs=["bold"]) + 'Received the following message:'
                   print confirm_msg

         except:
              print colored("publisher> ", "red", attrs=["bold"]) + "Unable to send the DeleteRemoteSIB request!"

         print colored("publisher> ", "blue", attrs=["bold"]) + "Goodbye!"

