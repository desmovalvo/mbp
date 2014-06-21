#!/usr/bin/python

#requirements
import sys
import json
import time
import uuid
import random
import datetime
from termcolor import *
from smart_m3.m3_kp import *
from lib.publisher3 import *
from lib.SIBLib import SibLib
import socket, select, string, sys

#main function
if __name__ == "__main__":
    try:
        if(len(sys.argv) < 7) :
            print colored("publisher> ", "red", attrs=["bold"]) + 'Usage : python newpublisher.py owner manager_ip manager_port realsib_ip realsib_port action'
            sys.exit()
        
        # manager sib informations
        manager_ip = sys.argv[2]
        manager_port = int(sys.argv[3])     
            
        # real sib information
        owner = sys.argv[1]
        realsib_ip = sys.argv[4]
        realsib_port = sys.argv[5]
        
        # action to perform
        action = sys.argv[6]

        # socket to the manager process
        manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        manager.settimeout(15)
         
        # connect to the manager
        try:
             manager.connect((manager_ip, manager_port))
        except :
             print colored("publisher> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
             sys.exit()        

        # build and send the request message 
        if action == "register":
            print colored("publisher> ", "blue", attrs=['bold']) + 'Connected to the manager. Sending RegisterPublicSIB request!'
            register_msg = {"command":"RegisterPublicSIB", "owner":owner, "ip":realsib_ip, "port":realsib_port}
            request = json.dumps(register_msg)
        elif action == "publish":
            print colored("publisher> ", "blue", attrs=['bold']) + 'Connected to the manager. Sending NewRemoteSIB request!'
            register_msg = {"command":"NewRemoteSIB", "owner":owner}
            request = json.dumps(register_msg)
        else:
            print colored("publisher> ", "red", attrs=["bold"]) + '"' + action + '" not valid: "action" parameter must be "register" or "publish"!'
            sys.exit()
 
        try:
             manager.send(request)
             timer = datetime.datetime.now()
        except:
             print colored("publisher> ", "red", attrs=["bold"]) + 'Registration failed! Try again!'  

        # wait for a reply
        while 1:
             if (datetime.datetime.now() - timer).total_seconds() > 15:
                  print colored("publisher> ", "red", attrs=["bold"]) + 'No reply received. Try again!'
                  sys.exit(0)

             try:
                  confirm_msg = manager.recv(4096)
             except socket.timeout:
                  print colored("publisher> ", "red", attrs=["bold"]) + "Request timed out"
                  sys.exit(0)

             if confirm_msg:
                  print colored("publisher> ", "blue", attrs=["bold"]) + 'Received the following message:'
                  print confirm_msg
                  break

        confirm = json.loads(confirm_msg)
        if confirm["return"] == "fail":
            print colored("publisher> ", "red", attrs=["bold"]) + 'Registration failed!' + confirm["cause"]
            sys.exit(0)

        elif confirm["return"] == "ok":
            manager.close()
            if action == "register":
                print colored("publisher> ", "blue", attrs=["bold"]) + 'Sib registered!'
                sib_id = confirm["sib_id"]
            else:
                print colored("publisher> ", "blue", attrs=["bold"]) + 'Virtual Sib created!'
                virtual_sib_id = confirm["virtual_sib_info"]["virtual_sib_id"]
                virtual_sib_ip = confirm["virtual_sib_info"]["virtual_sib_ip"]
                virtual_sib_pub_port = confirm["virtual_sib_info"]["virtual_sib_pub_port"]
            
                # lancio publisher
                timer = datetime.datetime.now()
                StartConnection(virtual_sib_id, virtual_sib_ip, virtual_sib_pub_port, timer, realsib_ip, realsib_port)
                                
    except KeyboardInterrupt:

         print colored("publisher> ", "blue", attrs=["bold"]) + "Keyboard interrupt, sending " + colored("SetSIBStatus", "cyan", attrs=["bold"]) + " request"
         
         # build json message
         manager.close()
         manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
         # if action == "register":
         #     delete_msg = {"command":"DeleteSIB", "sib_id":sib_id}         
         if action == "publish":
             set_status_msg = {"command":"SetSIBStatus", "sib_id":virtual_sib_id, "status":"offline"}         

         request = json.dumps(set_status_msg)
         
         print "prima del try"
         # send message
         try:
              manager.connect((manager_ip, manager_port))
              manager.send(request)
              confirm_msg = manager.recv(4096)
              if confirm_msg:
                   print colored("publisher> ", "blue", attrs=["bold"]) + 'Received the following message:'
                   print confirm_msg
         except socket.timeout:
              print colored("publisher> ", "red", attrs=["bold"]) + 'Connection timed out'

         except:
              print colored("publisher> ", "red", attrs=["bold"]) + "Unable to send the SetStatus request!"


         
        
         print colored("publisher> ", "blue", attrs=["bold"]) + "Keyboard interrupt, sending " + colored("DeleteRemoteSIB", "cyan", attrs=["bold"]) + " request"
         
         # build json message
         manager.close()
         manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
         # if action == "register":
         #     delete_msg = {"command":"DeleteSIB", "sib_id":sib_id}         
         if action == "publish":
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
         except socket.timeout:
              print colored("publisher> ", "red", attrs=["bold"]) + 'Connection timed out'

         except:
              print colored("publisher> ", "red", attrs=["bold"]) + "Unable to send the Delete request!"

         print colored("publisher> ", "blue", attrs=["bold"]) + "Goodbye!"

