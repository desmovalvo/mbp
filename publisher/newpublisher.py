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
            print colored("publisher> ", "red", attrs=["bold"]) + 'Usage : python newpublisher.py owner ancillary_ip ancillary_port manager_ip manager_port realsib_ip realsib_port'
            sys.exit()
        
        # manager sib informations
        manager_ip = sys.argv[4]
        manager_port = int(sys.argv[5])     
            
        # real sib information
        owner = sys.argv[1]
        realsib_ip = sys.argv[6]
        realsib_port = sys.argv[7]

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
        print colored("publisher> ", "blue", attrs=['bold']) + 'Connected to the manager. Sending register request!'
        register_msg = {"command":"NewRemoteSIB", "owner":owner}
        request = json.dumps(register_msg)
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
            print colored("publisher> ", "blue", attrs=["bold"]) + 'Virtual Sib Created!'
    
            virtual_sib_id = confirm["virtual_sib_info"]["virtual_sib_id"]
            virtual_sib_ip = confirm["virtual_sib_info"]["virtual_sib_ip"]
            virtual_sib_pub_port = confirm["virtual_sib_info"]["virtual_sib_pub_port"]
            
            # lancio publisher
            timer = datetime.datetime.now()
            StartConnection(virtual_sib_id, virtual_sib_ip, virtual_sib_pub_port, timer, realsib_ip, realsib_port)
                                
    except KeyboardInterrupt:
         print colored("publisher> ", "blue", attrs=["bold"]) + "Keyboard interrupt, sending " + colored("DeleteRemoteSIB", "cyan", attrs=["bold"]) + " request"
         
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
         except socket.timeout:
              print colored("publisher> ", "red", attrs=["bold"]) + 'Connection timed out'

         except:
              print colored("publisher> ", "red", attrs=["bold"]) + "Unable to send the DeleteRemoteSIB request!"

         print colored("publisher> ", "blue", attrs=["bold"]) + "Goodbye!"

