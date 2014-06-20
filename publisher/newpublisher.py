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
from lib.connection_helpers import *
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

        if sys.argv[6] == "publish":
            cnf = manager_request(manager_ip, manager_port, "publish", owner)
            if cnf:
                
                # setting virtual sib parameters
                virtual_sib_id = cnf["virtual_sib_info"]["virtual_sib_id"]
                virtual_sib_ip = cnf["virtual_sib_info"]["virtual_sib_ip"]
                virtual_sib_pub_port = cnf["virtual_sib_info"]["virtual_sib_pub_port"]
                print virtual_sib_id

                # starting the publisher
                timer = datetime.datetime.now()
                StartConnection(manager_ip, manager_port, owner, virtual_sib_id, virtual_sib_ip, virtual_sib_pub_port, timer, realsib_ip, realsib_port)

            else:
                sys.exit()

        elif sys.argv[6] == "register":
            cnf = manager_request(manager_ip, manager_port, "register", owner, realsib_ip, realsib_port)
            sys.exit()
                
        else:
            print colored("publisher> ", "red", attrs=["bold"]) + '"' + action + '" not valid: "action" parameter must be "register" or "publish"!'
            sys.exit()

    except KeyboardInterrupt:
        
        # CTRL-C pressed
        
        print colored("publisher> ", "blue", attrs=["bold"]) + "Keyboard interrupt, sending " + colored("DeleteRemoteSIB", "cyan", attrs=["bold"]) + " request"
        cnf = manager_request(manager_ip, manager_port, "delete", None, None, None, virtual_sib_id)
        
        print colored("publisher> ", "blue", attrs=["bold"]) + "Goodbye!"
        sys.exit()
            
