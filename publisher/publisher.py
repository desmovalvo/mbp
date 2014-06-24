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
from lib.output_helpers import *
import socket, select, string, sys
from lib.connection_helpers import *


# main function
if __name__ == "__main__":
    try:
        check = []
        check.append(False)
        
        if(len(sys.argv) < 5) :
            print publisher_print(False) + 'Usage : python publisher.py owner manager_ip:port realsib_ip:port action'
            sys.exit()
        
        # manager sib informations
        manager_ip = sys.argv[2].split(":")[0]
        manager_port = int(sys.argv[2].split(":")[1])     
            
        # real sib information
        owner = sys.argv[1]
        realsib_ip = sys.argv[3].split(":")[0]
        realsib_port = sys.argv[3].split(":")[1]
        
        # action to perform
        action = sys.argv[4]

        # performing requested action
        if sys.argv[4] == "publish":

            msg = {"command":"NewRemoteSIB", "sib_id":"none", "owner":owner}
            cnf = manager_request(manager_ip, manager_port, msg)

            if cnf:
                
                # setting virtual sib parameters
                virtual_sib_id = cnf["virtual_sib_info"]["virtual_sib_id"]
                virtual_sib_ip = cnf["virtual_sib_info"]["virtual_sib_ip"]
                virtual_sib_pub_port = cnf["virtual_sib_info"]["virtual_sib_pub_port"]

                # starting the publisher
                timer = datetime.datetime.now()
                StartConnection(manager_ip, manager_port, owner, virtual_sib_id, virtual_sib_ip, virtual_sib_pub_port, timer, realsib_ip, realsib_port, check)

            else:
                sys.exit()

        elif sys.argv[4] == "register":

            msg = {"command":"RegisterPublicSIB", "owner":owner, "ip":realsib_ip, "port":str(realsib_port)}
            cnf = manager_request(manager_ip, manager_port, msg)

            if cnf:

                # reading confirm 
                sib_id = cnf["sib_id"]

            print publisher_print(True), 
            raw_input("Press Enter to undo...")

            # Sending DeleteSIB request
            print publisher_print(True) + "Keyboard interrupt, sending " + command_print("DeleteSIB") + " request:",
            msg = {"command":"DeleteSIB", "sib_id":sib_id}
            cnf = manager_request(manager_ip, manager_port, msg)


            sys.exit()
                
        else:
            print publisher_print(False) + '"' + action + '" not valid: "action" parameter must be "register" or "publish"!'
            sys.exit()


    # CTRL-C pressed
    except KeyboardInterrupt: 

        if sys.argv[4] == "publish":

            # Sending DeleteRemoteSIB request
            print publisher_print(True) + "Keyboard interrupt, sending " + command_print("DeleteRemoteSIB") + " request:",
            msg = {"command":"DeleteRemoteSIB", "virtual_sib_id":virtual_sib_id}
            cnf = manager_request(manager_ip, manager_port, msg)

        elif sys.argv[4] == "register":

            # Sending DeleteSIB request
            print publisher_print(True) + "Keyboard interrupt, sending " + command_print("DeleteSIB") + " request:",
            msg = {"command":"DeleteSIB", "sib_id":sib_id}
            cnf = manager_request(manager_ip, manager_port, msg)

        
        # Exiting
        print publisher_print(True) + "Goodbye!"
        sys.exit()
            
