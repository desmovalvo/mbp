#!/usr/bin/python

#requirements
import sys
import json
import time
import uuid
import random
import getopt
import logging
import datetime
from termcolor import *
from smart_m3.m3_kp import *
from lib.SIBLib import SibLib
from lib.publisher_lib import *
from lib.output_helpers import *
import socket, select, string, sys
from lib.connection_helpers import *

# main function
if __name__ == "__main__":

    # initial setup
    manager_ip = None
    manager_port = None
    realsib_ip = None
    realsib_port = None
    owner = None
    action = None
    config_file = "publisher.conf"
    
    # read the parameters
    try: 
        opts, args = getopt.getopt(sys.argv[1:], "m:o:a:s:c:", ["manager=", "owner=", "action=", "sib=", "config="])
        for opt, arg in opts:
            if opt in ("-m", "--manager"):
                manager_ip = arg.split(":")[0]
                manager_port = int(arg.split(":")[1])
            elif opt in ("-s", "--sib"):
                realsib_ip = arg.split(":")[0]
                realsib_port = int(arg.split(":")[1])
            elif opt in ("-o", "--owner"):
                owner = arg
            elif opt in ("-c", "--config"):
                config_file = arg
            elif opt in ("-a", "--action"):
                action = arg
                if action not in ["publish", "register"]:
                    print publisher_print(False) + "the only valid actions are 'publish' and 'register'"
                    sys.exit()
            else:
                print publisher_print(False) + "unrecognized option " + str(opt)
        
        if not(manager_ip) and not(manager_port) and not(realsib_ip) and not(realsib_port) and not(owner) and not(action):
            print publisher_print(False) + 'Usage: python publisher.py -m manager_ip:port -o owner -s realsib_ip:port -a action'
            sys.exit()
        
    except getopt.GetoptError:
        print publisher_print(False) + 'Usage: python publisher.py -m manager_ip:port -o owner -s realsib_ip:port -a action'
        sys.exit()

    # now we can begin!

    # read the configuration file
    config_file_stream = open(config_file, "r")
    conf = json.load(config_file_stream)
    config_file_stream.close()

    # configure the logger
    log_enabled = conf["log"]    
    log_level = conf["log_level"]
    log_directory = conf["directory"]
    log_file = log_directory + str(time.strftime("%Y%m%d-%H%M-")) + "publisher.log"
    logging.basicConfig(filename=log_file, level=log_level)
    logger = logging.getLogger("publisher")

    # check if the real SIB is really online
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((realsib_ip, realsib_port))
        if log_enabled:
            logger.info(" connected to the real SIB on port " + realsib_ip + ":" + str(realsib_port))
        s.close()
    except socket.error:
        print  publisher_print(False) + 'connection to the real SIB failed'
        if log_enabled:
            logger.info(" connection to the real SIB on port " + realsib_ip + ":" + str(realsib_port) + " failed!")
        sys.exit()
    
    # real SIB is online, we can proceed
    try:
        check = []
        check.append(False)

        # performing requested action
        if action == "publish":

            msg = {"command":"NewRemoteSIB", "sib_id":"none", "owner":owner}
            cnf = manager_request(manager_ip, manager_port, msg)

            if cnf:
                
                # setting virtual sib parameters
                virtual_sib_id = cnf["virtual_sib_info"]["virtual_sib_id"]
                virtual_sib_ip = cnf["virtual_sib_info"]["virtual_sib_ip"]
                virtual_sib_pub_port = cnf["virtual_sib_info"]["virtual_sib_pub_port"]

                # log
                if log_enabled:
                    logger.info(" virtual SIB started on " + virtual_sib_ip + ":" + str(virtual_sib_pub_port))

                # starting the publisher
                timer = datetime.datetime.now()
                StartConnection(manager_ip, manager_port, owner, 
                                virtual_sib_id, virtual_sib_ip, virtual_sib_pub_port, 
                                timer, realsib_ip, realsib_port, check, logger)

            else:
                sys.exit()

        elif action == "register":

            msg = {"command":"RegisterPublicSIB", "owner":owner, "ip":realsib_ip, "port":str(realsib_port)}
            cnf = manager_request(manager_ip, manager_port, msg)

            if cnf:

                # reading confirm 
                sib_id = cnf["sib_id"]
                
                # log
                if log_enabled:
                    logger.info(" SIB registered")

            print publisher_print(True), 
            raw_input("Press Enter to undo...")

            # Sending DeleteSIB request
            print publisher_print(True) + "Keyboard interrupt, sending " + command_print("DeleteSIB") + " request:",
            msg = {"command":"DeleteSIB", "sib_id":sib_id}
            cnf = manager_request(manager_ip, manager_port, msg)
            
            # log
            if log_enabled:
                logger.info(" SIB unregistered")

            sys.exit()
                
        else:
            print publisher_print(False) + '"' + action + '" not valid: "action" parameter must be "register" or "publish"!'
            sys.exit()


    # CTRL-C pressed
    except KeyboardInterrupt: 

        if action == "publish":

            # Sending DeleteRemoteSIB request
            print publisher_print(True) + "Keyboard interrupt, sending " + command_print("DeleteRemoteSIB") + " request:",
            msg = {"command":"DeleteRemoteSIB", "virtual_sib_id":virtual_sib_id}
            cnf = manager_request(manager_ip, manager_port, msg)

            # log
            if log_enabled:
                logger.info(" SIB unregistered")

        elif action == "register":

            # Sending DeleteSIB request
            print publisher_print(True) + "Keyboard interrupt, sending " + command_print("DeleteSIB") + " request:",
            msg = {"command":"DeleteSIB", "sib_id":sib_id}
            cnf = manager_request(manager_ip, manager_port, msg)

            # log
            if log_enabled:
                logger.info(" SIB unregistered")
        
        # Exiting
        print publisher_print(True) + "Goodbye!"
        sys.exit()
            
