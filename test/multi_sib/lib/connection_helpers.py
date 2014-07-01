#!/usr/bin/python

# requirements
import json
import random
import socket
from SSAPLib import *
from termcolor import *
import sys
import traceback
import time


######################################################
#
# manager_request
#
######################################################

def manager_request(manager_ip, manager_port, msg):

    """This method is used to send requests to the manager"""

    # Building the json request
    request = json.dumps(msg)

    # Connect to the manager
    manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        manager.connect((manager_ip, manager_port))

    except socket.error:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
        return None

    # Send the request
    try:
        manager.send(request)

    except socket.error:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to send the request to the manager'
        manager.close()
        return None

    # Receive 
    try:
        confirm_msg = manager.recv(4096)

    except socket.timeout:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Request timed out'
        manager.close()
        return None

    except socket.error:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to receive the confirm from the manager'
        manager.close()
        return None
    
    # Analyze the reply
    confirm = json.loads(confirm_msg)
    print confirm
    if confirm["return"] == "fail":
        print colored("connection_helpers> ", "red", attrs=["bold"]) + 'Registration failed!' + confirm["cause"]
        manager.close()
        return None

    elif confirm["return"] == "ok":
        manager.close()
        print colored("connection_helpers> ", "blue", attrs=["bold"]) + msg["command"] + ' request successful!'
        return confirm
