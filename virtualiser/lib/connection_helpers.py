#!/usr/bin/python

# requirements
import json
import random
import socket
from SSAPLib import *
from termcolor import *


######################################################
#
# manager_request
#
######################################################

def manager_request(manager_ip, manager_port, request):

    """This method is used to send JSON requests from the virtualiser
    to the manager and to receive the reply. It returns a dict."""

    # Building the json request
    jmsg = json.dumps(request)

    # Connect to the manager
    manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        manager.connect((manager_ip, manager_port))

    except socket.error:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
        confirm = {"return" : "fail", "cause" : 'Unable to connect to the manager'}
        return confirm

    # Send the request
    try:
        manager.send(jmsg)

    except socket.error:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to send the request to the manager'
        manager.close()
        confirm = {"return" : "fail", "cause" : 'Unable to send the request to the manager'}
        return confirm

    # Receive 
    try:
        confirm_msg = manager.recv(4096)

    except socket.timeout:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Request timed out'
        manager.close()
        confirm = {"return" : "fail", "cause" : 'Request timed out'}
        return confirm

    except socket.error:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to receive the confirm from the manager'
        manager.close()
        confirm = {"return" : "fail", "cause" : 'Unable to receive the confirm from the manager'}
        return confirm
    
    # Analyze the reply
    manager.close()
    confirm = json.loads(confirm_msg)
    return confirm
