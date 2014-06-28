#!/usr/bin/python

# requirements
import json
import random
import socket
from termcolor import *


######################################################
#
# virtualiser_request
#
######################################################

def virtualiser_request(virtualiser_ip, virtualiser_port, request):

    """This method is used to send JSON requests from the virtualiser
    to the virtualiser and to receive the reply. It returns a dict."""

    # Building the json request
    jmsg = json.dumps(request)

    # Connect to the virtualiser
    virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        virtualiser.connect((virtualiser_ip, virtualiser_port))

    except socket.error:
        # print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser'
        confirm = {"return" : "fail", "cause" : 'Unable to connect to the virtualiser'}
        return confirm

    # Send the request
    try:
        virtualiser.send(jmsg)

    except socket.error:
        # print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to send the request to the virtualiser'
        virtualiser.close()
        confirm = {"return" : "fail", "cause" : 'Unable to send the request to the virtualiser'}
        return confirm

    # Receive 
    try:
        confirm_msg = virtualiser.recv(4096)

    except socket.timeout:
        # print colored("connection_helpers> ", "red", attrs=['bold']) + 'Request timed out'
        virtualiser.close()
        confirm = {"return" : "fail", "cause" : 'Request timed out'}
        return confirm

    except socket.error:
        # print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to receive the confirm from the virtualiser'
        virtualiser.close()
        confirm = {"return" : "fail", "cause" : 'Unable to receive the confirm from the virtualiser'}
        return confirm
    
    # Analyze the reply
    virtualiser.close()
    confirm = json.loads(confirm_msg)
    return confirm
