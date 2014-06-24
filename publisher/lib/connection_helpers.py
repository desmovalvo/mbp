#!/usr/bin/python

# requirements
import json
import random
import socket
from SSAPLib import *
from termcolor import *

######################################################
#
# register_request
#
######################################################

def register_request(vsib_ip, vsib_port, node_id, connected):

    # socket to the virtual sib
    vs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
     
    # connect to the virtualiser
    try:
        vs.connect((vsib_ip, vsib_port))
        print colored("connection_helpers> ", "blue", attrs=['bold']) + 'sending register request to the virtual sib'

        # building and sending the register request
        space_id = "X"
        transaction_id = random.randint(0, 1000)
        register_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(node_id,
                                                      space_id,
                                                      "REGISTER",
                                                      transaction_id, "")
        vs.send(register_msg)
        connected = True

    except socket.error:
        print colored("connection_helpers> ", "red", attrs=['bold']) + 'Unable to connect to the virtual SIB'
        print str(sys.exc_info()) + "\n" + str(traceback.print_exc())
        sys.exit()    

    # return
    return vs



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
