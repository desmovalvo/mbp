#!/usr/bin/python

import json
import socket
from termcolor import *

def manager_request(manager_ip, manager_port, request, owner, realsib_ip = None, realsib_port = None, vsib_id = None):

    # This method is used to make a sib public OR to register a public sib and make it discoverable OR
    # to delete a remote SIB
    # The following step are performed:
    # 0) send a NewRemoteSIB or a Register or a DeleteRemoteSIB request to the manager
    # 1) wait for a reply
    # 2) if positive return virtualiser connection data, otherwise return None

    # Building the json request
    if request == "publish":
        if vsib_id:
            msg = {"command" : "NewRemoteSIB", "owner" : owner, "sib_id" : vsib_id}
        else:
            msg = {"command" : "NewRemoteSIB", "owner" : owner, "sib_id" : "none"}
    elif request == "register":
        msg = {"command" : "RegisterPublicSIB", "owner" : owner, "ip" : realsib_ip, "port" : realsib_port}
    elif request == "delete":
        msg = {"command" : "DeleteRemoteSIB", "virtual_sib_id" : vsib_id }
    request = json.dumps(msg)

    # Connect to the manager
    manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        manager.connect((manager_ip, manager_port))

    except socket.error:
        print colored("publisher> ", "red", attrs=['bold']) + 'Unable to connect to the manager'
        return None

    # Send the NewRemoteSib request
    try:
        manager.send(request)

    except socket.error:
        print colored("publisher> ", "red", attrs=['bold']) + 'Unable to send the request to the manager'
        manager.close()
        return None

    # Receive 
    try:
        confirm_msg = manager.recv(4096)

    except socket.timeout:
        print colored("publisher> ", "red", attrs=['bold']) + 'Request timed out'
        manager.close()
        return None

    except socket.error:
        print colored("publisher> ", "red", attrs=['bold']) + 'Unable to receive the confirm from the manager'
        manager.close()
        return None
    
    # Analyze the reply
    confirm = json.loads(confirm_msg)
    if confirm["return"] == "fail":
        print colored("publisher> ", "red", attrs=["bold"]) + 'Registration failed!' + confirm["cause"]
        manager.close()
        return None

    elif confirm["return"] == "ok":
        manager.close()
        print colored("connection_helpers> ", "blue", attrs=["bold"]) + 'request successful!'
        return confirm
