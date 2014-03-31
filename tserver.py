#!/usr/bin/python

# requirements
from xml.etree import ElementTree as ET
from termcolor import *
from socket import *
import threading
import thread

KP_PORT = 10010
PUB_PORT = 10011
HOST = 'localhost'
BUFSIZ = 1024
KP_ADDR = (HOST, KP_PORT)
PUB_ADDR = (HOST, PUB_PORT)

# handler
def handler(clientsock,addr):
    while 1:
        ssap_msg = clientsock.recv(BUFSIZ)
        
        # check whether we received a blank message
        if not ssap_msg:
            break

        # try to decode the message
        try:

            # parse the ssap message
            root = ET.fromstring(ssap_msg)                    
            info = {}
            for child in root:
                if child.attrib.has_key("name"):
                    k = child.tag + "_" + str(child.attrib["name"])
                else:
                    k = child.tag
                info[k] = child.text
                
            # debug info
            print colored("tserver> ", "blue", attrs=["bold"]) + " received a " + info["transaction_type"] + " " + info["message_type"]

        except ET.ParseError:
            print colored("tserver> ", "red", attrs=["bold"]) + " ParseError"
            pass

        # clientsock.send(msg)
    clientsock.close()

# main program
if __name__=='__main__':
    
    # creating and activating the socket for the KPs
    kp_socket = socket(AF_INET, SOCK_STREAM)
    kp_socket.bind(KP_ADDR)
    kp_socket.listen(2)
    
    # creating and activating the socket for the Publishers
    pub_socket = socket(AF_INET, SOCK_STREAM)
    pub_socket.bind(PUB_ADDR)
    pub_socket.listen(2)

    while 1:
        print colored("tserver> ", "blue", attrs=["bold"]) + ' waiting for connections...'
        clientsock, addr = kp_socket.accept()
        print colored("tserver> ", "blue", attrs=["bold"]) + ' incoming connection from ...' + str(addr)
        thread.start_new_thread(handler, (clientsock, addr))
