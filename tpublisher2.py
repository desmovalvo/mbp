# requirements
import sys
from xml.etree import ElementTree as ET
import socket, select, string, sys
from termcolor import colored
import random
import uuid
from lib.SSAPLib import *
import threading
import thread
from termcolor import *


def handler(sock, ssap_msg):
    print "thread>" + ssap_msg
    
    # socket to the real SIB
    rs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rs.connect((realsib_host, realsib_port))

    # forward the message to the real SIB
    if not "<transaction_type>REGISTER</transaction_type>" in ssap_msg:
        rs.send(ssap_msg)

        # receive data from the socket
        ssap_msg = rs.recv(4096)
        if ssap_msg:
            print colored("tpublisher>", "red", attrs=["bold"]) + " Received confirm message from the Real Sib"

            if not ("<transaction_type>SUBSCRIBE</transaction_type>" in ssap_msg and "<message_type>CONFIRM</message_type>" in ssap_msg):
                rs.close()
            print colored("tpublisher>", "red", attrs=["bold"]) + " Forwarding confirm message to the Virtual Sib"
            vs.send(ssap_msg)

 
#main function
if __name__ == "__main__":

    node_id = str(uuid.uuid4())

    if(len(sys.argv) < 5) :
        print 'Usage : python publisher.py vsib_hostname vsib_port realsib_hostname realsib_port'
        sys.exit()
     
    vsib_host = sys.argv[1]
    vsib_port = int(sys.argv[2])
    realsib_host = sys.argv[3]
    realsib_port = int(sys.argv[4])
     
    vs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vs.settimeout(2)
     
    # connect to remote host
    try :
        vs.connect((vsib_host, vsib_port))
    except :
        print 'Unable to connect'
        sys.exit()
     

    print 'Connected to remote host. Sending register request!'

    # building and sending the register request
    space_id = "X"
    transaction_id = random.randint(0, 1000)
    register_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(node_id,
                                                  space_id,
                                                  "REGISTER",
                                                  transaction_id, "")
    vs.send(register_msg)

    socket_list = [vs]
        
    while 1:
        # Get the list sockets which are readable
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [])
         
        for sock in read_sockets:
            #incoming message from remote server
            if sock == vs:
                ssap_msg = sock.recv(4096)
                if not ssap_msg :
                    print '\nDisconnected from chat server'
                    sys.exit()
                else :
                    #print data
                    thread.start_new_thread(handler, (sock, ssap_msg))
                
             
