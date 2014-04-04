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
    print colored("tpublisher> ", "blue", attrs=["bold"]) + "started a thread"

    # socket to the real SIB
    rs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rs.connect((realsib_host, realsib_port))

    # forward the message to the real SIB
    if not "<transaction_type>REGISTER</transaction_type>" in ssap_msg:
        rs.send(ssap_msg)

        if ("<transaction_type>SUBSCRIBE</transaction_type>" in ssap_msg and "<message_type>REQUEST</message_type>"):
            # start a new thread to handle it
            thread.start_new_thread(subscription_handler, (rs, vs))

        else:
            # start a generic handler
            thread.start_new_thread(generic_handler, (rs, vs))



def generic_handler(rs, vs):

    # socket to the virtual sib
    tvs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tvs.settimeout(2)
    tvs.connect((vsib_host, vsib_port))

    while 1:
        # receive the confirm message
        ssap_msg = rs.recv(4096)
        
        if ssap_msg:
            print colored("tpublisher>", "blue", attrs=["bold"]) + " Received confirm message from the Real Sib"
            print colored("tpublisher>", "blue", attrs=["bold"]) + " Forwarding confirm message to the Virtual Sib"
    
            # connect to remote host
            try :
                tvs.send(ssap_msg)
            except socket.error:
                print colored("tpublisher>", "red", attrs=["bold"]) + "Socket error"

        else:
            rs.close()
            tvs.close()
            break    


def subscription_handler(rs, vs):

    # we open a socket for each subscription
    tvs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tvs.settimeout(2)
    tvs.connect((vsib_host, vsib_port))

    # wait for messages and examinate them!
    while 1:
        ssap_msg = rs.recv(4096)
        if len(ssap_msg) > 1:
            # forwarding subscription-related message to the virtual sib
            print colored("tpublisher>", "blue", attrs=["bold"]) + " Forwarding subscription-related message to the Virtual Sib"
     
            # connect to remote host
            try :
                tvs.send(ssap_msg)

                # if "<message_type>CONFIRM</message_type>" in ssap_msg and "<transaction_type>UNSUBSCRIBE</transaction_type>":
                #     tvs.close()
                #     break

            except socket.error:
                print colored("tpublisher>", "red", attrs=["bold"]) + "Socket error"
                
            
 
#main function
if __name__ == "__main__":

    subs = {}
    node_id = str(uuid.uuid4())

    if(len(sys.argv) < 5) :
        print colored("tpublisher> ", "red", attrs=["bold"]) + 'Usage : python publisher.py vsib_hostname vsib_port realsib_hostname realsib_port'
        sys.exit()
     
    # define hosts data
    vsib_host = sys.argv[1]
    vsib_port = int(sys.argv[2])
    realsib_host = sys.argv[3]
    realsib_port = int(sys.argv[4])
     
    # socket to the virtual sib
    vs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vs.settimeout(2)
     
    # connect to remote host
    try :
        vs.connect((vsib_host, vsib_port))
    except :
        print colored("tpublisher> ", "red", attrs=['bold']) + 'Unable to connect to the virtual SIB'
        sys.exit()    

    print colored("tpublisher> ", "blue", attrs=['bold']) + 'Connected to virtual SIB. Sending register request!'

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
                    print colored("tpublisher> ", "red", attrs=["bold"]) + 'Disconnected from the virtual SIB'
                    sys.exit()
                else :
                    print colored("tpublisher>", "blue", attrs=["bold"]) + 'Starting a new thread...'
                    thread.start_new_thread(handler, (sock, ssap_msg))

                
             
