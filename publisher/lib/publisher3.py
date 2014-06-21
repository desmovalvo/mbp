# requirements
import sys
from xml_helpers import *
from lib.SSAPLib import *
from xml.etree import ElementTree as ET
import socket, select, string, sys
from termcolor import colored
import random
import uuid
import threading
import thread
from termcolor import *
from smart_m3.m3_kp import *
from xml.sax import make_parser
import time
import datetime
from message_helpers import *
import traceback
from connection_helpers import *

def StartConnection(manager_ip, manager_port, owner, vsib_id, vsib_host, vsib_port, timer, realsib_ip, realsib_port):

    # variables
    complete_ssap_msg = ''
    subscriptions = {}
    subs = {}
    node_id = vsib_id
    
    # register request
    print colored("publisher3> ", "blue", attrs=['bold']) + " Virtual sib is on " + str(vsib_host) + ":" + str(vsib_port)
    connected = False
    vs = register_request(vsib_host, vsib_port, node_id, connected)
    socket_list = [vs]

    while 1:

        # Get the list sockets which are readable
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [], 15)
      
        if not(read_sockets or write_sockets or error_sockets):
            
            print colored("publisher3> ", "red", attrs=["bold"]) + 'Disconnected from the virtual SIB. Trying reconnection...'
            
            # reconnect to remote host
            connected = False

            while not connected:
                time.sleep(5)

                # closing connection to the virtualiser
                vs.close()

                # register request
                print colored("publisher3> ", "blue", attrs=['bold']) + " Virtual sib is on " + str(vsib_host) + ":" + str(vsib_port)
                vs = register_request(vsib_host, vsib_port, node_id)
                socket_list = [vs]
                if connected:
                    break                    
   

        # MAIN LOOP
        for sock in read_sockets:

            # incoming message from the virtual sib
            if sock == vs:

                # receive the message
                ssap_msg = sock.recv(4096)

                # check if it's a ping; if so send it back to the virtualiser
                if len(ssap_msg) == 1 and ssap_msg == " ":
                    vs.send(" ")     
                    
                # if it's not a ping...
                else:
                    # put the ssap_msg at the end of the complete_ssap_msg
                    complete_ssap_msg = complete_ssap_msg + ssap_msg
                    
                    # there are no messages
                    if not ssap_msg and not complete_ssap_msg:
                        print colored("publisher3> ", "red", attrs=["bold"]) + "Connection closed by foreign host"

                        # In this case the virtualiser died, so we should look for another virtualiser
                        # We should repeat the registration process
                        cnf = manager_request(manager_ip, manager_port, "publish", owner, None, None, vsib_id)                    
                        if not cnf:
                            
                            # TODO: maybe we should set the status offline, but at the moment
                            # we clear the sib from our information
                            cnf = manager_request(manager_ip, manager_port, "delete", None, None, None, vsib_id)
                            sys.exit()

                        else:
                            # A new virtualsib now exists, so we must update the connection parameters
                            vsib_host = cnf["virtual_sib_info"]["virtual_sib_ip"]
                            vsib_port = cnf["virtual_sib_info"]["virtual_sib_pub_port"]
                            print 'new parameters are: ' + str(vsib_host) + ':' + str(vsib_port)
                            socket_list.remove(vs)
                            vs.close()
                            vs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            vs.connect((vsib_host, vsib_port))
                            socket_list.append(vs)

                            # now we have to send a register request
                            # building and sending the register request
                            space_id = "X"
                            transaction_id = random.randint(0, 1000)
                            register_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(node_id,
                                                                          space_id,
                                                                          "REGISTER",
                                                                          transaction_id, "")
                            vs.send(register_msg)

                    # there are messages
                    else:
    
                        # extract all the messages and let the remaining part into the complete_ssap_msg variable
                        messages, complete_ssap_msg = extract_complete_messages(complete_ssap_msg)

                        # for every complete message start a new thread
                        for msg in messages:
                            
                            # updating the timer
                            timer = datetime.datetime.now()
    
                            # start a new thread
                            thread.start_new_thread(handler, (sock, msg, vs, vsib_host, vsib_port, subscriptions, realsib_ip, realsib_port))                        
                        
                        # there is no complete message
                        else:
                            
                            # wait for the next message
                            pass
    


######################################################
#
# handler
#
######################################################                        

def handler(sock, ssap_msg, vs, vsib_host, vsib_port, subscriptions, realsib_ip, realsib_port):
  
    """This thread is spawned once a new message arrives"""
  
    # socket to the real SIB
    rs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rs.connect((realsib_ip, int(realsib_port)))

    # build a dict from the ssap message
    ssap_root = ET.fromstring(ssap_msg)
    ssap_msg_dict = build_dict(ssap_root)
    
    # forward the message to the real SIB
    if ssap_msg_dict["transaction_type"] != "REGISTER":

        rs.send(ssap_msg)
        print colored("publisher3> ", "blue", attrs=["bold"]) + "forwarding " + colored(ssap_msg_dict["transaction_type"] + " " + ssap_msg_dict["message_type"], "cyan", attrs=["bold"]) + " to the real sib"

        # is it a subscribe request?
        if ssap_msg_dict["transaction_type"] == "SUBSCRIBE" and ssap_msg_dict["message_type"] == "REQUEST":
            # start a new thread to handle it
            thread.start_new_thread(subscription_handler, (rs, vs, vsib_host, vsib_port, subscriptions))

        # is it an unsubscribe request?
        elif ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] == "UNSUBSCRIBE":
            rs.close()

        # is it another kind of request?
        elif ssap_msg_dict["message_type"] == "REQUEST":
                
            # start a generic handler
            thread.start_new_thread(generic_handler, (rs, vs, vsib_host, vsib_port))
        
    # register confirm
    else:
        print colored("publisher3> ", "blue", attrs=["bold"]) + "REGISTER CONFIRM received"


######################################################
#
# generic_handler
#
######################################################
                
def generic_handler(rs, vs, vsib_host, vsib_port):

    """This function handles all the message that aren't related
    to subscriptions or registration"""

    # socket to the virtual sib
    tvs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tvs.connect((vsib_host, vsib_port))

    while 1:

        # receive the confirm message
        ssap_msg = rs.recv(4096)
        
        if ssap_msg:
    
            # connect to remote host
            try :
                
                # forwarding message to the virtual sib
                print colored("publisher3> ", "blue", attrs=["bold"]) + " Received confirm message from the Real Sib, sending it to the Virtual Sib"
                tvs.send(ssap_msg)

                # closing sockets
                if "</SSAP_message>" in ssap_msg:
                    tvs.close()
                    rs.close()
                    break

            # socket error
            except socket.error:
                print colored("publisher3> ", "red", attrs=["bold"]) + "Socket error"
                print sys.exc_info() + "\n" + traceback.print_exc()

        # no message, closing sockets
        else:
            rs.close()
            tvs.close()
            break
    
    # closing thread
    print colored("publisher3> ", "red", attrs=[]) + "Closing thread generic_handler"
    return


######################################################
#
# subscription_handler
#
######################################################

def subscription_handler(rs, vs, vsib_host, vsib_port, subscriptions):

    """This function is used to handle subscriptions. This function
    receives and forwards indications (as well as [un]subscribe
    confirms) to the virtual sib"""

    # thread name
    tn = random.randint(0,1000)

    # we open a socket for each subscription
    tvs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tvs.connect((vsib_host, vsib_port))

    # wait for messages and examinate them!
    while 1:

        # receive the message
        ssap_msg = rs.recv(4096)
        if len(ssap_msg) > 1:
            
            # send the message to the virtualiser
            try:
               
                # forwarding subscription-related message to the virtual sib
                print colored("tpublisher " + str(tn) + ">", "blue", attrs=["bold"]) + " Forwarding subscription-related message to the Virtual Sib"
                tvs.send(ssap_msg)

                # if it's an unsubscribe confirm we have to close sockets
                if "<message_type>CONFIRM</message_type>" in ssap_msg and "<transaction_type>UNSUBSCRIBE</transaction_type>" in ssap_msg:
                    tvs.close()
                    rs.close()
                    break
                                
            # socket error
            except socket.error:
                print colored("tpublisher " + str(tn) + ">", "red", attrs=["bold"]) + "Socket error"
                print sys.exc_info() + "\n" + traceback.print_exc()
                break

    # close thread
    print colored("publisher3> ", "red", attrs=[]) + "Closing thread subscription_handler"
    return
