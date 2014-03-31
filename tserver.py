#!/usr/bin/python

# requirements
from xml.etree import ElementTree as ET
from lib.treplies import *
from termcolor import *
import socket, select
import threading
import thread

KP_PORT = 10010
PUB_PORT = 10011
HOST = 'localhost'
BUFSIZ = 1024
KP_ADDR = (HOST, KP_PORT)
PUB_ADDR = (HOST, PUB_PORT)
sib_list = []
kp_list = {}
confirms = {}

##############################################################
#
# handler
#
##############################################################

def handler(clientsock, addr):
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

            ### REQUESTS

            # REGISTER REQUEST
            if info["message_type"] == "REQUEST" and info["transaction_type"] == "REGISTER":
                if handle_register_request(clientsock, info):
                    # add the sib to the list
                    sib_list.append(clientsock)
                    
            # JOIN REQUEST
            elif info["message_type"] == "REQUEST" and info["transaction_type"] == "JOIN":
                confirms[info["node_id"]] = len(sib_list)
                kp_list[info["node_id"]] = clientsock
                handle_join_request(info, ssap_msg, sib_list, kp_list)

            # LEAVE REQUEST
            elif info["message_type"] == "REQUEST" and info["transaction_type"] == "LEAVE":
                confirms[info["node_id"]] = len(sib_list)
                kp_list[info["node_id"]] = clientsock
                handle_leave_request(info, ssap_msg, sib_list, kp_list)

            # INSERT REQUEST
            elif info["message_type"] == "REQUEST" and info["transaction_type"] == "INSERT":
                confirms[info["node_id"]] = len(sib_list)
                kp_list[info["node_id"]] = clientsock
                handle_insert_request(info, ssap_msg, sib_list, kp_list)

            ### CONFIRMS

            # JOIN CONFIRM
            elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "JOIN":
                handle_join_confirm(clientsock, info, ssap_msg, confirms, kp_list)

            # LEAVE CONFIRM
            elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "LEAVE":
                handle_leave_confirm(info, ssap_msg, confirms, kp_list)

            # INSERT CONFIRM
            elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "INSERT":
                handle_insert_confirm(info, ssap_msg, confirms, kp_list)

            # debug info
            print colored("tserver> ", "blue", attrs=["bold"]) + " received a " + info["transaction_type"] + " " + info["message_type"]

        except ET.ParseError:
            print colored("tserver> ", "red", attrs=["bold"]) + " ParseError"
            pass


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':
    
    # creating and activating the socket for the KPs
    kp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    kp_socket.bind(KP_ADDR)
    kp_socket.listen(2)
    
    # creating and activating the socket for the Publishers
    pub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    pub_socket.bind(PUB_ADDR)
    pub_socket.listen(2)

    # sockets
    sockets = [kp_socket, pub_socket]

    # loop
    while 1:

        print colored("tserver> ", "blue", attrs=["bold"]) + ' waiting for connections...'
        
        # select the read_sockets
        read_sockets,write_sockets,error_sockets = select.select(sockets,[],[])

        # look for a connection on both the ports
        for sock in read_sockets:
            
            # new connection
            if sock in sockets:
                clientsock, addr = sock.accept()
                print colored("tserver> ", "blue", attrs=["bold"]) + ' incoming connection from ...' + str(addr)
                thread.start_new_thread(handler, (clientsock, addr))

            # incoming data
            else:
                print colored("tserver> ", "blue", attrs=["bold"]) + ' incoming DATA'
