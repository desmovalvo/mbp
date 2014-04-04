#!/usr/bin/python

# requirements
from xml.etree import ElementTree as ET
from lib.treplies import *
from lib.Subreq import *
from termcolor import *
import socket, select
import threading
import logging
import thread
import time

KP_PORT = 10010
PUB_PORT = 10011
HOST = 'localhost'
BUFSIZ = 1024
KP_ADDR = (HOST, KP_PORT)
PUB_ADDR = (HOST, PUB_PORT)
sib_list = []
kp_list = {}
confirms = {}
query_results = {}
initial_results = {}
active_subscriptions = {}
val_subscriptions = []

# logging configuration
LOG_DIRECTORY = "log/"
LOG_FILE = LOG_DIRECTORY + str(time.strftime("%Y%m%d-%H%M-")) + "tserver.log"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)
logger = logging.getLogger("tserver")

##############################################################
#
# handler
#
##############################################################

def handler(clientsock, addr):
    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = clientsock.recv(BUFSIZ)
            # check whether we received a blank message
            if not ssap_msg:
                break

            print "RICEVUTO: " + ssap_msg
            complete_ssap_msg = str(complete_ssap_msg) + str(ssap_msg)

            if "</SSAP_message>" in complete_ssap_msg:
                ssap_msg = complete_ssap_msg.split("</SSAP_message>")[0] + "</SSAP_message>"
                complete_ssap_msg = complete_ssap_msg.replace(ssap_msg, "")
                
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
                logger.info("Received the following  message from " + str(addr))
                logger.info(str(complete_ssap_msg).replace("\n", ""))
                logger.info("Message identified as a %s %s"%(info["transaction_type"], info["message_type"]))
                
                # message parsed
                #print ssap_msg
    
                ### REQUESTS
    
                # REGISTER REQUEST
                if info["message_type"] == "REQUEST" and info["transaction_type"] == "REGISTER":
                    if handle_register_request(logger, clientsock, info):
                        # add the sib to the list
                        sib_list.append(clientsock)
                        
                # JOIN REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "JOIN":
                    confirms[info["node_id"]] = len(sib_list)
                    kp_list[info["node_id"]] = clientsock
                    handle_join_request(logger, info, ssap_msg, sib_list, kp_list)
    
                # LEAVE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "LEAVE":
                    confirms[info["node_id"]] = len(sib_list)
                    kp_list[info["node_id"]] = clientsock
                    handle_leave_request(logger, info, ssap_msg, sib_list, kp_list)
    
                # INSERT REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "INSERT":
                    confirms[info["node_id"]] = len(sib_list)
                    kp_list[info["node_id"]] = clientsock
                    handle_insert_request(logger, info, ssap_msg, sib_list, kp_list)
    
                # REMOVE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "REMOVE":
                    confirms[info["node_id"]] = len(sib_list)
                    kp_list[info["node_id"]] = clientsock
                    handle_remove_request(logger, info, ssap_msg, sib_list, kp_list)
    
                # SPARQL QUERY REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "QUERY" and info["parameter_type"] == "sparql":
                    confirms[info["node_id"]] = len(sib_list)
                    query_results[info["node_id"]] = []
                    kp_list[info["node_id"]] = clientsock
                    handle_sparql_query_request(logger, info, ssap_msg, sib_list, kp_list)
    
                # RDF QUERY REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "QUERY" and info["parameter_type"] == "RDF-M3":
                    confirms[info["node_id"]] = len(sib_list)
                    query_results[info["node_id"]] = []
                    kp_list[info["node_id"]] = clientsock
                    handle_rdf_query_request(logger, info, ssap_msg, sib_list, kp_list)
    
                # RDF SUBSCRIBE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "SUBSCRIBE" and info["parameter_type"] == "RDF-M3":
    
                    confirms[info["node_id"]] = len(sib_list)
                    initial_results[info["node_id"]] = []
                    kp_list[info["node_id"]] = clientsock
                    handle_rdf_subscribe_request(logger, info, ssap_msg, sib_list, kp_list, clientsock, val_subscriptions)
    
                # RDF UNSUBSCRIBE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "UNSUBSCRIBE":
                    handle_rdf_unsubscribe_request(logger, info, ssap_msg, sib_list, kp_list, clientsock, val_subscriptions)
        
    
                ### CONFIRMS
    
                # JOIN CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "JOIN":
                    handle_join_confirm(logger, clientsock, info, ssap_msg, confirms, kp_list)
    
                # LEAVE CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "LEAVE":
                    handle_leave_confirm(logger, info, ssap_msg, confirms, kp_list)
    
                # INSERT CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "INSERT":
                    handle_insert_confirm(logger, info, ssap_msg, confirms, kp_list)
    
                # REMOVE CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "REMOVE":
                    handle_remove_confirm(logger, info, ssap_msg, confirms, kp_list)
    
                # SPARQL QUERY CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "QUERY" and "sparql" in ssap_msg:
                    handle_sparql_query_confirm(logger, info, ssap_msg, confirms, kp_list, query_results)
    
                # RDF QUERY CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "QUERY" and not "sparql" in ssap_msg:
                    handle_rdf_query_confirm(logger, info, ssap_msg, confirms, kp_list, query_results)
    
                # RDF SUBSCRIBE CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "SUBSCRIBE": # and not "sparql" in ssap_msg
                    handle_rdf_subscribe_confirm(logger, info, ssap_msg, confirms, kp_list, initial_results, active_subscriptions, clientsock, val_subscriptions)
    
                # RDF UNSUBSCRIBE CONFIRM
                elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "UNSUBSCRIBE": # and not "sparql" in ssap_msg
                    handle_rdf_unsubscribe_confirm(logger, info, ssap_msg, confirms, kp_list, initial_results, active_subscriptions, clientsock, val_subscriptions)
    
                ### INDICATIONS
                    
                # SUBSCRIBE INDICATION
                elif info["message_type"] == "INDICATION" and info["transaction_type"] == "SUBSCRIBE": 
                    handle_subscribe_indication(logger, ssap_msg, info, clientsock, val_subscriptions)
    
    
            except ET.ParseError:
                print colored("tserver> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except socket.error:
            print colored("tserver> ", "red", attrs=["bold"]) + " socket.error: break!"
#            break

    
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
    logger.info('Server waiting for KPs on port ' + str(KP_PORT))
    
    # creating and activating the socket for the Publishers
    pub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    pub_socket.bind(PUB_ADDR)
    pub_socket.listen(2)
    logger.info('Server waiting for publishers on port ' + str(PUB_PORT))

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
                logger.info('Incoming connection from ' + str(addr))
                thread.start_new_thread(handler, (clientsock, addr))

            # incoming data
            else:
                print colored("tserver> ", "blue", attrs=["bold"]) + ' incoming DATA'
