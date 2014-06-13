#!/usr/bin/python

# requirements
import time
import thread
import logging
import threading
from SIBLib import *
from Subreq import *
import socket, select
from treplies import *
from termcolor import *
from output_helpers import *
from xml_helpers import *
from xml.etree import ElementTree as ET

BUFSIZ = 1024

kp_list = {}
confirms = {}
query_results = {}
initial_results = {}
active_subscriptions = {}
val_subscriptions = []
sibs_info = {}
sib = {}
sib_ping = {}

# logging configuration
LOG_DIRECTORY = "log/"
LOG_FILE = LOG_DIRECTORY + str(time.strftime("%Y%m%d-%H%M-")) + "virtualMultiSIB.log"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)
logger = logging.getLogger("virtualMultiSIB")
ns = "http://smartM3Lab/Ontology.owl#"

##############################################################
#
# handler
#
##############################################################

def handler(clientsock, addr, port, sibs_info):
    
    # storing received parameters in thread-local variables
    kp_port = port

    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = clientsock.recv(BUFSIZ)

            # it may be a "space" character from a subscribed kp or from a publisher
            if len(ssap_msg) == 1 and ssap_msg == " ":
                # received a ping from publisher: update his timer
                if sib_ping["socket"] != None:
                    #print colored("remoteSIB> ", "blue", attrs=["bold"]) + str(clientsock) + " is alive "                    
                    sib_ping["timer"] = datetime.datetime.now()
            elif len(ssap_msg) == 0:
                break
      
            else:
    
                if ssap_msg != None:
                    complete_ssap_msg = str(complete_ssap_msg) + str(ssap_msg)

                # check whether we received a blank message
                if not ssap_msg and not complete_ssap_msg:
                    break

                else:
                    #extract all the messages and let the remaining part into the complete_ssap_msg variable
                    messages, complete_ssap_msg = extract_complete_messages(complete_ssap_msg)

                for ssap_msg in messages:
                    # try to decode the message
                    try:
                            
                        # parse the ssap message
                        # root = ET.fromstring(ssap_msg)           
                        # info = {}
                        # for child in root:
                        #     if child.attrib.has_key("name"):
                        #         k = child.tag + "_" + str(child.attrib["name"])
                        #     else:
                        #         k = child.tag
                        #     info[k] = child.text
                        ssap_root = ET.fromstring(ssap_msg)
                        ssap_msg_dict = build_dict(ssap_root)
            
                        # debug info
                        print vmsib_print(True) + " received a " + ssap_msg_dict["transaction_type"] + " " + ssap_msg_dict["message_type"]
                        logger.info("Received the following  message from " + str(addr))
                        logger.info(str(complete_ssap_msg).replace("\n", ""))
                        logger.info("Message identified as a %s %s"%(ssap_msg_dict["transaction_type"], ssap_msg_dict["message_type"]))
                            
                        ### REQUESTS
                        
                        # JOIN/LEAVE/REMOVE/INSERT REQUEST
                        if ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] in ["JOIN", "LEAVE", "REMOVE", "INSERT"]:
                            
                            # how many confirms should we wait? 
                            confirms[ssap_msg_dict["node_id"]] = len(sibs_info)
        
                            # store the client socket from which we received the request
                            kp_list[ssap_msg_dict["node_id"]] = clientsock
                            
                            # call the method that handles the request and wait for confirms
                            handle_generic_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, confirms[ssap_msg_dict["node_id"]])
            
                        # SPARQL/RDF QUERY REQUEST
                        elif ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] == "QUERY":
        
                            # how many confirms should we wait? 
                            confirms[ssap_msg_dict["node_id"]] = len(sibs_info)
        
                            # creation of an empty array in which we'll insert the query results
                            query_results[ssap_msg_dict["node_id"]] = []
        
                            # store the client socket from which we received the request
                            kp_list[ssap_msg_dict["node_id"]] = clientsock
        
                            # call the method that handles the request and wait for confirms
                            handle_query_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, confirms[ssap_msg_dict["node_id"]], query_results)
            
                        # RDF and SPARQL SUBSCRIBE REQUEST
                        elif ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] == "SUBSCRIBE":# and ssap_msg_dict["parameter_type"] == "RDF-M3":
            
                            # how many confirms should we wait? 
                            confirms[ssap_msg_dict["node_id"]] = len(sibs_info)
        
                            # creation of an empty array in which we'll insert the initial results
                            initial_results[ssap_msg_dict["node_id"]] = []
        
                            # store the client socket from which we received the request
                            kp_list[ssap_msg_dict["node_id"]] = clientsock
        
                            # call the method that handles the request and wait for confirms
                            handle_rdf_subscribe_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, confirms[ssap_msg_dict["node_id"]], clientsock, val_subscriptions, active_subscriptions, initial_results)
            
                        # RDF UNSUBSCRIBE REQUEST
                        elif ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] == "UNSUBSCRIBE":
        
                            # how many confirms should we wait? 
                            confirms[ssap_msg_dict["node_id"]] = len(sibs_info)
        
                            # call the method that handles the request and wait for confirms
                            handle_rdf_unsubscribe_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, confirms[ssap_msg_dict["node_id"]], clientsock, val_subscriptions)
                        
                    except ET.ParseError:
                        print vmsib_print(False) + " ParseError"
                        pass
        
        except socket.error:
            print vmsib_print(False) + " socket.error: break!"
#            break


##########################################################################
#
# virtualMultiSIB:
#
# this is called as a process by newVirtualMultiSIB function located in
# request_handlers.py
#
##########################################################################

def virtualMultiSIB(virtualiser_ip, kp_port, virtual_multi_sib_id, check_var, sib_list, ancillary_ip, ancillary_port):

    # debug print
    print vmsib_print(True) + ' started a new virtual multi SIB with ip ' + str(virtualiser_ip) + ", kpPort " + str(kp_port) + " and id " + str(virtual_multi_sib_id)

    # storing received arguments into variables
    host = virtualiser_ip
    kp_addr = (host, kp_port)
    sib["virtual_multi_sib_id"] = virtual_multi_sib_id
    ancillary_ip = ancillary_ip
    ancillary_port = ancillary_port

    # creating and activating the socket for the KPs
    kp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    kp_socket.bind(kp_addr)
    kp_socket.listen(10)
    logger.info('Virtual Multi SIB waiting for KPs on ' + str(host) + ":" + str(kp_port))
    
    # retrieving information about the real SIBs from the ancillary SIB
    a = SibLib(ancillary_ip, ancillary_port)
    for s in sib_list:
        sibs_info[s] = {}
        t = Triple(URI(ns + str(s)), URI(ns + "hasKpIpPort"), None)
        result = a.execute_rdf_query(t)
        sibs_info[s]["ip"] = str(result[0][2]).split("-")[0]
        sibs_info[s]["kp_port"] = int(str(result[0][2]).split("-")[1])
        
    # sockets
    sockets = [kp_socket]

    # loop
    while check_var:

        # debug print
        print vmsib_print(True) + ' waiting for connections...'
        
        # select the read_sockets
        read_sockets,write_sockets,error_sockets = select.select(sockets,[],[])
        
        # look for a connection on both the ports
        for sock in read_sockets:
            
            # new connection
            if sock in sockets:
                clientsock, addr = sock.accept()
                print vmsib_print(True) + ' incoming connection from ...' + str(addr)
                logger.info('Incoming connection from ' + str(addr))
                thread.start_new_thread(handler, (clientsock, addr, kp_port, sibs_info))

            # incoming data
            else:
                print vmsib_print(True) + ' incoming DATA'
