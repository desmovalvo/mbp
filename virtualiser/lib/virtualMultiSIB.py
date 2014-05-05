#!/usr/bin/python

# requirements
from xml.etree import ElementTree as ET
from treplies import *
from Subreq import *
from termcolor import *
import socket, select
import threading
import logging
import thread
import time
from SIBLib import *

BUFSIZ = 1024

kp_list = {}
confirms = {}
query_results = {}
initial_results = {}
active_subscriptions = {}
val_subscriptions = []
sibs_info = {}
sib = {}

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

            # check whether we received a blank message
            if not ssap_msg and not complete_ssap_msg:
                break

            if ssap_msg != None:
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
                print colored("virtualMultiSIB> ", "blue", attrs=["bold"]) + " received a " + info["transaction_type"] + " " + info["message_type"]
                logger.info("Received the following  message from " + str(addr))
                logger.info(str(complete_ssap_msg).replace("\n", ""))
                logger.info("Message identified as a %s %s"%(info["transaction_type"], info["message_type"]))
                    
                ### REQUESTS
                
                # sib_list_conn = {}
                
                # # create the sockets for the sibs
                # for s in sibs_info:
                #     ip = str(sibs_info[s]["ip"].split("#")[1])
                #     kp_port = sibs_info[s]["kp_port"]
                #     # socket to the sib
                #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #     sib_list_conn[s] = sock

                
                # JOIN REQUEST: completed
                if info["message_type"] == "REQUEST" and info["transaction_type"] == "JOIN":
                    confirms[info["node_id"]] = len(sibs_info)
                    kp_list[info["node_id"]] = clientsock
                    handle_join_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]])

    
                # LEAVE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "LEAVE":
                    confirms[info["node_id"]] = len(sibs_info)
                    kp_list[info["node_id"]] = clientsock
                    handle_leave_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]])
    
                # INSERT REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "INSERT":
                    confirms[info["node_id"]] = len(sibs_info)
                    kp_list[info["node_id"]] = clientsock
                    handle_insert_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]])
    
                # REMOVE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "REMOVE":
                    confirms[info["node_id"]] = len(sibs_info)
                    kp_list[info["node_id"]] = clientsock
                    handle_remove_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]])
    
                # SPARQL QUERY REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "QUERY" and info["parameter_type"] == "sparql":
                    confirms[info["node_id"]] = len(sibs_info)
                    query_results[info["node_id"]] = []
                    kp_list[info["node_id"]] = clientsock
                    handle_sparql_query_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]], query_results)
    
                # RDF QUERY REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "QUERY" and info["parameter_type"] == "RDF-M3":
                    confirms[info["node_id"]] = len(sibs_info)
                    query_results[info["node_id"]] = []
                    kp_list[info["node_id"]] = clientsock
                    handle_rdf_query_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]], query_results)
    
                # RDF SUBSCRIBE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "SUBSCRIBE" and info["parameter_type"] == "RDF-M3":
    
                    confirms[info["node_id"]] = len(sibs_info)
                    initial_results[info["node_id"]] = []
                    kp_list[info["node_id"]] = clientsock
                    handle_rdf_subscribe_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]], clientsock, val_subscriptions, active_subscriptions, initial_results)
    
                # RDF UNSUBSCRIBE REQUEST
                elif info["message_type"] == "REQUEST" and info["transaction_type"] == "UNSUBSCRIBE":
                    confirms[info["node_id"]] = len(sibs_info)
                    handle_rdf_unsubscribe_request(logger, info, ssap_msg, sibs_info, kp_list, confirms[info["node_id"]], clientsock, val_subscriptions)
                
                
            except ET.ParseError:
                print colored("virtualMultiSIB> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except socket.error:
            print colored("virtualMultiSIB> ", "red", attrs=["bold"]) + " socket.error: break!"
#            break


def virtualMultiSIB(virtualiser_ip, kp_port, pub_port, virtual_multi_sib_id, check_var, sib_list, ancillary_ip, ancillary_port):

    print colored("virtualMultiSIB> ", "blue", attrs=["bold"]) + ' started a new virtual multi SIB with ip ' + str(virtualiser_ip) + ", kpPort " + str(kp_port) + ", pubPort " + str(pub_port) + " and id " + str(virtual_multi_sib_id)

    host = virtualiser_ip
    kp_addr = (host, kp_port)
    pub_addr = (host, pub_port)
    
    print "kp addr: " + str(kp_addr)
    print "pub addr: " + str(pub_addr)

    sib["virtual_multi_sib_id"] = virtual_multi_sib_id

    # creating and activating the socket for the KPs
    kp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    kp_socket.bind(kp_addr)
    kp_socket.listen(2)
    logger.info('Virtual Multi SIB waiting for KPs on port ' + str(kp_port))
    
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

        print colored("virtualMultiSIB> ", "blue", attrs=["bold"]) + ' waiting for connections...'
        
        # select the read_sockets
        read_sockets,write_sockets,error_sockets = select.select(sockets,[],[])
        
        # look for a connection on both the ports
        for sock in read_sockets:
            
            # new connection
            if sock in sockets:
                clientsock, addr = sock.accept()
                print colored("virtualMultiSIB> ", "blue", attrs=["bold"]) + ' incoming connection from ...' + str(addr)
                logger.info('Incoming connection from ' + str(addr))
                thread.start_new_thread(handler, (clientsock, addr, kp_port, sibs_info))

            # incoming data
            else:
                print colored("virtualMultiSIB> ", "blue", attrs=["bold"]) + ' incoming DATA'
