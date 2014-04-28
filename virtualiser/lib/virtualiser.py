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

BUFSIZ = 1024

sib = {}

# TODO: Cancellare
sib_list = []
sib_socket = None
sib_list_timers = {}

kp_list = {}
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

            # it may be a "space" character
            if len(ssap_msg) == 1:
                if ssap_msg == " ":
                    if sib["socket"] != None:
                        print colored("tserver> ", "blue", attrs=["bold"]) + str(clientsock) + " is alive "                    
                        sib["timer"] = datetime.datetime.now()

            else:
    
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
                    print colored("tserver> ", "blue", attrs=["bold"]) + " received a " + info["transaction_type"] + " " + info["message_type"]
                    logger.info("Received the following  message from " + str(addr))
                    logger.info(str(complete_ssap_msg).replace("\n", ""))
                    logger.info("Message identified as a %s %s"%(info["transaction_type"], info["message_type"]))
                        
                    ### REQUESTS
        
                    # REGISTER REQUEST
                    if info["message_type"] == "REQUEST" and info["transaction_type"] == "REGISTER":

                        # build a reply message
                        reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                               info["space_id"],
                                                               "REGISTER",
                                                               info["transaction_id"],
                                                               '<parameter name="status">m3:Success</parameter>')
    
                        # try to receive, then return
                        try:
                            clientsock.send(reply)

                            # add the sib to the list
                            sib["socket"] = clientsock
                            
                            print colored("treplies>", "green", attrs=["bold"]) + " handle_register_request"
                            logger.info("REGISTER REQUEST handled by handle_register_request")
                            
                            # setting the timestamp
                            sib["timer"] = datetime.datetime.now()
                        
                            thread.start_new_thread(socket_observer, (sib,))                            
                            print colored("treplies> ", "blue", attrs=["bold"]) + "Socket observer started for socket " + str(sib["socket"])
                
                        except socket.error:
                            logger.error("REGISTER CONFIRM not sent!")
                    


                    # RDF/SPARQL SUBSCRIBE REQUEST
                    elif info["message_type"] == "REQUEST" and info["transaction_type"] == "SUBSCRIBE":
                        kp_list[info["node_id"]] = clientsock
                        
                        # debug info
                        print colored("treplies>", "green", attrs=["bold"]) + " request handled"
                        logger.info("SUBSCRIBE REQUEST handled")

                        # generating a Subreq instance
                        newsub = Subreq(clientsock, info["node_id"], info["transaction_id"])
 
                        # forwarding message to the publisher
                        try:
                            sib["socket"].send(ssap_msg)
                            val_subscriptions.append(newsub)

                        except socket.error:
                            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                             info["space_id"],
                                                             "SUBSCRIBE",
                                                             info["transaction_id"],
                                                             '<parameter name="status">m3:Error</parameter>')
                            newsub.conn.send(err_msg)
                            del nuwsub
                            
                            logger.error("SUBSCRIBE REQUEST forwarding failed")

 
                    # RDF/SPARQL SUBSCRIBE CONFIRM
                    elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "SUBSCRIBE": 
                        # debug info
                        print colored("treplies>", "green", attrs=["bold"]) + " confirm handled"
                        logger.info("SUBSCRIBE CONFIRM handled")
                        
                        # store the corrispondence between the real sib and the real_subscription_id
                        for s in val_subscriptions:                              
                            if s.node_id == info["node_id"] and s.request_transaction_id == info["transaction_id"]:                            
                                s.subscription_id = info["parameter_subscription_id"]
                                s.conn.send(ssap_msg)
                                break


                    # RDF/SPARQL UNSUBSCRIBE REQUEST
                    elif info["message_type"] == "REQUEST" and info["transaction_type"] == "UNSUBSCRIBE":
                        # debug info
                        print colored("treplies>", "green", attrs=["bold"]) + " request handled"
                        logger.info("UNSUBSCRIBE REQUEST handled")

                        # find the Subreq instance
                        for s in val_subscriptions:
                            if str(s.subscription_id) == str(info["parameter_subscription_id"]):

                                # forwarding message to the publishers
                                
                                try:
                                    # send the message
                                    sib["socket"].send(ssap_msg)                
                                except socket.error:
                                    err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                             info["space_id"],
                                                                             "UNSUBSCRIBE",
                                                                             info["transaction_id"],
                                                                             '<parameter name="status">m3:Error</parameter>')
                                    s.conn.send(err_msg)
                                        
                                    logger.error("RDF UNSUBSCRIBE REQUEST forwarding failed")

                                break
                                

 


                    # RDF/SPARQL UNSUBSCRIBE CONFIRM
                    elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "UNSUBSCRIBE": # and not "sparql" in ssap_msg
                        # debug info
                        print colored("treplies>", "green", attrs=["bold"]) + " confirm handled"
                        logger.info("UNSUBSCRIBE CONFIRM handled")


                        for s in val_subscriptions:
                            if str(s.subscription_id) == str(info["parameter_subscription_id"]):
                                s.conn.send(ssap_msg)
                                                                        
                                # destroy the class instance
                                del s


                    # INDICATIONS
                        
                    # RDF/SPARQL SUBSCRIBE INDICATION
                    elif info["message_type"] == "INDICATION" and info["transaction_type"] == "SUBSCRIBE": 
                        # debug info
                        print colored("treplies>", "green", attrs=["bold"]) + " indication handled"
                        logger.info("SUBSCRIBE INDICATION handled")

                        for s in val_subscriptions:
                            if str(s.subscription_id) == str(info["parameter_subscription_id"]):

                                # send the message to the kp
                                print "Inoltro la indication"
                                try:
                                    s.conn.send(ssap_msg)
                                except socket.error:
                                    print "inoltro indication fallito"
                                
                                break
                
                            
                    ### OTHER REQUESTS
                    elif info["message_type"] == "REQUEST":
                        kp_list[info["node_id"]] = clientsock

                        # debug message
                        print colored("treplies>", "green", attrs=["bold"]) + " request handled"
                        logger.info(info["transaction_type"] + " REQUEST handled")

                        # forwarding message to the publisher
                        try:
                            sib["socket"].send(ssap_msg)
                        except socket.error:
                            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                     info["space_id"],
                                                                     info["transaction_type"],
                                                                     info["transaction_id"],
                                                                     '<parameter name="status">m3:Error</parameter>')

                            # send a notification error to the KP
                            kp_list[info["node_id"]].send(err_msg)
                            del kp_list[info["node_id"]]
                            logger.error(info["transaction_type"] + " REQUEST forwarding failed")


                    ### OTHER CONFIRMS
        
                    elif info["message_type"] == "CONFIRM":

                        # debug info
                        print colored("treplies>", "green", attrs=["bold"]) + " confirm handled"
                        logger.info(info["transaction_type"] + " CONFIRM handled")
                    
                        # forward message to the kp
                        kp_list[info["node_id"]].send(ssap_msg)
                        kp_list[info["node_id"]].close()                        

        
        
                except ET.ParseError:
                    print colored("tserver> ", "red", attrs=["bold"]) + " ParseError"
                    pass
    
        except socket.error:
            print colored("tserver> ", "red", attrs=["bold"]) + " socket.error: break!"
            break


def virtualiser(kp_port, pub_port, virtual_sib_id):

    host = "localhost"
    kp_addr = (host, kp_port)
    pub_addr = (host, pub_port)
    
    print "kp addr: " + str(kp_addr)
    print "pub addr: " + str(pub_addr)

    sib["virtual_sib_id"] = virtual_sib_id

    # creating and activating the socket for the KPs
    kp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    kp_socket.bind(kp_addr)
    kp_socket.listen(2)
    logger.info('Server waiting for KPs on port ' + str(kp_port))
    
    # creating and activating the socket for the Publishers
    pub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    pub_socket.bind(pub_addr)
    pub_socket.listen(2)
    logger.info('Server waiting for publishers on port ' + str(pub_port))

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