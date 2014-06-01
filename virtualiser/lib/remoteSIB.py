#!/usr/bin/python

# requirements
from xml.etree import ElementTree as ET
from remoteSIB import *
from Subreq import *
from termcolor import *
import socket, select
import threading
import logging
import random
import thread
import time
from xml.sax import make_parser
from SIBLib import *
import time
import datetime
from SSAPLib import *


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
LOG_FILE = LOG_DIRECTORY + str(time.strftime("%Y%m%d-%H%M-")) + "remoteSIB.log"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)
logger = logging.getLogger("remoteSIB")

##############################################################
#
# handler
#
##############################################################

def handler(clientsock, addr, port, ancillary_ip, ancillary_port):

    # storing received parameters in thread-local variables
    kp_port = port

    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = clientsock.recv(BUFSIZ)

            # it may be a "space" character from a subscribed kp or from a publisher
            if len(ssap_msg) == 1:
                if ssap_msg == " ":
                    # received a ping from publisher: update his timer
                    if sib["socket"] != None:
                        #print colored("remoteSIB> ", "blue", attrs=["bold"]) + str(clientsock) + " is alive "                    
                        sib["timer"] = datetime.datetime.now()
            elif len(ssap_msg) == 0:
                break
      
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
                    print "BEGIN" + ssap_msg + "END"
                    root = ET.fromstring(ssap_msg)           
                    info = {}
                    for child in root:
                        if child.attrib.has_key("name"):
                            k = child.tag + "_" + str(child.attrib["name"])
                        else:
                            k = child.tag
                        info[k] = child.text
        
                    # debug info
                    print colored("remoteSIB> ", "blue", attrs=["bold"]) + " received a " + info["transaction_type"] + " " + info["message_type"]
                    logger.info("Received the following  message from " + str(addr))
                    logger.info(str(complete_ssap_msg).replace("\n", ""))
                    logger.info("Message identified as a %s %s"%(info["transaction_type"], info["message_type"]))
                        
                    ### REQUESTS
        
                    # REGISTER REQUEST
                    if info["message_type"] == "REQUEST" and info["transaction_type"] == "REGISTER":
                        
                        # set the status online
                        a = SibLib(ancillary_ip, ancillary_port)
                        t = []
                        t.append(Triple(URI(ns + str(info["node_id"])), URI(ns + "hasStatus"), URI(ns + "offline")))
                        a.remove(t)
                        t = []
                        t.append(Triple(URI(ns + str(info["node_id"])), URI(ns + "hasStatus"), URI(ns + "online")))
                        a.insert(t)       
                        t = []


                        # build a reply message
                        reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                               info["space_id"],
                                                               "REGISTER",
                                                               info["transaction_id"],
                                                               '<parameter name="status">m3:Success</parameter>')

                        # try to send, then return
                        try:
                            clientsock.send(reply)

                            # add the sib to the list
                            sib["socket"] = clientsock
                            #print "ACTUAL SIB: " + str(sib)
                            
                            print colored("remoteSIB>", "green", attrs=["bold"]) + " handle_register_request"
                            logger.info("REGISTER REQUEST handled by handle_register_request")
                            
                            # setting the timestamp
                            sib["timer"] = datetime.datetime.now()
                            
                            # New register request received:
                            # set check_var to False to kill the actual socket observer
                            check_var = False
                            time.sleep(1)
                            # Then set it to True and start the new socket observer
                            check_var = True
                            
                            # TODO kp_port non serve passarlo: il socket observer non lo usa!!
                            thread.start_new_thread(socket_observer, (sib, kp_port, check_var, ancillary_ip, ancillary_port))                            
                            print colored("treplies> ", "blue", attrs=["bold"]) + "Socket observer started for socket " + str(sib["socket"])

                
                        except socket.error:
                            logger.error("REGISTER CONFIRM not sent!")
                    
          

                    # RDF/SPARQL SUBSCRIBE REQUEST
                    elif info["message_type"] == "REQUEST" and info["transaction_type"] == "SUBSCRIBE":
                        # kp_list[info["node_id"]] = {}
                        # kp_list[info["node_id"]]["socket"] = clientsock
                        # kp_list[info["node_id"]]["timer"] = datetime.datetime.now()

                        # debug info
                        print colored("remoteSIB>", "green", attrs=["bold"]) + " request handled"
                        logger.info("SUBSCRIBE REQUEST handled")

                        # generating a Subreq instance
                        newsub = Subreq(clientsock, info)#, info["node_id"], info["transaction_id"])
                        
                        # forwarding message to the publisher
                        try:
                            sib["socket"].send(ssap_msg)
                            val_subscriptions.append(newsub)
                            # kp_check_var = True
                            # TODO: e' meglio far partire il seguente thread quando riceviamo la conferma di sottoscrizione? In caso inizia a pingare prima di mandare la conferma e da' problemi...
                            # thread.start_new_thread(kp_observer, (newsub, sib, ancillary_ip, ancillary_port))#, kp_list[info["node_id"]], kp_check_var)) 
                            
                            print colored("remoteSIB> ", "blue", attrs=["bold"]) + "Subscribed kp observer started for socket " + str(newsub.conn)


                        except socket.error:
                            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                             info["space_id"],
                                                             "SUBSCRIBE",
                                                             info["transaction_id"],
                                                             '<parameter name="status">m3:Error</parameter>')
                            newsub.conn.send(err_msg)
                            del newsub
                            
                            logger.error("SUBSCRIBE REQUEST forwarding failed")

 
                    # RDF/SPARQL SUBSCRIBE CONFIRM
                    elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "SUBSCRIBE": 
                        # debug info
                        print colored("remoteSIB>", "green", attrs=["bold"]) + " confirm handled"
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
                        print colored("remoteSIB>", "green", attrs=["bold"]) + " request handled"
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
                        print colored("remoteSIB>", "green", attrs=["bold"]) + " confirm handled"
                        logger.info("UNSUBSCRIBE CONFIRM handled")


                        for s in val_subscriptions:
                            if str(s.subscription_id) == str(info["parameter_subscription_id"]):
                                
                                try:
                                    s.conn.send(ssap_msg)
                                except socket.error:
                                    pass
                                                                        
                                # destroy the class instance
                                del s


                    # INDICATIONS
                        
                    # RDF/SPARQL SUBSCRIBE INDICATION
                    elif info["message_type"] == "INDICATION" and info["transaction_type"] == "SUBSCRIBE": 
                        # debug info
                        print colored("remoteSIB>", "green", attrs=["bold"]) + " indication handled"
                        logger.info("SUBSCRIBE INDICATION handled")

                        for s in val_subscriptions:
                            if str(s.subscription_id) == str(info["parameter_subscription_id"]):

                                # send the message to the kp
                                try:
                                    s.conn.send(ssap_msg)
                                except socket.error:
                                    print colored("remoteSIB>", "red", attrs=["bold"]) + " indication send failed"
                                break
                
                            
                    ### OTHER REQUESTS
                    elif info["message_type"] == "REQUEST":
                        kp_list[info["node_id"]] = clientsock

                        # debug message
                        print colored("remoteSIB>", "green", attrs=["bold"]) + " request handled"
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
                        print colored("remoteSIB>", "green", attrs=["bold"]) + " confirm handled"
                        logger.info(info["transaction_type"] + " CONFIRM handled")
                    
                        # forward message to the kp
                        kp_list[info["node_id"]].send(ssap_msg)
                        kp_list[info["node_id"]].close()                        

        
        
                except ZeroDivisionError: #ET.ParseError:
                    print colored("remoteSIB> ", "red", attrs=["bold"]) + " ParseError"
                    pass
    
        except socket.error:
            print colored("remoteSIB> ", "red", attrs=["bold"]) + " socket.error: break!"
            break


# SOCKET OBSERVER THREAD
def socket_observer(sib, port, check_var, ancillary_ip, ancillary_port):
    
    #print "obs id: " + str(uuid.uuid4())
    key = sib["socket"]

    while check_var:
        try:            
            if (datetime.datetime.now() - sib["timer"]).total_seconds() > 15:
                print colored("remoteSIB> ", "red", attrs=["bold"]) + " socket " + str(sib["socket"]) + " dead"
                
                # set the status offline
                a = SibLib(ancillary_ip, ancillary_port)

                t = [Triple(URI(ns + str(sib["virtual_sib_id"])), URI(ns + "hasStatus"), None)]
                result = a.execute_rdf_query(t)

                if len(result) > 0:
                    t = []
                    t.append(Triple(URI(ns + str(sib["virtual_sib_id"])), URI(ns + "hasStatus"), URI(ns + str(result[0][2]).split("#")[1])))
                    a.remove(t)

                    t = []
                    t.append(Triple(URI(ns + str(sib["virtual_sib_id"])), URI(ns + "hasStatus"), URI(ns + "offline")))
                    a.insert(t)       

                sib["socket"] = None                
                break
            else:
                time.sleep(5)
                #print colored("socket_observer> ", "blue", attrs=["bold"]) + " check if socket " + str(sib["socket"]) + " is alive"
                sib["socket"].send(" ")
                
        except IOError:
            print 'ioerror'
            pass

        # except KeyError:
        #     print 'keyerror'
        #     break

        # except:
        #     print 'boh'
        
    print colored("socket_observer> ", "red", attrs=["bold"]) + " closed observer thread for socket " + str(key)



# # SUBSCRIBED KP OBSERVER THREAD
# def kp_observer(newsub, sib, ancillary_ip, ancillary_port):#, kp, kp_check_var):
    
#     #print "kp obs id: " + str(uuid.uuid4())
    
#     while True:# kp_check_var:
        
#         time.sleep(5)
#         #print colored("socket_observer> ", "blue", attrs=["bold"]) + " check if socket " + str(sib["socket"]) + " is alive"
#         try:
#             while True:
#                 r, w, e = select.select([newsub.conn], [], [], 0)      # more data waiting?
#                 print "select: r=%s w=%s e=%s" % (r,w,e)        # debug output to command line
#                 if r:                                           # yes, data avail to read.
#                     t = newsub.conn.recv(1024, socket.MSG_PEEK)        # read without remove from queue
#                     print "peek: len=%d, data=%s" % (len(t),t)  # debug output
#                     if len(t) > 0:                               # length of data peeked 0?
#                         print "Client disconnected."            # client disconnected
#                         break                                   # quit program
#                 time.sleep(5)    
#                 newsub.conn.send(" ")                           # echo only if still connected

#             # #kp["socket"].send(" ")
#             # newsub.conn.send(" ")
#             # t = newsub.conn.recv(1024, socket.MSG_PEEK)        # read without remove from queue
#             # print "peek: len=" + str(len(t)) + ", data=" + str(t)  # debug output
#             # if len(t)==0:                               # length of data peeked 0?
#             #     print colored ("remoteSIB> ", "red", attrs=["bold"]) + "Subscribed kp disconnected. Sending an unsubscribe request"            # client disconnected
#             #     # build and send an unsubscribe request to the publisher
#             #     ssap_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(newsub.node_id,
#             #                                               newsub.space_id,
#             #                                               "UNSUBSCRIBE",
#             #                                               newsub.request_transaction_id,
#             #                                               '<parameter name="status">m3:Error</parameter>',
#             #                                               '<parameter name = "subscription_id">' + str(newsub.subscription_id) + '</parameter>')
                
            # TODO: build and send an unsubscribe request to the publisher

#             ssap_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(newsub.node_id,
#                                                       newsub.space_id,
#                                                       "UNSUBSCRIBE",
#                                                       newsub.request_transaction_id,
#                                                       '<parameter name="status">m3:Error</parameter>',
#                                                       '<parameter name = "subscription_id">' + newsub.subscription_id + '</parameter>')
# #             #     sib["socket"].send(ssap_msg)

#         except socket.error:
#             print colored("remoteSIB> ", "red", attrs=["bold"]) + " socket " + str(newsub.conn) + " dead"
                
#             # build and send an unsubscribe request to the publisher

#             ssap_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(newsub.node_id,
#                                                       newsub.space_id,
#                                                       "UNSUBSCRIBE",
#                                                       newsub.request_transaction_id,
#                                                       '<parameter name="status">m3:Error</parameter>',
#                                                       '<parameter name = "subscription_id">' + newsub.subscription_id + '</parameter>')
                
#             sib["socket"].send(ssap_msg)
            
#             # set kp["socket"] to None
#             #kp["socket"] = None
            
#             # exit while
#             # kp_check_var = False
#             break
        
#     print colored("kp_observer> ", "red", attrs=["bold"]) + " closed observer thread for socket " + str(newsub.conn)




def remoteSIB(virtualiser_ip, kp_port, pub_port, virtual_sib_id, check_var, ancillary_ip, ancillary_port):

    print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' started a new remote SIB with ip ' + str(virtualiser_ip) + ", kpPort " + str(kp_port) + ", pubPort " + str(pub_port) + " and id " + str(virtual_sib_id)

    host = virtualiser_ip
    kp_addr = (host, kp_port)
    pub_addr = (host, pub_port)

    print "kp addr: " + str(kp_addr)
    print "pub addr: " + str(pub_addr)

    sib["virtual_sib_id"] = virtual_sib_id

    # creating and activating the socket for the KPs
    kp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    kp_socket.bind(kp_addr)
    kp_socket.listen(2)
    logger.info('Remote SIB waiting for KPs on port ' + str(kp_port))
    
    # creating and activating the socket for the Publishers
    pub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    pub_socket.bind(pub_addr)
    pub_socket.listen(2)
    logger.info('Remote SIB waiting for publishers on port ' + str(pub_port))

    # sockets
    sockets = [kp_socket, pub_socket]

    # loop
    while check_var:

        print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' waiting for connections...'
        
        # select the read_sockets
        read_sockets,write_sockets,error_sockets = select.select(sockets,[],[])
        
        # look for a connection on both the ports
        for sock in read_sockets:
            
            # new connection
            if sock in sockets:
                clientsock, addr = sock.accept()
                print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' incoming connection from ...' + str(addr)
                logger.info('Incoming connection from ' + str(addr))
                thread.start_new_thread(handler, (clientsock, addr, kp_port, ancillary_ip, ancillary_port))

            # incoming data
            else:
                print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' incoming DATA'
