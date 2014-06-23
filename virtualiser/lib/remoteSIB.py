#!/usr/bin/python

# requirements
from connection_helpers import *
from xml.etree import ElementTree as ET
from remoteSIB import *
from Subreq import *
import traceback
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
from message_helpers import *
from xml_helpers import *
import sys
import json


BUFSIZ = 1024

sib = {}
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

def handler(clientsock, addr, port, ancillary_ip, ancillary_port, manager_ip, manager_port):

    # storing received parameters in thread-local variables
    kp_port = port

    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = clientsock.recv(BUFSIZ)

            # it may be a "space" character from a subscribed kp or from a publisher
            if len(ssap_msg) == 1 and ssap_msg == " ":

                # received a ping from publisher: update his timer
                if sib["socket"] != None:
                    sib["timer"] = datetime.datetime.now()

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
                        ssap_root = ET.fromstring(ssap_msg)
                        ssap_msg_dict = build_dict(ssap_root)
                            
                        ### REQUESTS
            
                        # REGISTER REQUEST
                        if ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] == "REGISTER":

                            # set the status online
                            msg = { "command" : "SetSIBStatus", "sib_id" : str(sib["virtual_sib_id"]) , "status" : "online" }
                            manager_request(manager_ip, manager_port, msg)
    
                            # build a reply message
                            reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                   ssap_msg_dict["space_id"],
                                                                   "REGISTER",
                                                                   ssap_msg_dict["transaction_id"],
                                                                   '<parameter name="status">m3:Success</parameter>')
    
                            # try to send, then return
                            try:
                                clientsock.send(reply)
    
                                # add the sib to the list
                                sib["socket"] = clientsock
                                
                                # setting the timestamp
                                sib["timer"] = datetime.datetime.now()
                                
                                # New register request received:

                                # set check_var to False to kill the actual socket observer
                                check_var = False
                                time.sleep(1)

                                # Then set it to True and start the new socket observer
                                check_var = True
                                
                                # TODO kp_port non serve passarlo: il socket observer non lo usa!!
                                thread.start_new_thread(socket_observer, (sib, kp_port, check_var, ancillary_ip, ancillary_port, manager_ip, manager_port))                            
                                print colored("treplies> ", "blue", attrs=["bold"]) + "Socket observer started for socket " + str(sib["socket"])
                    
                            except socket.error:
                                logger.error("REGISTER CONFIRM not sent!")
                                print sys.exc_info()
                        
              
                        # RDF/SPARQL SUBSCRIBE REQUEST
                        elif ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] == "SUBSCRIBE":
    
                            # generating a Subreq instance
                            newsub = Subreq(clientsock, ssap_msg_dict)#, ssap_msg_dict["node_id"], ssap_msg_dict["transaction_id"])
                            
                            # forwarding message to the publisher
                            try:
                                sib["socket"].send(ssap_msg)
                                val_subscriptions.append(newsub)

                            except AttributeError:
                                print "AttributeError - closing socket"
                                clientsock.close()
    
                            except socket.error:
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                 ssap_msg_dict["space_id"],
                                                                 "SUBSCRIBE",
                                                                 ssap_msg_dict["transaction_id"],
                                                                 '<parameter name="status">m3:Error</parameter>')
                                newsub.conn.send(err_msg)
                                del newsub
                                
                                logger.error("SUBSCRIBE REQUEST forwarding failed")
    
     
                        # RDF/SPARQL SUBSCRIBE CONFIRM
                        elif ssap_msg_dict["message_type"] == "CONFIRM" and ssap_msg_dict["transaction_type"] == "SUBSCRIBE": 
    
                            sib["timer"] = datetime.datetime.now()
                            
                            # store the corrispondence between the real sib and the real_subscription_id
                            for s in val_subscriptions:                              
                                if s.node_id == ssap_msg_dict["node_id"] and s.request_transaction_id == ssap_msg_dict["transaction_id"]:   

                                    # save the subscription id
                                    s.subscription_id = ssap_msg_dict["subscription_id"]

                                    # send the message
                                    s.conn.send(ssap_msg)
                                    
                                    # start a thread to ping
                                    thread.start_new_thread(subscription_observer, (sib, s))                            
                                                                       
                                    break
    
    
                        # RDF/SPARQL UNSUBSCRIBE REQUEST
                        elif ssap_msg_dict["message_type"] == "REQUEST" and ssap_msg_dict["transaction_type"] == "UNSUBSCRIBE":
    
                            # find the Subreq instance
                            for s in val_subscriptions:
                                if str(s.subscription_id) == str(ssap_msg_dict["subscription_id"]):
    
                                    # forwarding message to the publishers
                                    
                                    try:
                                        # send the message
                                        sib["socket"].send(ssap_msg)                

                                    except socket.error:
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                                 ssap_msg_dict["space_id"],
                                                                                 "UNSUBSCRIBE",
                                                                                 ssap_msg_dict["transaction_id"],
                                                                                 '<parameter name="status">m3:Error</parameter>')
                                        s.conn.send(err_msg)
                                            
                                        logger.error("RDF UNSUBSCRIBE REQUEST forwarding failed")
                                        print sys.exc_info()

                                    except AttributeError:
                                        clientsock.send()
    
                                    break
                                        
    
                        # RDF/SPARQL UNSUBSCRIBE CONFIRM                        
                        elif ssap_msg_dict["message_type"] == "CONFIRM" and ssap_msg_dict["transaction_type"] == "UNSUBSCRIBE": # and not "sparql" in ssap_msg
    
                            sib["timer"] = datetime.datetime.now()
        
                            for s in val_subscriptions:
                                if str(s.subscription_id) == str(ssap_msg_dict["subscription_id"]):
                                    
                                    try:
                                        if s.unsubscribed == False:
                                            s.conn.send(ssap_msg)
                                        print "close 256"
                                        s.conn.close()
                                    except socket.error:
                                        print sys.exc_info()
                                        pass
                                                                            
                                    # rather than destroying the class instance, 
                                    # we set the unsubscribed field to true, so that
                                    # the subscription_observer can avoid to send an
                                    # useless unsubscribe request to the publisher
                                    s.unsubscribed = True
    
    
                        # INDICATIONS
                            
                        # RDF/SPARQL SUBSCRIBE INDICATION
                        elif ssap_msg_dict["message_type"] == "INDICATION" and ssap_msg_dict["transaction_type"] == "SUBSCRIBE": 
    
                            sib["timer"] = datetime.datetime.now()
    
                            for s in val_subscriptions:
                                if str(s.subscription_id) == str(ssap_msg_dict["subscription_id"]):
    
                                    # send the message to the kp
                                    try:
                                        s.conn.send(ssap_msg)
                                    except socket.error:
                                        print colored("remoteSIB>", "red", attrs=["bold"]) + " indication send failed"
                                        print sys.exc_info()
                                    break
                    
                                
                        ### OTHER REQUESTS
                        elif ssap_msg_dict["message_type"] == "REQUEST":

                            # kp_list[ssap_msg_dict["node_id"]] = clientsock
                            kp_list[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = clientsock
    
                            # forwarding message to the publisher
                            try:
                                sib["socket"].send(ssap_msg)
                            except socket.error:
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                         ssap_msg_dict["space_id"],
                                                                         ssap_msg_dict["transaction_type"],
                                                                         ssap_msg_dict["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
    
                                # send a notification error to the KP
                                kp_list[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]].send(err_msg)
                                del kp_list[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]]
                                logger.error(ssap_msg_dict["transaction_type"] + " REQUEST forwarding failed")
                                print sys.exc_info()

                            except AttributeError:
                                clientsock.close()
    
    
                        ### OTHER CONFIRMS
            
                        elif ssap_msg_dict["message_type"] == "CONFIRM":

                            # update the timer
                            sib["timer"] = datetime.datetime.now()
                        
                            # forward message to the kp
                            kp_list[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]].send(ssap_msg)
                            kp_list[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]].close()                        
           
                    except ET.ParseError:
                        print colored("remoteSIB> ", "red", attrs=["bold"]) + " ParseError"
                        pass
        
        except socket.error:
            print colored("remoteSIB> ", "red", attrs=["bold"]) + " socket.error: break! 347"
            print sys.exc_info()
            break

# SOCKET OBSERVER THREAD
def socket_observer(sib, port, check_var, ancillary_ip, ancillary_port, manager_ip, manager_port):
    
    key = sib["socket"]
    
    while check_var:
        try:            
            if (datetime.datetime.now() - sib["timer"]).total_seconds() > 15:
                print colored("remoteSIB> ", "red", attrs=["bold"]) + " socket " + str(sib["socket"]) + " dead"
                sib["socket"] = None                
                
                # check if the vsib really exists
                a = SibLib(ancillary_ip, ancillary_port)
                res = a.execute_rdf_query(Triple(URI(ns + str(sib["virtual_sib_id"])), URI(rdf + "type"), URI(ns + "remoteSib")))
                if len(res) == 1:

                    # connect to the manager
                    manager = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    manager.connect((manager_ip, manager_port ))
                    
                    # Send a message to the manager to notify the new status
                    jmsg = {"command":"SetSIBStatus", "sib_id":str(sib["virtual_sib_id"]), "status":"offline"}
                    msg = json.dumps(jmsg)
                    manager.send(msg)
                    confirm = manager.recv(BUFSIZ)
                    confirm = json.loads(confirm)
                    print "mandato il SetSIBStatus al manager"
                    check_var = False
                    break

                break

            else:
                # socket is still alive, let's send another space
                time.sleep(5)
                sib["socket"].send(" ")
                
        except IOError:
            pass
        
    print colored("socket_observer> ", "red", attrs=["bold"]) + " closed observer thread for socket " + str(key)


def subscription_observer(sib, sub):

    print colored("remoteSib> ", "blue", attrs=["bold"]) + 'subscription observer started for subscription ' + str(sub.subscription_id)
    
    # local variables
    sub_id = sub.subscription_id

    # infinite loop
    while True:

        data = None
        time.sleep(5)
        try:
            sub.conn.send(" ")
            data = sub.conn.recv(4096)
                
        except socket.error:
            
            if sub.unsubscribed:
                print colored("remoteSib> ", "blue", attrs=["bold"]) + " subscription " + str(sub_id) + " closed"
            else:
                print colored("remoteSib> ", "red", attrs=["bold"]) + " subscription " + str(sub_id) + " dead"

                # In this case the subscription died, so it wasn't closed properly.
                # Now we build an unsubscribe request and send it to the publisher
                # so that it can close the subscription thread
                # NOTE: the transacation id for the unsubscribe request is generated incrementing
                # the transaction id used for the subscribe request. This may be a problem, we have to solve it!
                ssap_msg = SSAP_UNSUBSCRIBE_REQUEST_TEMPLATE%(str(int(sub.request_transaction_id) + 1), str(sub.node_id), str(sub.subscription_id))
                sib["socket"].send(ssap_msg)
                
            break

    return


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




def remoteSIB(virtualiser_ip, kp_port, pub_port, virtual_sib_id, check_var, ancillary_ip, ancillary_port, manager_ip, manager_port):

    print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' started a new remote SIB with ip ' + str(virtualiser_ip) + ", kpPort " + str(kp_port) + ", pubPort " + str(pub_port) + " and id " + str(virtual_sib_id)

    host = virtualiser_ip
    kp_addr = (host, kp_port)
    pub_addr = (host, pub_port)

    sib["virtual_sib_id"] = virtual_sib_id

    # creating and activating the socket for the KPs
    kp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #kp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    kp_socket.bind(kp_addr)
    kp_socket.listen(2)
    # logger.info('Remote SIB waiting for KPs on port ' + str(kp_port))
    
    # creating and activating the socket for the Publishers
    pub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    pub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    pub_socket.bind(pub_addr)
    pub_socket.listen(2)
    # logger.info('Remote SIB waiting for publishers on port ' + str(pub_port))

    # sockets
    sockets = [kp_socket, pub_socket]

    # loop
    while check_var:

        # print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' waiting for connections...'
        
        # select the read_sockets
        try:
            read_sockets,write_sockets,error_sockets = select.select(sockets,[],[])
        except:
            break
        
        # look for a connection on both the ports
        for sock in read_sockets:
            
            # new connection
            if sock in sockets:
                clientsock, addr = sock.accept()
                # print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' incoming connection from ...' + str(addr)
                # logger.info('Incoming connection from ' + str(addr))
                thread.start_new_thread(handler, (clientsock, addr, kp_port, ancillary_ip, ancillary_port, manager_ip, manager_port))

            # incoming data
            else:
               # print colored("remoteSIB> ", "blue", attrs=["bold"]) + ' incoming DATA'
                pass
    return
