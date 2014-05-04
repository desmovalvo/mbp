#!/usr/bin/python

# requirements
from SSAPLib import *
from termcolor import *
from lib.Subreq import *
from smart_m3.m3_kp import *
from xml.sax import make_parser
import thread
import threading
from xml.etree import ElementTree as ET
from termcolor import colored
from threading import Thread, Lock
import uuid


BUFSIZ = 1024

mutex = Lock()    
num_confirms = {}


##############################################################
#
# CONFIRMS
#
##############################################################

def join_confirm_handler(sib_sock, sibs_info, kp_list, n, logger):

    global mutex
    global num_confirms
    
    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
 
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

                if info["transaction_type"] == "JOIN":
                    # debug info
                    print colored("treplies>", "green", attrs=["bold"]) + " handle_join_confirm"
                    logger.info("JOIN CONFIRM handled by handle_join_confirm")
  
                    # check if we already received a failure
                    mutex.acquire()
                    if not num_confirms[info["node_id"]] == None:
                        try:

                            # check if the current message represent a successful insertion
                            if info["parameter_status"] == "m3:Success":
                               
                                num_confirms[info["node_id"]] -= 1
                                if num_confirms[info["node_id"]] == 0:
                                    kp_list[info["node_id"]].send(ssap_msg)
                                    print "inviata conferma join al kp"
                                    kp_list[info["node_id"]].close()
                
                            else:
                                num_confirms[info["node_id"]] = None
                                # send SSAP ERROR MESSAGE
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                         info["space_id"],
                                                                         "JOIN",
                                                                         info["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
                                kp_list[info["node_id"]].send(err_msg)
                                kp_list[info["node_id"]].close()
                                del kp_list[info["node_id"]]
                                logger.error("JOIN CONFIRM forwarding failed")
  
                        except socket.error:
                            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"

                
                    mutex.release()


            except ET.ParseError:
                print colored("treplies> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except ZeroDivisionError:#socket.error:
            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error"

                
            
        

def leave_confirm_handler(sib_sock, sibs_info, kp_list, n, logger):

    global mutex
    global num_confirms
    
    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = sib_sock.recv(BUFSIZ)

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


                if info["transaction_type"] == "LEAVE":
                    # debug info
                    print colored("treplies>", "green", attrs=["bold"]) + " handle_leave_confirm"
                    logger.info("LEAVE CONFIRM handled by handle_leave_confirm")

                    # check if we already received a failure
                    mutex.acquire()
                    if not num_confirms[info["node_id"]] == None:
                        try:
        
                            # check if the current message represent a successful insertion
                            if info["parameter_status"] == "m3:Success":
                                
                                num_confirms[info["node_id"]] -= 1
                                if num_confirms[info["node_id"]] == 0:
                                    kp_list[info["node_id"]].send(ssap_msg)       
                                    print "inviata conferma join al kp"
                                    kp_list[info["node_id"]].close()
                                    del kp_list[info["node_id"]]
        
                            else:
                                num_confirms[info["node_id"]] = None
                                # send SSAP ERROR MESSAGE
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                         info["space_id"],
                                                                         "LEAVE",
                                                                         info["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
                                kp_list[info["node_id"]].send(err_msg)
                                kp_list[info["node_id"]].close()
                                logger.error("LEAVE CONFIRM forwarding failed")

                        except socket.error:
                            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"

        
                    mutex.release()
                    
            except ET.ParseError:
                print colored("treplies> ", "red", attrs=["bold"]) + " ParseError"
                pass
            
            
        except socket.error:
            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"



# INSERT CONFIRM
def insert_confirm_handler(sib_sock, sibs_info, kp_list, n, logger):
    """This method is used to decide what to do once an INSERT CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    global mutex
    global num_confirms
    
    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
 
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

                if info["transaction_type"] == "INSERT":
  
                    # debug message
                    print colored("treplies>", "green", attrs=["bold"]) + " handle_insert_confirm"
                    logger.info("INSERT CONFIRM handled by handle_insert_confirm")
                    
                    # check if we already received a failure
                    mutex.acquire()
                    if not num_confirms[info["node_id"]] == None:
                        try:
                            # check if the current message represent a successful insertion
                            if info["parameter_status"] == "m3:Success":
                                num_confirms[info["node_id"]] -= 1
                                if num_confirms[info["node_id"]] == 0:    
                                    kp_list[info["node_id"]].send(ssap_msg)
                                    print "inviata conferma insert al kp"
                                    kp_list[info["node_id"]].close()
    
                            # if the current message represent a failure...
                            else:
                                
                                num_confirms[info["node_id"]] = None
                                # send SSAP ERROR MESSAGE
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                 info["space_id"],
                                                                 "INSERT",
                                                                 info["transaction_id"],
                                                                 '<parameter name="status">m3:Error</parameter>')
                                kp_list[info["node_id"]].send(err_msg)
                                kp_list[info["node_id"]].close()
                                logger.error("INSERT CONFIRM forwarding failed")

                        except socket.error:
                            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"

                
                    mutex.release()


            except ET.ParseError:
                print colored("treplies> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except socket.error:
            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error"


# REMOVE CONFIRM
def remove_confirm_handler(sib_sock, sibs_info, kp_list, n, logger):
    """This method is used to decide what to do once an REMOVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    global mutex
    global num_confirms

    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
 
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

                if info["transaction_type"] == "REMOVE":

                    # debug message
                    print colored("treplies>", "green", attrs=["bold"]) + " handle_remove_confirm"
                    logger.info("REMOVE CONFIRM handled by handle_remove_confirm")
                        
                    # check if we already received a failure
                    mutex.acquire()
                    if not num_confirms[info["node_id"]] == None:
                        try:
                            # check if the current message represent a successful insertion
                            if info["parameter_status"] == "m3:Success":
                                num_confirms[info["node_id"]] -= 1
                                if num_confirms[info["node_id"]] == 0:                      
                                    kp_list[info["node_id"]].send(ssap_msg)
                                    print "inviata conferma remove al kp"
                                    kp_list[info["node_id"]].close()
                    
                            # if the current message represent a failure...
                            else:
                                
                                confirms[info["node_id"]] = None
                                # send SSAP ERROR MESSAGE
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                 info["space_id"],
                                                                 "REMOVE",
                                                                 info["transaction_id"],
                                                                 '<parameter name="status">m3:Error</parameter>')
                                kp_list[info["node_id"]].send(err_msg)
                                kp_list[info["node_id"]].close()
                                logger.error("REMOVE CONFIRM forwarding failed")
                     
                        except socket.error:
                            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"

                
                    mutex.release()


            except ET.ParseError:
                print colored("treplies> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except socket.error:
            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error"



# SPARQL QUERY CONFIRM
def sparql_query_confirm_handler(sib_sock, sibs_info, kp_list, n, logger, query_results):
    """This method is used to manage sparql QUERY CONFIRM received. """

    global mutex
    global num_confirms

    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
 
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
                print "sono nel remove confirm handler: messaggio decodificato"
                if info["transaction_type"] == "QUERY" and "sparql" in ssap_msg:

                    # debug message
                    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_query_confirm"
                    logger.info("SPARQL QUERY CONFIRM handled by handle_sparql_query_confirm")
                            
                    # check if we already received a failure
                    mutex.acquire()
                    if not num_confirms[info["node_id"]] == None:
                        try:
                            # check if the current message represent a successful insertion
                            if info["parameter_status"] == "m3:Success":
                                num_confirms[info["node_id"]] -= 1
                                
                                # convert ssap_msg to dict
                                ssap_msg_dict = {}
                                parser = make_parser()
                                ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                                parser.setContentHandler(ssap_mh)
                                parser.parse(StringIO(ssap_msg))
                    
                                # extract triples from ssap reply
                                triple_list = parse_sparql(ssap_msg_dict["results"])
                                  
                                for triple in triple_list:
                                    query_results[info["node_id"]].append(triple)
                                
                                # remove duplicates
                                result = []
                                for triple in query_results[info["node_id"]]:
                                    if not triple in result:
                                        result.append(triple)
                                        
                                query_results[info["node_id"]] = result
                    
                                if num_confirms[info["node_id"]] == 0:    
                                    # build ssap reply
                                    ssap_reply = reply_to_sparql_query(ssap_msg_dict["node_id"],
                                                          ssap_msg_dict["space_id"],
                                                          ssap_msg_dict["transaction_id"],
                                                          result)
                    
                                    kp_list[info["node_id"]].send(ssap_reply)
                                    kp_list[info["node_id"]].close()
                    
                    
                            # if the current message represent a failure...
                            else:
                                
                                num_confirms[info["node_id"]] = None
                                # send SSAP ERROR MESSAGE
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                 info["space_id"],
                                                                 "QUERY",
                                                                 info["transaction_id"],
                                                                 '<parameter name="status">m3:Error</parameter>')
                                kp_list[info["node_id"]].send(err_msg)
                                kp_list[info["node_id"]].close()
                                logger.error("SPARQL CONFIRM forwarding failed")
                    
                        except socket.error:
                            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"

                
                    mutex.release()


            except ET.ParseError:
                print colored("treplies> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except socket.error:
            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error"
                    
# RDF QUERY CONFIRM
def rdf_query_confirm_handler(sib_sock, sibs_info, kp_list, n, logger, query_results):
    """This method is used to manage rdf QUERY CONFIRM received. """

    global mutex
    global num_confirms


    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:
        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
 
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
                print "sono nel remove confirm handler: messaggio decodificato"
                if info["transaction_type"] == "QUERY" and not "sparql" in ssap_msg:

                    # debug info
                    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_query_confirm"
                    logger.info("RDF QUERY CONFIRM handled by handle_rdf_query_confirm")
                    
                    # check if we already received a failure
                    mutex.acquire()
                    if not num_confirms[info["node_id"]] == None:
                        try:
                            # check if the current message represent a successful insertion
                            if info["parameter_status"] == "m3:Success":
                                num_confirms[info["node_id"]] -= 1
                                
                                # convert ssap_msg to dict
                                ssap_msg_dict = {}
                                parser = make_parser()
                                ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                                parser.setContentHandler(ssap_mh)
                                parser.parse(StringIO(ssap_msg))
                    
                                # extract triples from ssap reply
                                triple_list = parse_M3RDF(ssap_msg_dict["results"])
                                  
                                for triple in triple_list:
                                    query_results[info["node_id"]].append(triple)
                                
                                # remove duplicates
                                result = []
                                for triple in query_results[info["node_id"]]:
                                    if not triple in result:
                                        result.append(triple)
                                        
                                query_results[info["node_id"]] = result
                    
                                if num_confirms[info["node_id"]] == 0:    
                                    # build ssap reply
                                    ssap_reply = reply_to_rdf_query(ssap_msg_dict["node_id"],
                                                          ssap_msg_dict["space_id"],
                                                          ssap_msg_dict["transaction_id"],
                                                          result)
                    
                                    kp_list[info["node_id"]].send(ssap_reply)
                                    kp_list[info["node_id"]].close()
                    
                    
                            # if the current message represent a failure...
                            else:
                                
                                num_confirms[info["node_id"]] = None
                                # send SSAP ERROR MESSAGE
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                 info["space_id"],
                                                                 "QUERY",
                                                                 info["transaction_id"],
                                                                 '<parameter name="status">m3:Error</parameter>')
                                kp_list[info["node_id"]].send(err_msg)
                                kp_list[info["node_id"]].close()
                                logger.error("RDF QUERY CONFIRM forwarding failed")
                    
                        except socket.error:
                            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"

                
                    mutex.release()


            except ET.ParseError:
                print colored("treplies> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except socket.error:
            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error"
                    


# RDF SUBSCRIBE CONFIRM
def rdf_subscribe_confirm_handler(sib_sock, sibs_info, kp_list, n, logger, initial_results, active_subscriptions, clientsock, val_subscriptions, newsub):
    """This method is used to manage rdf SUBSCRIBE CONFIRM received. """

    global mutex
    global num_confirms

    print "sono nel subscribe confirm!!!!!"
    
    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:
        print "while"
        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
            print "------------------" + ssap_msg
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
                print "prova decod"
                
                # parse the ssap message
                root = ET.fromstring(ssap_msg)           
                info = {}
                for child in root:
                    if child.attrib.has_key("name"):
                        k = child.tag + "_" + str(child.attrib["name"])
                    else:
                        k = child.tag
                    info[k] = child.text
                print "sono nel subscribe confirm handler: messaggio decodificato"
                print ssap_msg
                if info["transaction_type"] == "SUBSCRIBE":

                    if info["message_type"] == "CONFIRM":
                        # debug info
                        print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_subscribe_confirm"
                        logger.info("RDF SUBSCRIBE CONFIRM handled by handle_rdf_subscribe_confirm")
                        
                        # check if we already received a failure
                        mutex.acquire()
                        if not num_confirms[info["node_id"]] == None:
                            try:
                                # check if the current message represent a successful insertion
                                if info["parameter_status"] == "m3:Success":
                        
                                    num_confirms[info["node_id"]] -= 1
                        
                                    # store the corrispondence between the real sib and the real_subscription_id
                                    for s in val_subscriptions:                              
                                        
                                        if s.node_id == info["node_id"] and s.request_transaction_id == info["transaction_id"]:
                        #                    s.received_confirm(clientsock, info["parameter_subscription_id"])
                                            subreq_instance = s
                                        
                                            # convert ssap_msg to dict
                                            ssap_msg_dict = {}
                                            parser = make_parser()
                                            ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                                            parser.setContentHandler(ssap_mh)
                                            parser.parse(StringIO(ssap_msg))
                                
                                            # extract triples from ssap reply
                                            triple_list = parse_M3RDF(ssap_msg_dict["results"])
                                              
                                            for triple in triple_list:
                                                initial_results[info["node_id"]].append(triple)
                                            
                                            # remove duplicates
                                            for triple in initial_results[info["node_id"]]:
                                                if not triple in s.result:
                                                    s.result.append(triple)
                                                    
                                            initial_results[info["node_id"]] = s.result
                                
                                            if num_confirms[info["node_id"]] == 0:    
                                                # build ssap reply                
                                                ssap_reply = reply_to_rdf_subscribe(ssap_msg_dict["node_id"],
                                                                                    ssap_msg_dict["space_id"],
                                                                                    ssap_msg_dict["transaction_id"],
                                                                                    s.result,
                                                                                    ssap_msg_dict["subscription_id"])
                                                                                    #subreq_instance.subscription_id)                        
                                                print ssap_reply
                                                subreq_instance.conn.send(ssap_reply)
                        
                                # if the current message represent a failure...
                                else:
                                    
                                    num_confirms[info["node_id"]] = None
                                    # send SSAP ERROR MESSAGE
                                    err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                             info["space_id"],
                                                                             "SUBSCRIBE",
                                                                             info["transaction_id"],
                                                                             '<parameter name="status">m3:Error</parameter>')
                        
                                    for s in val_subscriptions:
                                        if s.node_id == info["node_id"] and s.request_transaction_id == ["transaction_id"]:
                                            s.conn.send(err_msg)
                                            logger.error("SUBSCRIBE CONFIRM forwarding failed")
                        
                            except socket.error:
                                print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"
                    
                        mutex.release()
                    elif info["message_type"] == "INDICATION":
                        mutex.acquire()
                        subreq_instance.conn.send(ssap_msg)
                        mutex.release()

                        
                elif info["transaction_type"] == "UNSUBSCRIBE":
                    # debug info
                    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_unsubscribe_confirm"
                    logger.info("RDF UNSUBSCRIBE CONFIRM handled by handle_rdf_unsubscribe_confirm")
                    
                    # check if we already received a failure
                    mutex.acquire()
                    if not num_confirms[info["node_id"]] == None:
                        try:
                            # check if the current message represent a successful insertion
                            if info["parameter_status"] == "m3:Success":
                    
                                num_confirms[info["node_id"]] -= 1
                    
                                if num_confirms[info["node_id"]] == 0:    
                                    subreq_instance.conn.send(ssap_msg)
                    
                            # if the current message represent a failure...
                            else:
                                
                                num_confirms[info["node_id"]] = None
                                # send SSAP ERROR MESSAGE
                                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                                         info["space_id"],
                                                                         "UNSUBSCRIBE",
                                                                         info["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
                    
                                subreq_instance.conn.send(err_msg)
                                logger.error("UNSUBSCRIBE CONFIRM forwarding failed")
                    
                        except socket.error:
                            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error: break!"
                
                    mutex.release()


            except ET.ParseError:
                print colored("treplies> ", "red", attrs=["bold"]) + " ParseError"
                pass

        except socket.error:
            print colored("treplies> ", "red", attrs=["bold"]) + " socket.error"
                                        
                    
    
    
##############################################################
#
# REQUESTS
#
##############################################################
    
# JOIN REQUEST
def handle_join_request(logger, info, ssap_msg, sibs_info, kp_list, num):
    """The present method is used to manage the join request received from a KP."""
    t = {}
    global num_confirms 
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

      
    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_join_request"
    logger.info("JOIN REQUEST handled by handle_join_request")
    
    # print sibs_info
    
    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock

        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except :
             print colored("publisher> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "JOIN",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[info["node_id"]].send(err_msg)
             del kp_list[info["node_id"]]
             logger.error("JOIN REQUEST forwarding failed")

        
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(join_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger))
    
# LEAVE REQUEST
def handle_leave_request(logger, info, ssap_msg, sibs_info, kp_list, num):
    """The present method is used to manage the leave request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

    
    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_leave_request"
    logger.info("LEAVE REQUEST handled by handle_leave_request")

    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock
        
        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except :
             print colored("publisher> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "JOIN",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[info["node_id"]].send(err_msg)
             del kp_list[info["node_id"]]
             logger.error("JOIN REQUEST forwarding failed")

    
        print "***************   " + str(num_confirms)
        
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(leave_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger))


# INSERT REQUEST
def handle_insert_request(logger, info, ssap_msg, sibs_info, kp_list, num):
    """The present method is used to manage the insert request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_insert_request"
    logger.info("INSERT REQUEST handled by handle_insert_request")


    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock
        
        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except :
             print colored("treplies> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "INSERT",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[info["node_id"]].send(err_msg)
             logger.error("INSERT REQUEST forwarding failed")

    
        print "***************   " + str(num_confirms)
        
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(insert_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger))



# REMOVE REQUEST
def handle_remove_request(logger, info, ssap_msg, sibs_info, kp_list, num):
    """The present method is used to manage the remove request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_remove_request"
    logger.info("REMOVE REQUEST handled by handle_remove_request")


    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock
        
        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except :
             print colored("treplies> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "REMOVE",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[info["node_id"]].send(err_msg)
             logger.error("REMOVE REQUEST forwarding failed")

    
        print "***************   " + str(num_confirms)
        
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(remove_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger))


# SPARQL QUERY REQUEST
def handle_sparql_query_request(logger, info, ssap_msg, sibs_info, kp_list, num, query_results):
    """The present method is used to manage the sparql query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_query_request"
    logger.info("SPARQL QUERY REQUEST handled by handle_sparql_query_request")


    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock
        
        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except :
             print colored("treplies> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "QUERY",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[info["node_id"]].send(err_msg)
             logger.error("SPARQL QUERY REQUEST forwarding failed")

    
        print "***************   " + str(num_confirms)
        
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(sparql_query_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, query_results))


# RDF QUERY REQUEST
def handle_rdf_query_request(logger, info, ssap_msg, sibs_info, kp_list, num, query_results):
    """The present method is used to manage the rdf query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_query_request"
    logger.info("RDF QUERY REQUEST handled by handle_rdf_query_request")


    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock
        
        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except :
             print colored("treplies> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "QUERY",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[info["node_id"]].send(err_msg)
             logger.error("SPARQL QUERY REQUEST forwarding failed")

    
        print "***************   " + str(num_confirms)
        
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(rdf_query_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, query_results))


# RDF SUBSCRIBE REQUEST
def handle_rdf_subscribe_request(logger, info, ssap_msg, sibs_info, kp_list, num, clientsock, val_subscriptions, active_subscriptions, initial_results):
    """The present method is used to manage the rdf query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_subscribe_request"
    logger.info("RDF SUBSCRIBE REQUEST handled by handle_rdf_subscribe_request")

    # generating a Subreq instance
    newsub = Subreq(clientsock, info["node_id"], info["transaction_id"])
    val_subscriptions.append(newsub)

    # convert ssap_msg to dict
    ssap_msg_dict = {}
    parser = make_parser()
    ssap_mh = SSAPMsgHandler(ssap_msg_dict)
    parser.setContentHandler(ssap_mh)
    parser.parse(StringIO(ssap_msg))        


    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock
        
        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except socket.error:
             print colored("treplies> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "SUBSCRIBE",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             newsub.conn.send(err_msg)
             logger.error("RDF SUBSCRIBE REQUEST forwarding failed")
             
             
    
        print "***************   " + str(num_confirms)
        
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(rdf_subscribe_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, initial_results, active_subscriptions, clientsock, val_subscriptions, newsub))



# RDF UNSUBSCRIBE REQUEST
def handle_rdf_unsubscribe_request(logger, info, ssap_msg, sibs_info, kp_list, num, clientsock, val_subscriptions):
    """The present method is used to manage the rdf query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[info["node_id"]] = num
    sib_list_conn = {}

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_unsubscribe_request"
    logger.info("RDF UNSUBSCRIBE REQUEST handled by handle_rdf_unsubscribe_request")

    # # find the Subreq instance
    # for s in val_subscriptions:
    #     if str(s.virtual_subscription_id) == str(info["parameter_subscription_id"]):


    for s in sibs_info:
        ip = str(sibs_info[s]["ip"].split("#")[1])
        kp_port = sibs_info[s]["kp_port"]
        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock
        
        # connect to the 
        try:
            sock.connect((ip, kp_port))

            try:
                sock.send(ssap_msg)
            except socket.error:
                print colored("treplies>", "red", attrs=["bold"]) + " Send failed"       
               
        except socket.error:
             print colored("treplies> ", "red", attrs=['bold']) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "UNSUBSCRIBE",
                                                      info["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             s.conn.send(err_msg)
             logger.error("RDF UNSUBSCRIBE REQUEST forwarding failed")
             break
             
    
        print "***************   " + str(num_confirms)



##############################################################
#
# OLD confirms
#
##############################################################

# RDF SUBSCRIBE CONFIRM
def handle_rdf_subscribe_confirm(logger, info, ssap_msg, confirms, kp_list, initial_results, active_subscriptions, clientsock, val_subscriptions):
    """This method is used to manage rdf SUBSCRIBE CONFIRM received. """

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_subscribe_confirm"
    logger.info("RDF SUBSCRIBE CONFIRM handled by handle_rdf_subscribe_confirm")
    
    # check if we already received a failure
    if not confirms[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":

            confirms[info["node_id"]] -= 1

            # store the corrispondence between the real sib and the real_subscription_id
            for s in val_subscriptions:                              
                
                if s.node_id == info["node_id"] and s.request_transaction_id == info["transaction_id"]:
#                    s.received_confirm(clientsock, info["parameter_subscription_id"])
                    subreq_instance = s
                
                    # convert ssap_msg to dict
                    ssap_msg_dict = {}
                    parser = make_parser()
                    ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                    parser.setContentHandler(ssap_mh)
                    parser.parse(StringIO(ssap_msg))
        
                    # extract triples from ssap reply
                    triple_list = parse_M3RDF(ssap_msg_dict["results"])
                      
                    for triple in triple_list:
                        initial_results[info["node_id"]].append(triple)
                    
                    # remove duplicates
                    for triple in initial_results[info["node_id"]]:
                        if not triple in s.result:
                            s.result.append(triple)
                            
                    initial_results[info["node_id"]] = s.result
        
                    if confirms[info["node_id"]] == 0:    
                        # build ssap reply                
                        ssap_reply = reply_to_rdf_subscribe(ssap_msg_dict["node_id"],
                                                            ssap_msg_dict["space_id"],
                                                            ssap_msg_dict["transaction_id"],
                                                            s.result,
                                                            subreq_instance.virtual_subscription_id)                        
                        subreq_instance.conn.send(ssap_reply)

        # if the current message represent a failure...
        else:
            
            confirms[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "SUBSCRIBE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')

            for s in val_subscriptions:
                if s.node_id == info["node_id"] and s.request_transaction_id == ["transaction_id"]:
                    s.conn.send(err_msg)
                    logger.error("SUBSCRIBE CONFIRM forwarding failed")
                    

# RDF UNSUBSCRIBE CONFIRM
def handle_rdf_unsubscribe_confirm(logger, info, ssap_msg, confirms, kp_list, initial_results, active_subscriptions, clientsock, val_subscriptions):
    """This method is used to manage UNSUBSCRIBE CONFIRM received. """

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_unsubscribe_confirm"
    logger.info("RDF UNSUBSCRIBE CONFIRM handled by handle_rdf_unsubscribe_confirm")


    for s in val_subscriptions:
        if str(s.virtual_subscription_id) == str(info["parameter_subscription_id"]):
            # check if we already received a failure
            if not confirms[info["node_id"]] == None:
                # check if the current message represent a successful insertion
                if info["parameter_status"] == "m3:Success":

                    confirms[info["node_id"]] -= 1

                    if confirms[info["node_id"]] == 0:    

                        s.conn.send(ssap_msg)


                # if the current message represent a failure...
                else:
            
                    confirms[info["node_id"]] = None
                    # send SSAP ERROR MESSAGE
                    err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                             info["space_id"],
                                                             "UNSUBSCRIBE",
                                                             info["transaction_id"],
                                                             '<parameter name="status">m3:Error</parameter><parameter name="subscription_id">virtual_sub_id</parameter>')

                    s.conn.send(ssap_reply)

                    logger.error("SUBSCRIBE CONFIRM forwarding failed")
                    
                    # destroy the class instance
                    del s


##############################################################
#
# INDICATIONS
#
##############################################################

def handle_subscribe_indication(logger, ssap_msg, info, fromsocket, val_subscriptions):

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_subscribe_indication"
    logger.info("SUBSCRIBE INDICATION handled by handle_subscribe_indication")

    for s in val_subscriptions:
        if str(s.virtual_subscription_id) == str(info["parameter_subscription_id"]):

            # send the message to the kp
            print "Inoltro la indication"
            try:
                s.conn.send(ssap_msg)
            except socket.error:
                print "inoltro indication fallito"
            
            break
                

##############################################################
#
# UTILITIES
#
##############################################################

def reply_to_sparql_query(node_id, space_id, transaction_id, results):
    # TODO: puo' dare problemi il fatto che piu' thread usano questa funzione?
    # building HEAD part of the query results
    variable_list = []
    for triple in results:
        for element in triple:    
            if not SSAP_VARIABLE_TEMPLATE%(str(element[0])) in variable_list:
                variable_list.append(SSAP_VARIABLE_TEMPLATE%(str(element[0])))
    head = SSAP_HEAD_TEMPLATE%(''.join(variable_list))
    
    # building RESULTS part of the query results
    result_string = ""
    for triple in results:
        binding_string = ""
        for element in triple:    
            binding_string = binding_string + SSAP_BINDING_TEMPLATE%(element[0], element[2])
        result_string = result_string + SSAP_RESULT_TEMPLATE%(binding_string)
    results_string = SSAP_RESULTS_TEMPLATE%(result_string)
    body = SSAP_RESULTS_SPARQL_PARAM_TEMPLATE%(head + results_string)

    # finalizing the reply
    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(node_id, 
                                    space_id, 
                                    "QUERY",
                                    transaction_id,
                                    body)
    return reply



def reply_to_rdf_query(node_id, space_id, transaction_id, results):

    tr = ""
    for el in results:
        tr = tr + SSAP_TRIPLE_TEMPLATE%(el[0], el[1], el[2])
            
    body = SSAP_RESULTS_RDF_PARAM_TEMPLATE%(SSAP_TRIPLE_LIST_TEMPLATE%(tr))
    
    # finalizing the reply
    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(node_id, 
                                    space_id, 
                                    "QUERY",
                                    transaction_id,
                                    body)
    return reply


def reply_to_rdf_subscribe(node_id, space_id, transaction_id, results, subscription_id):
    tr = ""
    for el in results:
        tr = tr + SSAP_TRIPLE_TEMPLATE%(el[0], el[1], el[2])            
    body = SSAP_RESULTS_SUB_RDF_PARAM_TEMPLATE%(subscription_id, SSAP_TRIPLE_LIST_TEMPLATE%(tr))
    
    # finalizing the reply
    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(node_id, 
                                    space_id, 
                                    "SUBSCRIBE",
                                    transaction_id,
                                    body)
    return reply


