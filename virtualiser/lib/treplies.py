#!/usr/bin/python

# requirements
import random
import uuid
import thread
import threading
from SSAPLib import *
from termcolor import *
from lib.Subreq import *
from smart_m3.m3_kp import *
from output_helpers import *
from termcolor import colored
from xml.sax import make_parser
from threading import Thread, Lock
from xml.etree import ElementTree as ET
import time
from message_helpers import *
from xml_helpers import *
import sys

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
    
    print "sono nel join confirm handler"
    
    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:


        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
 
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
                        #info = parse_message(ssap_msg)

                        # closing socket
                        try:
                            sib_sock.close()
                        except:
                            "Socket already closed"
        
                        if ssap_msg_dict["transaction_type"] == "JOIN":
        
                            # debug info
                            print treplies_print(True) + " handle_join_confirm"
                            logger.info("JOIN CONFIRM handled by handle_join_confirm")
          
                            # check if we already received a failure
                            mutex.acquire()
                            if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:
                                try:
        
                                    # check if the current message represent a successful insertion
                                    if ssap_msg_dict["status"] == "m3:Success":
                                       
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                                        if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:
                                            kp_list[ssap_msg_dict["node_id"]].send(ssap_msg)
                                            print "inviata conferma join al kp"
                                            kp_list[ssap_msg_dict["node_id"]].close()
                        
                                    else:
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = None
                                        # send SSAP ERROR MESSAGE
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                                 ssap_msg_dict["space_id"],
                                                                                 "JOIN",
                                                                                 ssap_msg_dict["transaction_id"],
                                                                                 '<parameter name="status">m3:Error</parameter>')
                                        kp_list[ssap_msg_dict["node_id"]].send(err_msg)
                                        kp_list[ssap_msg_dict["node_id"]].close()
                                        del kp_list[ssap_msg_dict["node_id"]]
                                        logger.error("JOIN CONFIRM forwarding failed")
          
                                except socket.error:
                                    print treplies_print(False) + " socket.error treplies: break! 104"
                                
                            mutex.release()
        
        
                    except ET.ParseError:
                        #print treplies_print(False) + " ParseError"
                        pass
        
        except socket.error:
            print treplies_print(False) + " socket.error treplies 120"
            break

                
            
        

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
                        #info = parse_message(ssap_msg)

                        # closing socket
                        try:
                            sib_sock.close()
                        except:
                            "Socket already closed"                        
        
                        if ssap_msg_dict["transaction_type"] == "LEAVE":
                            # debug info
                            print treplies_print(True) + " handle_leave_confirm"
                            logger.info("LEAVE CONFIRM handled by handle_leave_confirm")
        
                            # check if we already received a failure
                            mutex.acquire()
                            if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:
                                try:
                
                                    # check if the current message represent a successful insertion
                                    if ssap_msg_dict["status"] == "m3:Success":
                                        
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                                        if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:
                                            kp_list[ssap_msg_dict["node_id"]].send(ssap_msg)       
        
                                            kp_list[ssap_msg_dict["node_id"]].close()
                                            del kp_list[ssap_msg_dict["node_id"]]
                
                                    else:
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = None
                                        # send SSAP ERROR MESSAGE
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                                 ssap_msg_dict["space_id"],
                                                                                 "LEAVE",
                                                                                 ssap_msg_dict["transaction_id"],
                                                                                 '<parameter name="status">m3:Error</parameter>')
                                        kp_list[ssap_msg_dict["node_id"]].send(err_msg)
                                        kp_list[ssap_msg_dict["node_id"]].close()
                                        logger.error("LEAVE CONFIRM forwarding failed")
        
                                except socket.error:
                                    print treplies_print(False) + " socket.error treplies: break! 191"
        
                
                            mutex.release()
                            
                    except ET.ParseError:
                        #print treplies_print(False) + " ParseError"
                        pass
                    
                    
        except socket.error:
            print treplies_print(False) + " socket.error treplies: break! 201"
            break



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
                        #info = parse_message(ssap_msg)

                        # closing socket
                        try:
                            sib_sock.close()
                        except:
                            "Socket already closed"
                                
                        if ssap_msg_dict["transaction_type"] == "INSERT":
          
                            # debug message
                            print treplies_print(True) + " handle_insert_confirm"
                            logger.info("INSERT CONFIRM handled by handle_insert_confirm")
                            
                            # check if we already received a failure
                            mutex.acquire()
                            if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:
                                try:
                                    # check if the current message represent a successful insertion
                                    if ssap_msg_dict["status"] == "m3:Success":
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                                        if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:    
                                            kp_list[ssap_msg_dict["node_id"]].send(ssap_msg)
                                            
                                            kp_list[ssap_msg_dict["node_id"]].close()
            
                                    # if the current message represent a failure...
                                    else:
                                        
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = None
                                        # send SSAP ERROR MESSAGE
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                         ssap_msg_dict["space_id"],
                                                                         "INSERT",
                                                                         ssap_msg_dict["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
                                        kp_list[ssap_msg_dict["node_id"]].send(err_msg)
                                        kp_list[ssap_msg_dict["node_id"]].close()
                                        logger.error("INSERT CONFIRM forwarding failed")
        
                                except socket.error:
                                    print treplies_print(False) + " socket.error treplies: break! 282"
        
                        
                            mutex.release()
        
        
                    except ET.ParseError:
                        #print treplies_print(False) + " ParseError"
                        pass
        
        except socket.error:
            print treplies_print(False) + " socket.error treplies 293"
            break


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
                        #info = parse_message(ssap_msg)

                        # closing socket
                        try:
                            sib_sock.close()
                        except:
                            "Socket already closed"         
        
                        if ssap_msg_dict["transaction_type"] == "REMOVE":
        
                            # debug message
                            print treplies_print(True) + " handle_remove_confirm"
                            logger.info("REMOVE CONFIRM handled by handle_remove_confirm")
                                
                            # check if we already received a failure
                            mutex.acquire()
                            if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:
                                try:
                                    # check if the current message represent a successful insertion
                                    if ssap_msg_dict["status"] == "m3:Success":
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                                        if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:                      
                                            kp_list[ssap_msg_dict["node_id"]].send(ssap_msg)
        
                                            kp_list[ssap_msg_dict["node_id"]].close()
                            
                                    # if the current message represent a failure...
                                    else:
                                        
                                        confirms[ssap_msg_dict["node_id"]] = None
                                        # send SSAP ERROR MESSAGE
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                         ssap_msg_dict["space_id"],
                                                                         "REMOVE",
                                                                         ssap_msg_dict["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
                                        kp_list[ssap_msg_dict["node_id"]].send(err_msg)
                                        kp_list[ssap_msg_dict["node_id"]].close()
                                        logger.error("REMOVE CONFIRM forwarding failed")
                             
                                except socket.error:
                                    print treplies_print(False) + " socket.error treplies: break! 373"
        
                        
                            mutex.release()
        
        
                    except ET.ParseError:
                        #print treplies_print(False) + " ParseError"
                        pass
        
        except socket.error:
            print treplies_print(False) + " socket.error treplies 384"
            break


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

                        ssap_root = ET.fromstring(ssap_msg)
                        ssap_msg_dict = build_dict(ssap_root)

                        # closing socket
                        try:
                            sib_sock.close()
                        except:
                            "Socket already closed"

                        # # convert ssap_msg to dict
                        # ssap_msg_dict = msg_to_dict(ssap_msg)
                        
                        if ssap_msg_dict["transaction_type"] == "QUERY":
        
                            # debug message
                            print treplies_print(True) + " handle_sparql_query_confirm"
                            logger.info("SPARQL QUERY CONFIRM handled by handle_sparql_query_confirm")
                                    
                            # check if we already received a failure
                            mutex.acquire()
                            if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:

                                try:

                                    # check if the current message represent a successful insertion
                                    if ssap_msg_dict["status"] == "m3:Success":

                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                                        
                                        start = time.time()
                                                                    
                                        # extract triples from ssap reply
                                        triple_list = extract_sparql_triples(ssap_root)
                                        #triple_list = parse_sparql(ssap_msg_dict["results"])
                                          
                                        end = time.time() - start
                                        
#                                        print "Tempo parsing loro: " + str(end)


                                        # duplicates removal
                                        start = time.time()
                                        lista = query_results[ssap_msg_dict["node_id"]] + triple_list
                                        seen = set()
                                        new_list = []
                                        for triple in lista:
                                            if str(triple) in seen:
                                                continue
                                            else:
                                                seen.add(str(triple))
                                                new_list.append(triple)

                                        end = time.time() - start
#                                        print "tempo rimozione duplicati " + str(end) 
                            
                                        if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:    

                                            print "IF 4"
                                            
                                            # build ssap reply
                                            print "sto costruendo il messaggio..."
                                            start = time.time()
                                            ssap_reply = reply_to_sparql_query(ssap_msg_dict["node_id"],
                                                                  ssap_msg_dict["space_id"],
                                                                  ssap_msg_dict["transaction_id"],
#                                                                  query_results[ssap_msg_dict["node_id"]])
                                                                  new_list)
                                            end = time.time() - start 
#                                            print "Tempo di costruzione mex " + str(end) 
                                            
                                            kp_list[ssap_msg_dict["node_id"]].send(ssap_reply)
                                            kp_list[ssap_msg_dict["node_id"]].close()
                            
                            
                                    # if the current message represent a failure...
                                    else:
                                        
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = None
                                        # send SSAP ERROR MESSAGE
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                         ssap_msg_dict["space_id"],
                                                                         "QUERY",
                                                                         ssap_msg_dict["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
                                        kp_list[ssap_msg_dict["node_id"]].send(err_msg)
                                        kp_list[ssap_msg_dict["node_id"]].close()
                                        logger.error("SPARQL CONFIRM forwarding failed")
                            
                                except socket.error:
                                    print treplies_print(False) + " socket.error treplies: break! 502"
        
                        
                            mutex.release()
        
        
                    except ET.ParseError:
                        print treplies_print(False) + " ParseError"
                        pass
        
        except socket.error:
            print treplies_print(False) + " socket.error treplies 513"
            break


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

                        # convert ssap_msg to dict
                        ssap_root = ET.fromstring(ssap_msg)
                        ssap_msg_dict = build_dict(ssap_root)
                                                   
                        # closing socket
                        try:
                            sib_sock.close()
                        except:
                            "Socket already closed"
                            

                        if ssap_msg_dict["transaction_type"] == "QUERY":

                            # debug info
                            print treplies_print(True) + " handle_rdf_query_confirm"
                            logger.info("RDF QUERY CONFIRM handled by handle_rdf_query_confirm")
                            
                            # check if we already received a failure
                            mutex.acquire()
                            if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:

                                try:
                                    # check if the current message represent a successful insertion
                                    if ssap_msg_dict["status"] == "m3:Success":
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                                        
                                        # extract triples from ssap reply
                                        start = time.time()
#                                        triple_list = parse_M3RDF(ssap_msg_dict["results"])
                                        triple_list = extract_rdf_triples(ssap_root)
                                        end = time.time() - start
#                                        print "tempo estrazione delle triple " + str(end) 
                                          
                                        # for triple in triple_list:
                                        #     query_results[ssap_msg_dict["node_id"]].append(triple)
                                        
                                        # # remove duplicates
                                        # result = []
                                        # for triple in query_results[ssap_msg_dict["node_id"]]:
                                        #     if not triple in result:
                                        #         result.append(triple)
                                                
                                        
                                        start = time.time()
                                        lista = query_results[ssap_msg_dict["node_id"]] + triple_list
                                        seen = set()
                                        new_list = []
                                        for triple in lista:
                                            if str(triple) in seen:
                                                continue
                                            else:
                                                seen.add(str(triple))
                                                new_list.append(triple)

                                        end = time.time() - start
#                                        print "tempo rimozione duplicati " + str(end) 
                            
                                        if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:    
                                            # build ssap reply
                                            ssap_reply = reply_to_rdf_query(ssap_msg_dict["node_id"],
                                                                  ssap_msg_dict["space_id"],
                                                                  ssap_msg_dict["transaction_id"],
                                                                  new_list)
                            
                                            kp_list[ssap_msg_dict["node_id"]].send(ssap_reply)
                                            kp_list[ssap_msg_dict["node_id"]].close()
                            
                            
                                    # if the current message represent a failure...
                                    else:
                                        
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = None
                                        # send SSAP ERROR MESSAGE
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                         ssap_msg_dict["space_id"],
                                                                         "QUERY",
                                                                         ssap_msg_dict["transaction_id"],
                                                                         '<parameter name="status">m3:Error</parameter>')
                                        kp_list[ssap_msg_dict["node_id"]].send(err_msg)
                                        kp_list[ssap_msg_dict["node_id"]].close()
                                        logger.error("RDF QUERY CONFIRM forwarding failed")
                                        
                                except socket.error:
                                    print treplies_print(False) + " socket.error treplies: break! 626"

                
                            mutex.release()
                            

                    except ET.ParseError:
                        #print treplies_print(False) + " ParseError"
                        pass

        except socket.error:
            print treplies_print(False) + " socket.error treplies 636"
            break


# RDF SUBSCRIBE CONFIRM
def rdf_subscribe_confirm_handler(sib_sock, sibs_info, kp_list, n, logger, initial_results, active_subscriptions, clientsock, val_subscriptions, newsub, query_type):
    """This method is used to manage rdf SUBSCRIBE CONFIRM received. """

    global mutex
    global num_confirms

    ###############################################
    ## ricezione e riunificazione del messaggio  ##
    ###############################################
    complete_ssap_msg = ""
    while 1:


        try:
            ssap_msg = sib_sock.recv(BUFSIZ)
 
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
                        # convert ssap_msg to dict
                        ssap_root = ET.fromstring(ssap_msg)
                        ssap_msg_dict = build_dict(ssap_root)
                        # print ssap_msg_dict
#                        ssap_msg_dict = msg_to_dict(ssap_msg)

                        if ssap_msg_dict["transaction_type"] == "SUBSCRIBE":
        
                            if ssap_msg_dict["message_type"] == "CONFIRM":
                                # debug info             
#                                print ssap_msg_dict
                                print treplies_print(True) + " handle_subscribe_confirm"
                                logger.info(query_type + " SUBSCRIBE CONFIRM handled by handle_subscribe_confirm")
                                
                                # check if we already received a failure
                                mutex.acquire()
                                if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:
                                    try:
                                        
                                        print "SONO NEL TRY"

                                        # check if the current message represent a successful insertion
                                        if ssap_msg_dict["status"] == "m3:Success":
                                
                                            num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                                
                                            # store the corrispondence between the real sib and the real_subscription_id
                                            for s in val_subscriptions:                              
                                                
                                                if s.node_id == ssap_msg_dict["node_id"] and s.request_transaction_id == ssap_msg_dict["transaction_id"]:
                                #                    s.received_confirm(clientsock, info["parameter_subscription_id"])
                                                    subreq_instance = s
                                                
                                                    # extract triples from ssap reply
                                                    if query_type == "RDF-M3":
                                                        # triple_list = parse_M3RDF(ssap_msg_dict["results"])
                                                        triple_list = extract_rdf_triples(ssap_root)
                                                    else:
                                                        triple_list = extract_sparql_triples(ssap_root)
                                                        #triple_list = parse_sparql(ssap_msg_dict["results"])
                                                    

                                                    ########
                                                    # duplicates removal
                                                    start = time.time()
                                                    lista = initial_results[ssap_msg_dict["node_id"]] + triple_list
                                                    seen = set()
                                                    new_list = []
                                                    for triple in lista:
                                                        if str(triple) in seen:
                                                            continue
                                                        else:
                                                            seen.add(str(triple))
                                                            #s.result.append(triple)
                                                            new_list.append(triple)

                                                    end = time.time() - start
#                                                    print "tempo rimozione duplicati " + str(end) 

                                                    ###########

                                                      
                                                    # for triple in triple_list:
                                                    #     initial_results[ssap_msg_dict["node_id"]].append(triple)
                                                    
                                                    # # remove duplicates
                                                    # for triple in initial_results[ssap_msg_dict["node_id"]]:
                                                    #     if not triple in s.result:
                                                    #         s.result.append(triple)
                                                            
                                                    initial_results[ssap_msg_dict["node_id"]] = new_list

                                                    #print "---------risultati iniziali: ----------\n" + str(new_list)

                                                                                                                                                                                             
                                                    if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:    
                                                        if query_type == "RDF-M3":
                                                            # build ssap reply                
                                                            ssap_reply = reply_to_rdf_subscribe(ssap_msg_dict["node_id"],
                                                                                                ssap_msg_dict["space_id"],
                                                                                                ssap_msg_dict["transaction_id"],
                                                                                                new_list,
                                                                                                ssap_msg_dict["subscription_id"])
                                                                                            #subreq_instance.subscription_id)                        
                                                        else:
# build ssap reply                
                                                            ssap_reply = reply_to_sparql_subscribe(ssap_msg_dict["node_id"],
                                                                                                ssap_msg_dict["space_id"],
                                                                                                ssap_msg_dict["transaction_id"],
                                                                                                new_list,
                                                                                                ssap_msg_dict["subscription_id"])
                                                                                            #subreq_instance.subscription_id)                        
        
                                                        subreq_instance.conn.send(ssap_reply)
                                
                                        # if the current message represent a failure...
                                        else:
                                            
                                            num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = None
                                            print "SETTING num_confirms " + str(ssap_msg_dict["node_id"]) + "_" + str(ssap_msg_dict["transaction_id"]) + " a NONE"
                                            
                                            # send SSAP ERROR MESSAGE
                                            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                                     ssap_msg_dict["space_id"],
                                                                                     "SUBSCRIBE",
                                                                                     ssap_msg_dict["transaction_id"],
                                                                                     '<parameter name="status">m3:Error</parameter>')
                                
                                            for s in val_subscriptions:
                                                if s.node_id == ssap_msg_dict["node_id"] and s.request_transaction_id == ["transaction_id"]:
                                                    s.conn.send(err_msg)
                                                    s.conn.close()
                                                    logger.error("SUBSCRIBE CONFIRM forwarding failed")
                                
                                    except socket.error:
                                        print treplies_print(False) + " socket.error treplies: break! 788"
                            
                                mutex.release()
                            elif ssap_msg_dict["message_type"] == "INDICATION":
                                mutex.acquire()
                                subreq_instance.conn.send(ssap_msg)
                                mutex.release()
        
                                
                        elif ssap_msg_dict["transaction_type"] == "UNSUBSCRIBE":
                            # TODO qui bisogna anche eliminare l'istanza della subscription!
                            # debug info
                            print treplies_print(True) + " handle_unsubscribe_confirm"
                            logger.info(query_type + " UNSUBSCRIBE CONFIRM handled by handle_unsubscribe_confirm")
                            

                            # closing socket
                            try:
                                sib_sock.close()
                            except:
                                "Socket already closed"

                            # check if we already received a failure
                            mutex.acquire()
                            if not num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == None:
                                try:
                                    # check if the current message represent a successful insertion
                                    if ssap_msg_dict["status"] == "m3:Success":
                                                                    
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] -= 1
                            
                                        if num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] == 0:    
                                            subreq_instance.conn.send(ssap_msg)
                                            subreq_instance.conn.close()
                            
                                    # if the current message represent a failure...
                                    else:
                                        
                                        num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = None
                                        # send SSAP ERROR MESSAGE
                                        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                                                 ssap_msg_dict["space_id"],
                                                                                 "UNSUBSCRIBE",
                                                                                 ssap_msg_dict["transaction_id"],
                                                                                 '<parameter name="status">m3:Error</parameter>')
                            
                                        subreq_instance.conn.send(err_msg)
                                        logger.error("UNSUBSCRIBE CONFIRM forwarding failed")
                            
                                except socket.error:
                                    print treplies_print(False) + " socket.error treplies: break! 830"
                        
                            mutex.release()
        
        
                    except ET.ParseError:
                        #print treplies_print(False) + " ParseError"
                        pass
        
        except socket.error:
            print treplies_print(False) + " socket.error treplies 839"
            break
                    
    
    
##############################################################
#
# REQUESTS
#
##############################################################
    
# JOIN/LEAVE/INSERT/REMOVE REQUESTS
def handle_generic_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, num):

    """The present method is used to manage the join/leave/insert/remove
    requests received from a KP."""

    print "sono nel generic handler"
    t = {}
    global num_confirms 
    num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = num
    sib_list_conn = {}
      
    # debug message
    print treplies_print(True) + " handle_generic_request"
    logger.info(ssap_msg_dict["transaction_type"] + " REQUEST handled by handle_generic_request")
    
    print 'SIBS INFO: '
    print sibs_info

    # cycle through all the SIBs that compose the VMSIB
    for s in sibs_info:

        # get connection parameters from the sibs_info dict
        ip = str(sibs_info[s]["ip"])
        kp_port = sibs_info[s]["kp_port"]

        # socket to the sib
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(15)
        sib_list_conn[s] = sock

        # connect to the SIBs
        try:
            print "CONTROLLA QUI:"
            print ip, 
            print type(kp_port)
            sock.connect((ip, kp_port))
            print "connessa alla sib"

            try:
                print "sto inoltrando il messaggio ad una sib"
                sock.send(ssap_msg)
            except socket.error:
                print treplies_print(False) + " Send failed 885"       
               
        except :
             print treplies_print(False) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      ssap_msg_dict["transaction_type"],
                                                      ssap_msg_dict["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[ssap_msg_dict["node_id"]].send(err_msg)
             del kp_list[ssap_msg_dict["node_id"]]
             logger.error(ssap_msg_dict["transaction_type"] + " REQUEST forwarding failed")

        
        n = str(uuid.uuid4())
        t[n] = n

        # spawning threads
        print 'Start a thread for ' + str(s)
        func = globals()[ssap_msg_dict["transaction_type"].lower() + "_confirm_handler"]
        print "sto richiamando l'handler confirm..."
        thread.start_new_thread(func, (sib_list_conn[s], sibs_info, kp_list, t[n], logger))


# RDF/SPARQL QUERY REQUEST
def handle_query_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, num, query_results):

    """The present method is used to manage the query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = num
    sib_list_conn = {}

    # debug info
    print treplies_print(True) + " handle_query_request"
    logger.info(ssap_msg_dict["query_type"] + " QUERY REQUEST handled by handle_sparql_query_request")


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
                print "SENDING MESSAGE TO SIB: " + str(s)
                sock.send(ssap_msg)
            except socket.error:
                print treplies_print(False) + " Send failed 940"       
               
        except :
             print treplies_print(False) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      "QUERY",
                                                      ssap_msg_dict["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[ssap_msg_dict["node_id"]].send(err_msg)
             logger.error(ssap_msg_dict["query_type"] + " QUERY REQUEST forwarding failed")

    
        n = str(uuid.uuid4())
        t[n] = n

        if ssap_msg_dict["query_type"] == "sparql":
            print "START A THREAD FOR " + str(s)
            thread.start_new_thread(sparql_query_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, query_results))
        else:
            thread.start_new_thread(rdf_query_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, query_results))



# SPARQL QUERY REQUEST
def handle_sparql_query_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, num, query_results):
    """The present method is used to manage the sparql query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = num
    sib_list_conn = {}

    # debug info
    print treplies_print(True) + " handle_sparql_query_request"
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
                print treplies_print(False) + " Send failed"       
               
        except :
             print treplies_print(False) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      "QUERY",
                                                      ssap_msg_dict["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[ssap_msg_dict["node_id"]].send(err_msg)
             logger.error("SPARQL QUERY REQUEST forwarding failed")

    
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(sparql_query_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, query_results))
        


# RDF QUERY REQUEST
def handle_rdf_query_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, num, query_results):
    """The present method is used to manage the rdf query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = num
    sib_list_conn = {}

    # debug info
    print treplies_print(True) + " handle_rdf_query_request"
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
                print treplies_print(False) + " Send failed 1042"       
               
        except :
             print treplies_print(False) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      "QUERY",
                                                      ssap_msg_dict["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             kp_list[ssap_msg_dict["node_id"]].send(err_msg)
             logger.error("SPARQL QUERY REQUEST forwarding failed")

    
        n = str(uuid.uuid4())
        t[n] = n
        thread.start_new_thread(rdf_query_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, query_results))


# RDF SUBSCRIBE REQUEST
def handle_rdf_subscribe_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, num, clientsock, val_subscriptions, active_subscriptions, initial_results):
    """The present method is used to manage the rdf query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = num
    sib_list_conn = {}

    # debug info
    print treplies_print(True) + " handle_rdf_subscribe_request"
    logger.info("RDF SUBSCRIBE REQUEST handled by handle_rdf_subscribe_request")

    # generating a Subreq instance
    newsub = Subreq(clientsock, ssap_msg_dict)
    val_subscriptions.append(newsub)

    # convert ssap_msg to dict
    ssap_root = ET.fromstring(ssap_msg)
    ssap_msg_dict = build_dict(ssap_root)
    # ssap_msg_dict = {}
    # parser = make_parser()
    # ssap_mh = SSAPMsgHandler(ssap_msg_dict)
    # parser.setContentHandler(ssap_mh)
    # parser.parse(StringIO(ssap_msg))        


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
                print treplies_print(False) + " Send failed - line 1110"       
               
        except socket.error:
             print treplies_print(False) + 'Unable to connect to the sibs'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      "SUBSCRIBE",
                                                      ssap_msg_dict["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             newsub.conn.send(err_msg)
             logger.error("RDF SUBSCRIBE REQUEST forwarding failed")
             
             
        n = str(uuid.uuid4())
        t[n] = n
        query_type = ssap_msg_dict["query_type"]
        thread.start_new_thread(rdf_subscribe_confirm_handler, (sib_list_conn[s], sibs_info, kp_list, t[n], logger, initial_results, active_subscriptions, clientsock, val_subscriptions, newsub, query_type))



# RDF UNSUBSCRIBE REQUEST
def handle_rdf_unsubscribe_request(logger, ssap_msg_dict, ssap_msg, sibs_info, kp_list, num, clientsock, val_subscriptions):
    """The present method is used to manage the rdf query request received from a KP."""

    t = {}
    global num_confirms
    num_confirms[ssap_msg_dict["node_id"] + "_" + ssap_msg_dict["transaction_id"]] = num
    sib_list_conn = {}

    # debug info
    print treplies_print(True) + " handle_rdf_unsubscribe_request"
    logger.info("RDF UNSUBSCRIBE REQUEST handled by handle_rdf_unsubscribe_request")

    # # find the Subreq instance
    # for s in val_subscriptions:
    #     if str(s.virtual_subscription_id) == str(info["parameter_subscription_id"]):

    clientsock.close()

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
                # forwarding unsubscribe request to the virtual sibs
                sock.send(ssap_msg)
                sock.close()
                
            except socket.error:
                print treplies_print(False) + " Send failed"       
               
        except socket.error:
             print treplies_print(False) + 'Unable to connect to the sibs 1161'
             err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      "UNSUBSCRIBE",
                                                      ssap_msg_dict["transaction_id"],
                                                      '<parameter name="status">m3:Error</parameter>')
             # send a notification error to the KP
             s.conn.send(err_msg)
             s.conn.close() ### TODO: controllare su che porta dobbiamo mandare st'errore
             logger.error("RDF UNSUBSCRIBE REQUEST forwarding failed")
             break
             



                

##############################################################
#
# UTILITIES
#
##############################################################

def reply_to_sparql_query(node_id, space_id, transaction_id, results):

    start = time.time()
    # building HEAD part of the query results
    variable_list = []
    head_template_list = []
    for triple in results:
        for element in triple:    
            
    #         if not SSAP_VARIABLE_TEMPLATE%(str(element[0])) in variable_list:
    #             variable_list.append(SSAP_VARIABLE_TEMPLATE%(str(element[0])))
    # head = SSAP_HEAD_TEMPLATE%(''.join(variable_list))

            if not element[0] in variable_list:
                variable_list.append(element[0])
                head_template_list.append( SSAP_VARIABLE_TEMPLATE % (str(element[0])) )

    head = SSAP_HEAD_TEMPLATE%(''.join(head_template_list))

    # building RESULTS part of the query results
    result_list = []
    result_string = ""

    for triple in results:
        binding_string = ""
        binding_list = []

        for element in triple:    
            binding_list.append(SSAP_BINDING_TEMPLATE%(element[0], element[2]))

        result_list.append(SSAP_RESULT_TEMPLATE%(''.join(binding_list)))

    results_string = SSAP_RESULTS_TEMPLATE%(''.join(result_list))
    body = SSAP_RESULTS_SPARQL_PARAM_TEMPLATE%(head + results_string)

    start = time.time()

    # finalizing the reply
    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(node_id, 
                                    space_id, 
                                    "QUERY",
                                    transaction_id,
                                    body)
    return reply



def reply_to_rdf_query(node_id, space_id, transaction_id, results):

    start = time.time()
    tr_list = []
    for el in results:
        tr_list.append(SSAP_TRIPLE_TEMPLATE%(el[0], el[1], el[2]))
    tr = ''.join(tr_list)
            
    body = SSAP_RESULTS_RDF_PARAM_TEMPLATE%(SSAP_TRIPLE_LIST_TEMPLATE%(tr))
    
    # finalizing the reply
    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(node_id, 
                                    space_id, 
                                    "QUERY",
                                    transaction_id,
                                    body)
    end = time.time() - start
#    print "TEMPO DI COSTRUZIONE MESSAGGIO: " + str(end)
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


def reply_to_sparql_subscribe(node_id, space_id, transaction_id, results, subscription_id):

    start = time.time()
    # building HEAD part of the query results
    variable_list = []
    head_template_list = []
    for triple in results:
        for element in triple:    
            
    #         if not SSAP_VARIABLE_TEMPLATE%(str(element[0])) in variable_list:
    #             variable_list.append(SSAP_VARIABLE_TEMPLATE%(str(element[0])))
    # head = SSAP_HEAD_TEMPLATE%(''.join(variable_list))

            if not element[0] in variable_list:
                variable_list.append(element[0])
                head_template_list.append( SSAP_VARIABLE_TEMPLATE % (str(element[0])) )

    head = SSAP_HEAD_TEMPLATE%(''.join(head_template_list))

    # building RESULTS part of the query results
    result_list = []
    result_string = ""

    for triple in results:
        binding_string = ""
        binding_list = []

        for element in triple:    
            binding_list.append(SSAP_BINDING_TEMPLATE%(element[0], element[2]))

        result_list.append(SSAP_RESULT_TEMPLATE%(''.join(binding_list)))

    results_string = SSAP_RESULTS_TEMPLATE%(''.join(result_list))
    body = SSAP_RESULTS_SPARQL_CONFIRM_TEMPLATE%(subscription_id, head + results_string)
    
    # finalizing the reply
    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(node_id, 
                                    space_id, 
                                    "SUBSCRIBE",
                                    transaction_id,
                                    body)

    return reply


def parse_message(ssap_msg):
    # parse the ssap message
    root = ET.fromstring(ssap_msg)           
    info = {}
    for child in root:
        if child.attrib.has_key("name"):
            k = child.tag + "_" + str(child.attrib["name"])
        else:
            k = child.tag
        info[k] = child.text
    return info


def msg_to_dict(ssap_msg):
    ssap_msg_dict = {}
    parser = make_parser()
    ssap_mh = SSAPMsgHandler(ssap_msg_dict)
    parser.setContentHandler(ssap_mh)
    parser.parse(StringIO(ssap_msg))
    return ssap_msg_dict
