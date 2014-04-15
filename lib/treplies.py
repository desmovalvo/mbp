#!/usr/bin/python

# requirements
import sys
import struct
from SSAPLib import *
from termcolor import *
from lib.Subreq import *
from smart_m3.m3_kp import *
from xml.sax import make_parser


##############################################################
#
# REQUESTS
#
##############################################################

# REGISTER REQUEST
def handle_register_request(logger, conn, info):
    """This method is used to forge and send a reply to the REGISTER
    REQUEST sent by a publisher entity."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_register_request"
    logger.info("REGISTER REQUEST handled by handle_register_request")

    # build a reply message
    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "REGISTER",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')
    
    # try to receive, then return
    try:
        conn.send(reply)
        return True
    except socket.error:
        logger.error("REGISTER CONFIRM not sent!")
        return False


# JOIN REQUEST
def handle_join_request(logger, info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the join request received from a KP."""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_join_request"
    logger.info("JOIN REQUEST handled by handle_join_request")

    # check the number of connected real sibs
    if len(sib_list) > 0:
        
        # forwarding message to the publishers
        for sock in sib_list:
            try:
                sock.send(ssap_msg)
            except socket.error:
                
                # debug print
                print colored('treplies> ', "red", attrs=["bold"]) + " socket error while forwarding a join request"

                # send a notification error to the KP
                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                         info["space_id"],
                                                         "JOIN",
                                                         info["transaction_id"],
                                                         '<parameter name="status">m3:Error</parameter>')            
                kp_list[info["node_id"]].send(err_msg)
                del kp_list[info["node_id"]]
                logger.error("JOIN REQUEST forwarding failed")

    else:
        
        # no sib connected, sending an error reply
        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                 info["space_id"],
                                                 "JOIN",
                                                 info["transaction_id"],
                                                 '<parameter name="status">m3:Error</parameter>')

        # send a notification error to the KP
        kp_list[info["node_id"]].send(err_msg)
        del kp_list[info["node_id"]]
        print colored("treplies> ", "red", attrs=["bold"]), + "error while forwarding a JOIN REQUEST. No real sib present."
        logger.error("JOIN REQUEST failed: no real sib connected")


# LEAVE REQUEST
def handle_leave_request(logger, info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the leave request received from a KP."""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_leave_request"
    logger.info("LEAVE REQUEST handled by handle_leave_request")

    # check the number of connected real sibs
    if len(sib_list) > 0:
            
        # forwarding message to the publishers    
        for sock in sib_list:    
            try:    
                sock.send(ssap_msg)    
            except socket.error:    
                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],    
                                                 info["space_id"],    
                                                 "LEAVE",    
                                                 info["transaction_id"],    
                                                 '<parameter name="status">m3:Error</parameter>')    
                kp_list[info["node_id"]].send(err_msg)    
                logger.error("LEAVE REQUEST forwarding failed")    

    # no real sib present
    else:
        # build and send an error message
        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],    
                                                 info["space_id"],    
                                                 "LEAVE",    
                                                 info["transaction_id"],    
                                                 '<parameter name="status">m3:Error</parameter>')    
        kp_list[info["node_id"]].send(err_msg)    
        print colored("treplies> ", "red", attrs=["bold"]) + "error while forwarding a LEAVE REQUEST. No real sib present."
        logger.error("LEAVE REQUEST forwarding failed: no real sib present")    
    

# INSERT REQUEST
def handle_insert_request(logger, info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the insert request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_insert_request"
    logger.info("INSERT REQUEST handled by handle_insert_request")

    # check the number of connected real sibs
    if len(sib_list) > 0:

        # forwarding message to the publishers
        for sock in sib_list:
            try:
                sock.send(ssap_msg)
            except socket.error:
                err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                 info["space_id"],
                                                 "INSERT",
                                                 info["transaction_id"],
                                                 '<parameter name="status">m3:Error</parameter>')
                kp_list[info["node_id"]].send(err_msg)
                logger.error("INSERT REQUEST forwarding failed")

    # no real sib present
    else:
        # build and send an error message
        err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                 info["space_id"],
                                                 "INSERT",
                                                 info["transaction_id"],
                                                 '<parameter name="status">m3:Error</parameter>')    
        kp_list[info["node_id"]].send(err_msg)    
        print colored("treplies> ", "red", attrs=["bold"]) + "error while forwarding a INSERT REQUEST. No real sib present."
        logger.error("INSERT REQUEST forwarding failed: no real sib present")    
    


# REMOVE REQUEST
def handle_remove_request(logger, info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the remove request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_remove_request"
    logger.info("REMOVE REQUEST handled by handle_remove_request")

    # forwarding message to the publishers
    for sock in sib_list:
        try:
            sock.send(ssap_msg)
        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "REMOVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            logger.error("REMOVE REQUEST forwarding failed")

# SPARQL QUERY REQUEST
def handle_sparql_query_request(logger, info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the sparql query request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_query_request"
    logger.info("SPARQL QUERY REQUEST handled by handle_sparql_query_request")

    # forwarding message to the publishers
    for sock in sib_list:
        try:
            sock.send(ssap_msg)
        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "QUERY",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            logger.error("SPARQL QUERY REQUEST forwarding failed")


# RDF QUERY REQUEST
def handle_rdf_query_request(logger, info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the rdf query request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_query_request"
    logger.info("RDF QUERY REQUEST handled by handle_rdf_query_request")

    # forwarding message to the publishers
    for sock in sib_list:
        try:
            sock.send(ssap_msg)
        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "QUERY",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            logger.error("RDF QUERY REQUEST forwarding failed")

# RDF SUBSCRIBE REQUEST
def handle_sparql_subscribe_request(logger, info, ssap_msg, sib_list, kp_list, clientsock, val_subscriptions):
    """The present method is used to manage the rdf query request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_subscribe_request"
    logger.info("SPARQL SUBSCRIBE REQUEST handled by handle_sparql_subscribe_request")

    # generating a Subreq instance
    newsub = Subreq(clientsock, info["node_id"], info["transaction_id"])
    val_subscriptions.append(newsub)

    # convert ssap_msg to dict
    ssap_msg_dict = {}
    parser = make_parser()
    ssap_mh = SSAPMsgHandler(ssap_msg_dict)
    parser.setContentHandler(ssap_mh)
    parser.parse(StringIO(ssap_msg))        

    # forwarding message to the publishers
    for sock in sib_list:
        try:
            
            pars = '<parameter name = "type">sparql</parameter><parameter name = "query">' + ssap_msg_dict["query"] + '</parameter><virtual_subscription_id>' + str(newsub.virtual_subscription_id) + '</virtual_subscription_id>'

            ssap_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "SUBSCRIBE",
                                                      info["transaction_id"],
                                                      pars)
            sock.send(ssap_msg)

        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                     info["space_id"],
                                                     "SUBSCRIBE",
                                                     info["transaction_id"],
                                                     '<parameter name="status">m3:Error</parameter>')
            newsub.conn.send(err_msg)
            #TODO delete class!
            
            logger.error("SPARQL SUBSCRIBE REQUEST forwarding failed")


# RDF SUBSCRIBE REQUEST
def handle_rdf_subscribe_request(logger, info, ssap_msg, sib_list, kp_list, clientsock, val_subscriptions):
    """The present method is used to manage the rdf query request received from a KP."""

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

    # forwarding message to the publishers
    for sock in sib_list:
        try:
            
            pars = '<parameter name = "type">RDF-M3</parameter><parameter name = "query">' + ssap_msg_dict["query"] + '</parameter><virtual_subscription_id>' + str(newsub.virtual_subscription_id) + '</virtual_subscription_id>'

            ssap_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(info["node_id"],
                                                      info["space_id"],
                                                      "SUBSCRIBE",
                                                      info["transaction_id"],
                                                      pars
                                                      )

            sock.send(ssap_msg)

        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "SUBSCRIBE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            newsub.conn.send(err_msg)
            #TODO delete class!
            
            logger.error("RDF SUBSCRIBE REQUEST forwarding failed")

# RDF UNSUBSCRIBE REQUEST
def handle_rdf_unsubscribe_request(logger, info, ssap_msg, sib_list, kp_list, clientsock, val_subscriptions):
    """The present method is used to manage the rdf query request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_unsubscribe_request"
    logger.info("RDF UNSUBSCRIBE REQUEST handled by handle_rdf_unsubscribe_request")

    # find the Subreq instance
    for s in val_subscriptions:
        if str(s.virtual_subscription_id) == str(info["parameter_subscription_id"]):

            # forwarding message to the publishers
            for sock in sib_list:
                try:
                    # send the message
                    sock.send(ssap_msg)                
                except socket.error:
                    err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                                     info["space_id"],
                                                     "UNSUBSCRIBE",
                                                     info["transaction_id"],
                                                     '<parameter name="status">m3:Error</parameter>')
                    s.conn.send(err_msg)
                    
                    logger.error("RDF UNSUBSCRIBE REQUEST forwarding failed")

            break
            
        

##############################################################
#
# confirms
#
##############################################################

# JOIN CONFIRM
def handle_join_confirm(logger, conn, info, ssap_msg, confirms, kp_list):
    ''' This method forwards the join confirm message to the KP '''
    
    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_join_confirm"
    logger.info("JOIN CONFIRM handled by handle_join_confirm")

    if not confirms[info["node_id"]] == None:
        
        if info["parameter_status"] == "m3:Success":
            # insert successful
            confirms[info["node_id"]] -= 1
            if confirms[info["node_id"]] == 0:    
                kp_list[info["node_id"]].send(ssap_msg)
                kp_list[info["node_id"]].close()
         
        else:
            # insert failed
            confirms[info["node_id"]] = None
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


# LEAVE CONFIRM
def handle_leave_confirm(logger, info, ssap_msg, confirms, kp_list):
    """This method is used to decide what to do once an LEAVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_leave_confirm"
    logger.info("LEAVE CONFIRM handled by handle_leave_confirm")

    # check if we already received a failure
    if not confirms[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":

            confirms[info["node_id"]] -= 1
            if confirms[info["node_id"]] == 0:    
                kp_list[info["node_id"]].send(ssap_msg)
                kp_list[info["node_id"]].close()
                del kp_list[info["node_id"]]
            
        # if the current message represent a failure...
        else:
            
            confirms[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "LEAVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            kp_list[info["node_id"]].close()

            logger.error("LEAVE CONFIRM forwarding failed")


# INSERT CONFIRM
def handle_insert_confirm(logger, info, ssap_msg, confirms, kp_list):
    """This method is used to decide what to do once an INSERT CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_insert_confirm"
    logger.info("INSERT CONFIRM handled by handle_insert_confirm")
    
    # check if we already received a failure
    if not confirms[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":
            confirms[info["node_id"]] -= 1
            if confirms[info["node_id"]] == 0:    
                kp_list[info["node_id"]].send(ssap_msg)
                kp_list[info["node_id"]].close()

        # if the current message represent a failure...
        else:
            
            confirms[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            kp_list[info["node_id"]].close()
            
            logger.error("INSERT CONFIRM forwarding failed")


# REMOVE CONFIRM
def handle_remove_confirm(logger, info, ssap_msg, confirms, kp_list):
    """This method is used to decide what to do once an REMOVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_remove_confirm"
    logger.info("REMOVE CONFIRM handled by handle_remove_confirm")
        
    # check if we already received a failure
    if not confirms[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":

            confirms[info["node_id"]] -= 1
            if confirms[info["node_id"]] == 0:    
                kp_list[info["node_id"]].send(ssap_msg)
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


# SPARQL QUERY CONFIRM
def handle_sparql_query_confirm(logger, info, ssap_msg, confirms, kp_list, query_results):
    """This method is used to manage sparql QUERY CONFIRM received. """

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_query_confirm"
    logger.info("SPARQL QUERY CONFIRM handled by handle_sparql_query_confirm")
            
    # check if we already received a failure
    if not confirms[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":
            confirms[info["node_id"]] -= 1
            
            # convert ssap_msg to dict
            ssap_msg_dict = {}
            parser = make_parser()
            ssap_mh = SSAPMsgHandler(ssap_msg_dict)
            parser.setContentHandler(ssap_mh)
            parser.parse(StringIO(ssap_msg))

            # extract triples from ssap reply
            triple_list = parse_sparql(ssap_msg_dict["results"])
              
            for triple in triple_list:
                if not triple in query_results[info["node_id"]]:
                    query_results[info["node_id"]].append(triple)

            if confirms[info["node_id"]] == 0:    
                # build ssap reply
                ssap_reply = reply_to_sparql_query(ssap_msg_dict["node_id"],
                                      ssap_msg_dict["space_id"],
                                      ssap_msg_dict["transaction_id"],
                                      result)

                kp_list[info["node_id"]].send(ssap_reply)
                kp_list[info["node_id"]].close()


        # if the current message represent a failure...
        else:
            
            confirms[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "QUERY",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            kp_list[info["node_id"]].close()
            logger.error("SPARQL CONFIRM forwarding failed")


# RDF QUERY CONFIRM
def handle_rdf_query_confirm(logger, info, ssap_msg, confirms, kp_list, query_results):
    """This method is used to manage rdf QUERY CONFIRM received. """

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_query_confirm"
    logger.info("RDF QUERY CONFIRM handled by handle_rdf_query_confirm")
    
    # check if we already received a failure
    if not confirms[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":
            confirms[info["node_id"]] -= 1
            
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

            if confirms[info["node_id"]] == 0:    
                # build ssap reply
                ssap_reply = reply_to_rdf_query(ssap_msg_dict["node_id"],
                                      ssap_msg_dict["space_id"],
                                      ssap_msg_dict["transaction_id"],
                                      result)

                kp_list[info["node_id"]].send(ssap_reply)
                kp_list[info["node_id"]].close()


        # if the current message represent a failure...
        else:
            
            confirms[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "QUERY",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            kp_list[info["node_id"]].close()
            logger.error("RDF QUERY CONFIRM forwarding failed")


# SPARQL SUBSCRIBE CONFIRM
def handle_sparql_subscribe_confirm(logger, info, ssap_msg, confirms, kp_list, initial_results, active_subscriptions, clientsock, val_subscriptions):
    """This method is used to manage sparql SUBSCRIBE CONFIRM received. """

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_subscribe_confirm"
    logger.info("SPARQL SUBSCRIBE CONFIRM handled by handle_sparql_subscribe_confirm")

    # check if we already received a failure
    if not confirms[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":

            confirms[info["node_id"]] -= 1

            # store the corrispondence between the real sib and the real_subscription_id
            for s in val_subscriptions:                              
                
                if s.node_id == info["node_id"] and s.request_transaction_id == info["transaction_id"]:
                    subreq_instance = s
                
                    # convert ssap_msg to dict
                    ssap_msg_dict = {}
                    parser = make_parser()
                    ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                    parser.setContentHandler(ssap_mh)
                    parser.parse(StringIO(ssap_msg))
        
                    # extract triples from ssap reply
                    triple_list = parse_sparql(ssap_msg_dict["results"])
                      
                    for triple in triple_list:
                        if triple not in initial_results[info["node_id"]]:
                            initial_results[info["node_id"]].append(triple)
                    
                    # remove duplicates
                    for triple in initial_results[info["node_id"]]:
                        if not triple in s.result:
                            s.result.append(triple)
                            
                    initial_results[info["node_id"]] = s.result

                    print 'Time to build the reply'
        
                    if confirms[info["node_id"]] == 0:    

                        print ssap_msg_dict.keys()
                        
                        # build ssap reply                                        
                        ssap_reply = reply_to_sparql_subscribe(ssap_msg_dict["node_id"], 
                                                               ssap_msg_dict["space_id"], 
                                                               ssap_msg_dict["transaction_id"], 
                                                               ssap_msg_dict["subscription_id"],
                                                               s.result)
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

            # decrement the number of waited confirms
            confirms[info["node_id"]] -= 1

            # store the corrispondence between the real sib and the real_subscription_id
            for s in val_subscriptions:                              
                
                # we found the desired Subreq instance
                if s.node_id == info["node_id"] and s.request_transaction_id == info["transaction_id"]:
                    subreq_instance = s
                
                    # convert ssap_msg to dict
                    ssap_msg_dict = {}
                    parser = make_parser()
                    ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                    parser.setContentHandler(ssap_mh)
                    parser.parse(StringIO(ssap_msg))
        
                    # extract triples from ssap reply and remove the duplicates
                    triple_list = parse_M3RDF(ssap_msg_dict["results"])                      
                    for triple in triple_list:
                        if not triple in s.result:
                            s.result.append(triple)
                    initial_results[info["node_id"]] = s.result
        
                    # check if it's time to send the confirm
                    if confirms[info["node_id"]] == 0:    

                        # build ssap reply                
                        ssap_reply = reply_to_rdf_subscribe(ssap_msg_dict["node_id"],
                                                            ssap_msg_dict["space_id"],
                                                            ssap_msg_dict["transaction_id"],
                                                            s.result,
                                                            subreq_instance.virtual_subscription_id)                        

                        # send the ssap reply
                        subreq_instance.conn.send(ssap_reply)
                        
        # if the current message represent a failure...
        else:

            # delete the subscription from the list of subscriptions and send an error message            

            confirms[info["node_id"]] = None
            err_msg = SSAP_MESSAGE_SUBSCRIBE_CONFIRM_TEMPLATE%(info["node_id"],
                                                               info["space_id"],
                                                               "SUBSCRIBE",
                                                               info["transaction_id"],
                                                               info["parameter_subscription_id"],
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

def handle_rdf_subscribe_indication(logger, ssap_msg, info, fromsocket, val_subscriptions):

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_rdf_subscribe_indication"
    logger.info("RDF SUBSCRIBE INDICATION handled by handle_subscribe_indication")

    # lock
    print "Acquiring lock"
    lock = threading.Lock()

    for s in val_subscriptions:
        if str(s.virtual_subscription_id) == str(info["parameter_subscription_id"]):

            # send the message to the kp
            print "Inoltro la indication"
            print ssap_msg
            try:
                s.conn.send(ssap_msg)
            except socket.error:
                print "inoltro indication fallito"
            
            break

    print "Releasing the lock"
    lock.release


def handle_sparql_subscribe_indication(logger, ssap_msg, info, fromsocket, val_subscriptions):

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_subscribe_indication"
    logger.info("SPARQL SUBSCRIBE INDICATION handled by handle_subscribe_indication")

    # lock
    print "Acquiring lock"
    lock = threading.Lock()

    for s in val_subscriptions:
        if str(s.virtual_subscription_id) == str(info["parameter_subscription_id"]):

            # send the message to the kp
            print "Inoltro la indication"
            try:
                s.conn.send(ssap_msg)
            except socket.error:
                print "inoltro indication fallito"
            
            break                

    print "Releasing the lock"
    lock.release


##############################################################
#
# UTILITIES
#
##############################################################

def reply_to_sparql_query(node_id, space_id, transaction_id, results):

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


# The following method is used to build a reply to the client sparql subscribe request
def reply_to_sparql_subscribe(node_id, space_id, transaction_id, subscription_id, results):

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
            if element[1] == "uri":
                binding_string = binding_string + SSAP_BINDING_URI_TEMPLATE%(element[0], element[2])
            elif element[1] == "literal":
                binding_string = binding_string + SSAP_BINDING_LITERAL_TEMPLATE%(element[0], element[2])

        result_string = result_string + SSAP_RESULT_TEMPLATE%(binding_string)
    results_string = SSAP_RESULTS_TEMPLATE%(result_string)
    body = SSAP_RESULTS_SPARQL_PARAM_TEMPLATE%(head + results_string)

    # finalizing the reply
    reply = SSAP_MESSAGE_SUBSCRIBE_CONFIRM_TEMPLATE%(node_id,
                                                     space_id, 
                                                     "SUBSCRIBE",
                                                     transaction_id,
                                                     subscription_id,
                                                     body)
    return reply
