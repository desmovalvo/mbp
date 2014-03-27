#!/usr/bin/python

# requirements
import uuid
from SSAPLib import *
from smart_m3.m3_kp import *
from termcolor import colored
from xml.sax import make_parser

######################################################
#
# REQUESTS
#
######################################################


def handle_join_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the join request received from a KP."""

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "JOIN",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


def handle_leave_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the leave request received from a KP."""

    # debug info
    print colored(" * replies.py: handle_leave_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "JOIN",
                                             "LEAVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


def handle_insert_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the insert request received from a KP."""

    # debug info
    print colored(" * replies.py: handle_insert_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


def handle_register_request(conn, info):
    """This method is used to forge and send a reply to the REGISTER
    REQUEST sent by a publisher entity."""

    # debug info
    print colored(" * replies.py: handle_register_request", "cyan", attrs=[])

    reply = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "REGISTER",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')
    conn.send(reply)


def handle_remove_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the remove request received from a KP."""

    # debug info
    print colored(" * replies.py: handle_remove_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "REMOVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)

def handle_sparql_query_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the sparql query request received from a KP."""

    # debug info
    print colored(" * replies.py: handle_sparql_query_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "QUERY",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


def handle_rdf_query_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the rdf query request received from a KP."""

    # debug info
    print colored(" * replies.py: handle_rdf_query_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "QUERY",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


def handle_rdf_subscription_request(conn, ssap_msg, info, SIB_LIST, KP_LIST, active_subscriptions):
    """The present method is used to manage the rdf subscription request received from a KP."""

    # debug info
    print colored(" * replies.py: handle_rdf_subscription_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            print "inoltro la sub request"
            socket.send(ssap_msg)
        except:
            print "errore"
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "SUBSCRIBE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            
            active_subscriptions[info["node_id"]][info["transaction_id"]]["conn"].send(err_msg)
            
            # remove subscription from active_subscriptions dict
            del active_subscriptions[info["node_id"]][info["transaction_id"]]
            
            if len(active_subscriptions[info["node_id"]].keys) == 0:
                del active_subscriptions[info["node_id"]]

            
######################################################
#
# CONFIRMS
#
######################################################

def handle_join_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST):
    ''' This method forwards the join confirm message to the KP '''
    
    if not CONFIRMS[info["node_id"]] == None:
        
        if info["parameter_status"] == "m3:Success":
            # insert successful
            CONFIRMS[info["node_id"]] -= 1
            if CONFIRMS[info["node_id"]] == 0:    
                KP_LIST[info["node_id"]].send(ssap_msg)
        else:
            # insert failed
            CONFIRMS[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "JOIN",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


def handle_insert_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST):
    """This method is used to decide what to do once an INSERT CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug info
    print colored(" * replies.py: handle_insert_confirm", "cyan", attrs=[])
    
    # check if we already received a failure
    if not CONFIRMS[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":
            CONFIRMS[info["node_id"]] -= 1
            if CONFIRMS[info["node_id"]] == 0:    
                KP_LIST[info["node_id"]].send(ssap_msg)

        # if the current message represent a failure...
        else:
            
            CONFIRMS[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)
                            

def handle_remove_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST, remove_requests):
    """This method is used to decide what to do once an REMOVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug info
    print colored(" * replies.py: handle_remove_confirm", "cyan", attrs=[])
        
    # check if we already received a failure
    if not CONFIRMS[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":

            CONFIRMS[info["node_id"]] -= 1
            if CONFIRMS[info["node_id"]] == 0:    
                remove_requests[info["node_id"]].send(ssap_msg)

                # removing the request
                del remove_requests[info["node_id"]]

        # if the current message represent a failure...
        else:
            
            CONFIRMS[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "REMOVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)

            # removing the request
            del remove_requests[info["node_id"]]
                            

def handle_leave_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST):
    """This method is used to decide what to do once an LEAVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug info
    print colored(" * replies.py: handle_leave_confirm", "cyan", attrs=[])

    # check if we already received a failure
    if not CONFIRMS[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":

            CONFIRMS[info["node_id"]] -= 1
            if CONFIRMS[info["node_id"]] == 0:    
                KP_LIST[info["node_id"]].send(ssap_msg)

        # if the current message represent a failure...
        else:
            
            CONFIRMS[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "LEAVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)

def handle_query_sparql_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST, QUERY_RESULTS):
    """This method is used to manage sparql QUERY CONFIRM received. """

    # debug info
    print colored(" * replies.py: handle_query_confirm", "cyan", attrs=[])
    
    # check if we already received a failure
    if not CONFIRMS[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":
            CONFIRMS[info["node_id"]] -= 1
            # COSTRUIRE LA RISPOSTA: estrarre triple
            
            # convert ssap_msg to dict
            ssap_msg_dict = {}
            parser = make_parser()
            ssap_mh = SSAPMsgHandler(ssap_msg_dict)
            parser.setContentHandler(ssap_mh)
            parser.parse(StringIO(ssap_msg))

            # extract triples from ssap reply
            triple_list = parse_sparql(ssap_msg_dict["results"])
              
            for triple in triple_list:
                QUERY_RESULTS[info["node_id"]].append(triple)
            
            # remove duplicates
            result = []
            for triple in QUERY_RESULTS[info["node_id"]]:
                if not triple in result:
                    result.append(triple)
                    
            QUERY_RESULTS[info["node_id"]] = result
            for r in result:
                print str(r)

            if CONFIRMS[info["node_id"]] == 0:    
                # build ssap reply
                ssap_reply = reply_to_sparql_query(ssap_msg_dict["node_id"],
                                      ssap_msg_dict["space_id"],
                                      ssap_msg_dict["transaction_id"],
                                      result)

                KP_LIST[info["node_id"]].send(ssap_reply)


        # if the current message represent a failure...
        else:
            
            CONFIRMS[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)



def reply_to_sparql_query(node_id, space_id, transaction_id, results):

    print results

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


def handle_query_rdf_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST, QUERY_RESULTS):
    """This method is used to manage rdf QUERY CONFIRM received. """

    # debug info
    print colored(" * replies.py: handle_query_rdf_confirm", "cyan", attrs=[])
    
    # check if we already received a failure
    if not CONFIRMS[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":
            CONFIRMS[info["node_id"]] -= 1
            
            # convert ssap_msg to dict
            ssap_msg_dict = {}
            parser = make_parser()
            ssap_mh = SSAPMsgHandler(ssap_msg_dict)
            parser.setContentHandler(ssap_mh)
            parser.parse(StringIO(ssap_msg))

            # extract triples from ssap reply
            triple_list = parse_M3RDF(ssap_msg_dict["results"])
              
            for triple in triple_list:
                QUERY_RESULTS[info["node_id"]].append(triple)
            
            # remove duplicates
            result = []
            for triple in QUERY_RESULTS[info["node_id"]]:
                if not triple in result:
                    result.append(triple)
                    
            QUERY_RESULTS[info["node_id"]] = result
            for r in result:
                print str(r)

            if CONFIRMS[info["node_id"]] == 0:    
                # build ssap reply
                ssap_reply = reply_to_rdf_query(ssap_msg_dict["node_id"],
                                      ssap_msg_dict["space_id"],
                                      ssap_msg_dict["transaction_id"],
                                      result)

                KP_LIST[info["node_id"]].send(ssap_reply)


        # if the current message represent a failure...
        else:
            
            CONFIRMS[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


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


def handle_subscribe_rdf_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST, INITIAL_RESULTS, subscribe_requests, active_subscriptions):
    """This method is used to manage rdf SUBSCRIBE CONFIRM received. """

    # debug info
    print colored(" * replies.py: handle_subscribe_rdf_confirm", "cyan", attrs=[])
    
    # check if we already received a failure
    if not CONFIRMS[info["node_id"]] == None:

        # check if the current message represent a successful insertion
        if info["parameter_status"] == "m3:Success":
            CONFIRMS[info["node_id"]] -= 1
            
            # convert ssap_msg to dict
            ssap_msg_dict = {}
            parser = make_parser()
            ssap_mh = SSAPMsgHandler(ssap_msg_dict)
            parser.setContentHandler(ssap_mh)
            parser.parse(StringIO(ssap_msg))

            # extract triples from ssap reply
            triple_list = parse_M3RDF(ssap_msg_dict["results"])
              
            for triple in triple_list:
                INITIAL_RESULTS[info["node_id"]].append(triple)
            
            # remove duplicates
            result = []
            for triple in INITIAL_RESULTS[info["node_id"]]:
                if not triple in result:
                    result.append(triple)
                    
            INITIAL_RESULTS[info["node_id"]] = result
            for r in result:
                print str(r)

            if CONFIRMS[info["node_id"]] == 0:    
                # generate a random subscription_id
                active_subscriptions[info["node_id"]][info["transaction_id"]]["subscription_id"] = uuid.uuid4()
                # build ssap reply
                ssap_reply = reply_to_rdf_subscribe(ssap_msg_dict["node_id"],
                                                    ssap_msg_dict["space_id"],
                                                    ssap_msg_dict["transaction_id"],
                                                    result,
                                                    active_subscriptions[info["node_id"]][info["transaction_id"]]["subscription_id"])

                active_subscriptions[info["node_id"]][info["transaction_id"]]["conn"].send(ssap_reply)


        # if the current message represent a failure...
        else:
            
            CONFIRMS[info["node_id"]] = None
            # send SSAP ERROR MESSAGE
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "SUBSCRIBE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')

            active_subscriptions[info["node_id"]][info["transaction_id"]]["conn"].send(err_msg)
                

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
