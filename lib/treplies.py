#!/usr/bin/python

# requirements
from SSAPLib import *
from termcolor import *
from smart_m3.m3_kp import *
from xml.sax import make_parser


##############################################################
#
# REQUESTS
#
##############################################################

# REGISTER REQUEST
def handle_register_request(conn, info):
    """This method is used to forge and send a reply to the REGISTER
    REQUEST sent by a publisher entity."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_register_request"

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
        return False


# JOIN REQUEST
def handle_join_request(info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the join request received from a KP."""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_join_request"

    # forwarding message to the publishers
    for socket in sib_list:
        try:
            socket.send(ssap_msg)
        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "JOIN",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            # send a notification error to the KP
            kp_list[info["node_id"]].send(err_msg)
            del kp_list[info["node_id"]]


# LEAVE REQUEST
def handle_leave_request(info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the leave request received from a KP."""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_leave_request"

    # forwarding message to the publishers
    for socket in sib_list:
        try:
            socket.send(ssap_msg)
        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "LEAVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)


# INSERT REQUEST
def handle_insert_request(info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the insert request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_insert_request"

    # forwarding message to the publishers
    for socket in sib_list:
        try:
            socket.send(ssap_msg)
        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)


# REMOVE REQUEST
def handle_remove_request(info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the remove request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_remove_request"

    # forwarding message to the publishers
    for socket in sib_list:
        try:
            socket.send(ssap_msg)
        except socket.error:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "REMOVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)

# SPARQL QUERY REQUEST
def handle_sparql_query_request(info, ssap_msg, sib_list, kp_list):
    """The present method is used to manage the sparql query request received from a KP."""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_query_request"

    # forwarding message to the publishers
    for socket in sib_list:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "QUERY",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)


##############################################################
#
# confirms
#
##############################################################

# JOIN CONFIRM
def handle_join_confirm(conn, info, ssap_msg, confirms, kp_list):
    ''' This method forwards the join confirm message to the KP '''
    
    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_join_confirm"

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


# LEAVE CONFIRM
def handle_leave_confirm(info, ssap_msg, confirms, kp_list):
    """This method is used to decide what to do once an LEAVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug info
    print colored("treplies>", "green", attrs=["bold"]) + " handle_leave_confirm"

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


# INSERT CONFIRM
def handle_insert_confirm(info, ssap_msg, confirms, kp_list):
    """This method is used to decide what to do once an INSERT CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_insert_confirm"
    
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


# REMOVE CONFIRM
def handle_remove_confirm(info, ssap_msg, confirms, kp_list):
    """This method is used to decide what to do once an REMOVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_remove_confirm"
        
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


def handle_sparql_query_confirm(info, ssap_msg, confirms, kp_list, query_results):
    """This method is used to manage sparql QUERY CONFIRM received. """

    # debug message
    print colored("treplies>", "green", attrs=["bold"]) + " handle_sparql_query_confirm"
            
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
                query_results[info["node_id"]].append(triple)
            
            # remove duplicates
            result = []
            for triple in query_results[info["node_id"]]:
                if not triple in result:
                    result.append(triple)
                    
            query_results[info["node_id"]] = result
            for r in result:
                print str(r)

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
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            kp_list[info["node_id"]].send(err_msg)
            kp_list[info["node_id"]].close()


##############################################################
#
# INDICATIONS
#
##############################################################



##############################################################
#
# UTILITIES
#
##############################################################

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
