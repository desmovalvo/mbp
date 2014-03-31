#!/usr/bin/python

# requirements
from SSAPLib import *
from termcolor import *


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
    print colored("treplies> ", "green", attrs=["bold"]) + " handle_register_request"

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
    print colored("treplies> ", "green", attrs=["bold"]) + " handle_join_request"

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
    print colored("treplies> ", "green", attrs=["bold"]) + " handle_leave_request"

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


##############################################################
#
# confirms
#
##############################################################

# JOIN CONFIRM
def handle_join_confirm(conn, info, ssap_msg, confirms, kp_list):
    ''' This method forwards the join confirm message to the KP '''
    
    # debug info
    print colored("treplies> ", "green", attrs=["bold"]) + " handle_join_confirm"

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
    print colored("treplies> ", "green", attrs=["bold"]) + " handle_leave_confirm"

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


##############################################################
#
# INDICATIONS
#
##############################################################
