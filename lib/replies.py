#!/usr/bin/python

# requirements
from SSAPLib import *
from termcolor import colored

######################################################
#
# REQUESTS
#
######################################################

def handle_insert_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the insert request received from a KP."""

    # debug info
    print colored("* replies.py: handle_insert_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)


def handle_register_request(conn, info):
    """This method is used to forge and send a reply to the REGISTER
    REQUEST sent by a publisher entity."""

    # debug info
    print colored("* replies.py: handle_register_request", "cyan", attrs=[])

    reply = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "REGISTER",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')
    conn.send(reply)


def handle_join_request(conn, info):
    """This method is used to forge and send a reply to the JOIN
    REQUEST sent by a KP entity."""

    # TODO: this method must be rewritten to forward the join
    # request to all the real SIBs and forge a proper reply.

    reply = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "JOIN",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')
    conn.send(reply)
    if conn in KP_LIST:
        KP_LIST.remove(conn)


def handle_leave_request(conn, info):
    """This method is used to forge and send a reply for the LEAVE
    REQUEST sent by a KP entity."""

    # TODO: this method must be rewritten to forward the leave
    # request to all the real SIBs and forge a proper reply.

    # forge a reply
    reply = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "LEAVE",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')

    # send the reply to the KP
    # TODO: the following code must send the reply only to the
    # kp who sent the request!!!
    conn.send(reply)
    if conn in KP_LIST:
        KP_LIST.remove(conn)


def handle_remove_request(conn, ssap_msg, info, SIB_LIST, KP_LIST):
    """The present method is used to manage the remove request received from a KP."""

    # debug info
    print colored(" * replies.py:handle_remove_request", "cyan", attrs=[])

    # forwarding message to the publishers
    for socket in SIB_LIST:
        try:
            socket.send(ssap_msg)
        except:
            err_msg = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "REMOVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)

    # forwarding message to the publishers
    for socket in SIB_LIST:
        if socket != vsibkp_socket and socket != sock :
            socket.send(ssap_msg)


######################################################
#
# CONFIRMS
#
######################################################

def handle_insert_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST):
    """This method is used to decide what to do once an INSERT CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug info
    print colored(" * replies.py:handle_insert_confirm", "cyan", attrs=[])
    
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
            err_msg = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "INSERT",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)
                            

def handle_remove_confirm(conn, ssap_msg, info, CONFIRMS, KP_LIST):
    """This method is used to decide what to do once an REMOVE CONFIRM
    is received. We can send the confirm back to the KP (if all the
    sibs sent a confirm), decrement a counter (if we are waiting for
    other sibs to reply) or send an error message (if the current
    message or one of the previous replies it's a failure)"""

    # debug info
    print colored(" * replies.py:handle_remove_confirm", "cyan", attrs=[])
        
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
            err_msg = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                             info["space_id"],
                                             "REMOVE",
                                             info["transaction_id"],
                                             '<parameter name="status">m3:Error</parameter>')
            KP_LIST[info["node_id"]].send(err_msg)
                            
