#!/usr/bin/python

######################################################
#
# REQUESTS
#
######################################################

def handle_register_request(conn, info):
    """This method is used to forge and send a reply to the REGISTER
    REQUEST sent by a publisher entity."""

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


def handle_insert_request(conn, ssap_msg):

    # TODO: this method must be rewritten in order to 
    # check whether the request reaches the publishers
    # and to eventually build a proper reply

    # forwarding message to the publishers
    for socket in SIB_LIST:
        if socket != vsibkp_socket and socket != sock :
            socket.send(ssap_msg)


def handle_remove_request(conn, ssap_msg):

    # TODO: this method must be rewritten in order to 
    # check whether the request reaches the publishers
    # and to eventually build a proper reply

    # forwarding message to the publishers
    for socket in SIB_LIST:
        if socket != vsibkp_socket and socket != sock :
            socket.send(ssap_msg)


######################################################
#
# CONFIRMS
#
######################################################

def handle_insert_confirm(conn, ssap_msg):
    """This method is used to forge and send a reply for the LEAVE
    REQUEST sent by a KP entity."""
    for kp in KP_LIST:
        kp.send(ssap_msg)
