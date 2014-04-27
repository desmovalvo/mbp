#!/usr/bin/python

# requirements
from SSAPLib import *

# constants
BUFFER_SIZE = 1024

####################################################
#
# JOIN request made by a KP
#
####################################################

def manage_join_request(kps, conn, addr, info):

    # check if the registrant KPs is already present
    if not kps.has_key(info["node_id"]):
        kps[info["node_id"]] = {}
        
    # fill / update sib's data
    kps[info["node_id"]]["conn"] = conn
    kps[info["node_id"]]["addr"] = addr

    # building a proper reply
    reply_msg = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                       info["space_id"],
                                       "JOIN",
                                       info["transaction_id"],
                                       '<parameter name="status">m3:Success</parameter>')
    
    # sending a reply
    kps[info["node_id"]]["conn"].send(reply_msg)


####################################################
#
# LEAVE request made by a KP
#
####################################################    

def manage_leave_request(kps, conn, addr, info):
    
    # check if the registrant KPs is effectively present
    if kps.has_key(info["node_id"]):           
        
        # delete sib's data
        del kps[info["node_id"]]

        # building a proper reply
        reply_msg = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                           info["space_id"],
                                           "LEAVE",
                                           info["transaction_id"],
                                           '<parameter name="status">m3:Success</parameter>')
            
        # sending a reply
        conn.send(reply_msg)

####################################################
#
# INSERT confirm made by a publisher
#
####################################################

def manage_insert_confirm(sibs, kps, conn, addr, info, ssap_msg):

    # print "SONO QUIIIIIIIIIIIIIIIIIIIIII"

    # # send the reply to the requiring kp
    # conn.send(ssap_msg)    
    pass

####################################################
#
# INSERT request made by a KP
#
####################################################

def manage_insert_request(sibs, kps, conn, addr, info, ssap_msg):

    # forward the insert request to all the publishers
    for pub in sibs.keys():
        print str(type(sibs[pub]["conn"]))
        sibs[pub]["conn"].send(ssap_msg)

####################################################
#
# REGISTER request made by a SIB
#
####################################################

def manage_register_request(sibs, conn, addr, info):

    # check if the registrant SIBs is already present
    if not sibs.has_key(info["node_id"]):
        sibs[info["node_id"]] = {}
        
    # fill / update sib's data
    sibs[info["node_id"]]["conn"] = conn
    sibs[info["node_id"]]["addr"] = addr
    sibs[info["node_id"]]["status"] = "ONLINE"

    # building a proper reply
    reply_msg = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                       info["space_id"],
                                       "REGISTER",
                                       info["transaction_id"],
                                       '<parameter name="status">m3:Success</parameter>')

    # sending a reply
    conn.send(reply_msg)

