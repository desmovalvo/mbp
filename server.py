#!/usr/bin/python
 
import socket, select
from termcolor import colored
from xml.etree import ElementTree as ET

SSAP_MESSAGE_TEMPLATE = '''<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>%s</transaction_type>
<message_type>CONFIRM</message_type>
<transaction_id>%s</transaction_id>
%s
</SSAP_message>'''
 
def reply_to_sib(conn, info):
    reply = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "REGISTER",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')
    conn.send(reply)


def reply_to_join(conn, info):
    reply = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "JOIN",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')
    conn.send(reply)
    if conn in KP_LIST:
        KP_LIST.remove(conn)


def reply_to_leave(conn, info):
    reply = SSAP_MESSAGE_TEMPLATE%(info["node_id"],
                                   info["space_id"],
                                   "LEAVE",
                                   info["transaction_id"],
                                   '<parameter name="status">m3:Success</parameter>')
    conn.send(reply)
    if conn in KP_LIST:
        KP_LIST.remove(conn)


def reply_to_insert_confirm(conn, ssap_msg):
    for kp in KP_LIST:
        kp.send(ssap_msg)

def reply_to_insert(conn, ssap_msg):

    # forwarding message to the publishers
    print "Lista SIB"
    for socket in SIB_LIST:
        print str(socket)
    print "Lista KP"
    for socket in KP_LIST:
        print str(socket)
    print str(conn)

    for socket in SIB_LIST:
        if socket != vsibkp_socket and socket != sock :
            socket.send(ssap_msg)

    # TODO: reply to the kp. We disabled the reply in the SibLib class
    # to avoid a crash due to incomplete message


def reply_to_remove(conn, ssap_msg):

    # forwarding message to the publishers
    for socket in SIB_LIST:
        if socket != vsibkp_socket and socket != sock :
            socket.send(ssap_msg)

    # TODO: reply to the kp. We disabled the reply in the SibLib class
    # to avoid a crash due to incomplete message

 
if __name__ == "__main__":
     
    # List to keep track of socket descriptors
    CONNECTION_LIST = []
    SIB_LIST = []
    KP_LIST = []
    RECV_BUFFER = 1024 # Advisable to keep it as an exponent of 2
    KP_PORT = 10010 # On this port we expect connections from the KPs
    PUB_PORT = 10011 # On this port we receive connections from the publishers

    # configuring the socket dedicated to the kps     
    vsibkp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vsibkp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    vsibkp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    vsibkp_socket.bind(("0.0.0.0", KP_PORT))
    vsibkp_socket.listen(10)
    
    # configuring the socket dedicated to the publishers
    vsibpub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vsibpub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    vsibpub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    vsibpub_socket.bind(("0.0.0.0", PUB_PORT))
    vsibpub_socket.listen(10)    
 
    # Add server socket to the list of readable connections
    CONNECTION_LIST.append(vsibkp_socket)
    CONNECTION_LIST.append(vsibpub_socket)
 
    print "VSIB started on port " + str(KP_PORT)
    print "VSIB publisher interface started on port " + str(PUB_PORT)
 
    while 1:
        # Get the list sockets which are ready to be read through select
        read_sockets,write_sockets,error_sockets = select.select(CONNECTION_LIST,[],[])

        for sock in read_sockets:
            # New connection
            if sock in [vsibkp_socket, vsibpub_socket]:
                # Handle the case in which there is a new connection received through vsibkp_socket
                conn, addr = sock.accept()
                print str(type(conn))
                CONNECTION_LIST.append(conn)
                print "Client (%s, %s) connected" % addr
                # broadcast_data(conn, "[%s:%s] entered room\n" % addr)
             
            # Some incoming message from a client
            else:

                # Ssap_Msg received from client, process it
                try:
                    ssap_msg = sock.recv(RECV_BUFFER)

                    # parse the ssap message
                    root = ET.fromstring(ssap_msg)
                    info = {}
                    for child in root:
                        if child.attrib.has_key("name"):
                            k = child.tag + "_" + str(child.attrib["name"])
                        else:
                            k = child.tag
                            info[k] = child.text
                    print "Received a %s %s"%(info["transaction_type"], info["message_type"])

                    # check the type of message

                    # check if we have to register a new SIB
                    if info["message_type"] == "REQUEST" and info["transaction_type"] == "REGISTER":
                        SIB_LIST.append(conn)
                        reply_to_sib(conn, info)
                        
                    # check whether we have to register a new KP
                    elif info["message_type"] == "REQUEST" and info["transaction_type"] == "JOIN":
                        KP_LIST.append(conn)
                        reply_to_join(conn, info)

                    # check whether we have to delete a KP
                    elif info["message_type"] == "REQUEST" and info["transaction_type"] == "LEAVE":
                        KP_LIST.append(conn)
                        reply_to_leave(conn, info)

                    # check whether it's an INSERT request
                    elif info["message_type"] == "REQUEST" and info["transaction_type"] == "INSERT":
                        KP_LIST.append(conn)
                        reply_to_insert(conn, ssap_msg)

                    # check whether it's an REMOVE request
                    elif info["message_type"] == "REQUEST" and info["transaction_type"] == "REMOVE":
                        reply_to_remove(conn, ssap_msg)

                    # check whether it's an INSERT confirm
                    elif info["message_type"] == "CONFIRM" and info["transaction_type"] == "INSERT":
                        reply_to_insert_confirm(conn, ssap_msg)
                 
                except:
                    print "Client (%s, %s) is offline" % addr
                    # sock.close()
                    CONNECTION_LIST.remove(sock)
                    continue
     
    vsibkp_socket.close()
    vsibpub_socket.close()
