# requirements
import sys
from xml.etree import ElementTree as ET
import socket, select, string, sys
from termcolor import colored
import random
import uuid
from lib.SSAPLib import *
import threading
import thread
from termcolor import *
from smart_m3.m3_kp import *
from xml.sax import make_parser


subscriptions = {}

def handler(sock, ssap_msg):
    print colored("tpublisher> ", "blue", attrs=["bold"]) + "started a thread"

    # socket to the real SIB
    rs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rs.connect((realsib_host, realsib_port))

    # forward the message to the real SIB
    if not "<transaction_type>REGISTER</transaction_type>" in ssap_msg:


        if ("<transaction_type>SUBSCRIBE</transaction_type>" in ssap_msg and "<message_type>REQUEST</message_type>"):
     
            # convert ssap_msg to dict
            ssap_msg_dict = {}
            parser = make_parser()
            ssap_mh = SSAPMsgHandler(ssap_msg_dict)
            parser.setContentHandler(ssap_mh)
            parser.parse(StringIO(ssap_msg))        

            # store the virtual subscription id
            subscriptions[ssap_msg_dict["virtual_subscription_id"]] = None
            
            # build subscribe request message       
            if ssap_msg_dict["type"] == "sparql":
                pars = '<parameter name = "type">sparql</parameter><parameter name = "query">' + ssap_msg_dict["query"] + '</parameter>'
            elif ssap_msg_dict["type"] == "RDF-M3":
                pars = '<parameter name = "type">RDF-M3</parameter><parameter name = "query">' + ssap_msg_dict["query"] + '</parameter>'

            ssap_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      "SUBSCRIBE",
                                                      ssap_msg_dict["transaction_id"],
                                                      pars
                                                      )

            # send to the real sib
            rs.send(ssap_msg)
     
            # start a new thread to handle it
            if ssap_msg_dict["type"] == "RDF-M3":
                thread.start_new_thread(rdf_subscription_handler, (rs, vs, ssap_msg_dict["virtual_subscription_id"]))
            elif ssap_msg_dict["type"] == "sparql":
                thread.start_new_thread(sparql_subscription_handler, (rs, vs, ssap_msg_dict["virtual_subscription_id"]))

        elif ("<transaction_type>UNSUBSCRIBE</transaction_type>" in ssap_msg and "<message_type>REQUEST</message_type>"):
            # convert ssap_msg to dict
            ssap_msg_dict = {}
            parser = make_parser()
            ssap_mh = SSAPMsgHandler(ssap_msg_dict)
            parser.setContentHandler(ssap_mh)
            parser.parse(StringIO(ssap_msg))        

            # get real subscription id from subscriptions structure
            subscription_id = subscriptions[ssap_msg_dict["subscription_id"]]
            
            pars = '<parameter name = "subscription_id">' + subscription_id + '</parameter>'

            # build unsubscribe request with real subscription id
            ssap_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(ssap_msg_dict["node_id"],
                                                      ssap_msg_dict["space_id"],
                                                      "UNSUBSCRIBE",
                                                      ssap_msg_dict["transaction_id"],
                                                      pars)

            # send to the real sib
            rs.send(ssap_msg)
            
        else:
            # send to the real sib
            rs.send(ssap_msg)
            
            # start a generic handler
            thread.start_new_thread(generic_handler, (rs, vs))
            
            


def generic_handler(rs, vs):

    # socket to the virtual sib
    tvs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tvs.settimeout(2)
    tvs.connect((vsib_host, vsib_port))

    while 1:
        # receive the confirm message
        ssap_msg = rs.recv(4096)
        
        if ssap_msg:
            print colored("tpublisher>", "blue", attrs=["bold"]) + " Received confirm message from the Real Sib"
            print colored("tpublisher>", "blue", attrs=["bold"]) + " Forwarding confirm message to the Virtual Sib"
    
            # connect to remote host
            try :
                tvs.send(ssap_msg)
            except socket.error:
                print colored("tpublisher>", "red", attrs=["bold"]) + "Socket error"

        else:
            rs.close()
            tvs.close()
            break    


def rdf_subscription_handler(rs, vs, vsub_id):

    # we open a socket for each subscription
    tvs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tvs.settimeout(2)
    tvs.connect((vsib_host, vsib_port))

    # wait for messages and examinate them!
    while 1:
        ssap_msg = rs.recv(4096)
        if len(ssap_msg) > 1:
            # forwarding subscription-related message to the virtual sib
            print colored("tpublisher>", "blue", attrs=["bold"]) + " Forwarding subscription-related message to the Virtual Sib"
            
            # connect to remote host
            try :

                # convert ssap_msg to dict
                ssap_msg_dict = {}
                parser = make_parser()
                ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                parser.setContentHandler(ssap_mh)
                parser.parse(StringIO(ssap_msg))        
                  
                subscriptions[vsub_id] = ssap_msg_dict["subscription_id"]
                            
                if "<message_type>CONFIRM</message_type>" in ssap_msg and "<transaction_type>SUBSCRIBE</transaction_type>" in ssap_msg:
                    
                    # replace subscription id with virtual subscription id
                    pars = '<parameter name="status">' + ssap_msg_dict["status"] + '</parameter><parameter name="subscription_id">' + vsub_id + '</parameter><parameter name="results">' + ssap_msg_dict["results"] + '</parameter>'

                    ssap_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                              ssap_msg_dict["space_id"],
                                                              ssap_msg_dict["transaction_type"],
                                                              ssap_msg_dict["transaction_id"],
                                                              pars
                                                              )

                    tvs.send(ssap_msg)

                elif "<message_type>INDICATION</message_type>" in ssap_msg and "<transaction_type>SUBSCRIBE</transaction_type>" in ssap_msg:
                    
                    # replace subscription id with virtual subscription id
                    ssap_msg = SSAP_INDICATION_TEMPLATE%(ssap_msg_dict["space_id"],
                                                                 ssap_msg_dict["node_id"],
                                                                 ssap_msg_dict["transaction_id"],
                                                                 ssap_msg_dict["ind_sequence"],
                                                                 str(vsub_id),
                                                                 ssap_msg_dict["new_results"],
                                                                 ssap_msg_dict["obsolete_results"]
                                                              )

                    tvs.send(ssap_msg)


                elif "<message_type>CONFIRM</message_type>" in ssap_msg and "<transaction_type>UNSUBSCRIBE</transaction_type>" in ssap_msg:
                    
                    # replace real subscription id with virtual subscription id
                    pars = '<parameter name="status">' + ssap_msg_dict["status"] + '</parameter><parameter name="subscription_id">' + vsub_id + '</parameter>'

                    ssap_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                              ssap_msg_dict["space_id"],
                                                              ssap_msg_dict["transaction_type"],
                                                              ssap_msg_dict["transaction_id"],
                                                              pars
                                                              )


                    tvs.send(ssap_msg)
                    tvs.close()
                    rs.close()
                    break
                                

            except socket.error:
                print colored("tpublisher>", "red", attrs=["bold"]) + "Socket error"



def sparql_subscription_handler(rs, vs, vsub_id):

    # we open a socket for each subscription
    tvs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tvs.settimeout(2)
    tvs.connect((vsib_host, vsib_port))

    # wait for messages and examinate them!
    complete_ssap_msg = ""
    while 1:
        ssap_msg = rs.recv(4096)        

        # check whether we received a blank message
        if not ssap_msg and not complete_ssap_msg:
            break

        if ssap_msg != None:
            complete_ssap_msg = str(complete_ssap_msg) + str(ssap_msg)
            print "RICEVUTO " + ssap_msg

        # check if we have a complete message to parse
        if "</SSAP_message>" in complete_ssap_msg:
            ssap_msg = complete_ssap_msg.split("</SSAP_message>")[0] + "</SSAP_message>"
            complete_ssap_msg = complete_ssap_msg.replace(ssap_msg, "")

            # forwarding subscription-related message to the virtual sib
            print colored("tpublisher>", "blue", attrs=["bold"]) + " Forwarding subscription-related message to the Virtual Sib"
            
            # connect to remote host
            try :

                # convert ssap_msg to dict
                ssap_msg_dict = {}
                parser = make_parser()
                ssap_mh = SSAPMsgHandler(ssap_msg_dict)
                parser.setContentHandler(ssap_mh)
                parser.parse(StringIO(ssap_msg))        
                  
                subscriptions[vsub_id] = ssap_msg_dict["subscription_id"]
                            
                if "<message_type>CONFIRM</message_type>" in ssap_msg and "<transaction_type>SUBSCRIBE</transaction_type>" in ssap_msg:
                    
                    # replace subscription id with virtual subscription id
                    pars = '<parameter name="status">' + ssap_msg_dict["status"] + '</parameter><parameter name="subscription_id">' + vsub_id + '</parameter><parameter name="results">' + ssap_msg_dict["results"] + '</parameter>'

                    ssap_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                              ssap_msg_dict["space_id"],
                                                              ssap_msg_dict["transaction_type"],
                                                              ssap_msg_dict["transaction_id"],
                                                              pars
                                                              )
                    tvs.send(ssap_msg)

                elif "<message_type>INDICATION</message_type>" in ssap_msg and "<transaction_type>SUBSCRIBE</transaction_type>" in ssap_msg:
                    
                    sys.exit()

                    # replace subscription id with virtual subscription id
                    ssap_msg = SSAP_INDICATION_TEMPLATE%(ssap_msg_dict["space_id"],
                                                                 ssap_msg_dict["node_id"],
                                                                 ssap_msg_dict["transaction_id"],
                                                                 ssap_msg_dict["ind_sequence"],
                                                                 str(vsub_id),
                                                                 ssap_msg_dict["new_results"],
                                                                 ssap_msg_dict["obsolete_results"]
                                                              )

                    tvs.send(ssap_msg)


                elif "<message_type>CONFIRM</message_type>" in ssap_msg and "<transaction_type>UNSUBSCRIBE</transaction_type>" in ssap_msg:
                    
                    # replace real subscription id with virtual subscription id
                    pars = '<parameter name="status">' + ssap_msg_dict["status"] + '</parameter><parameter name="subscription_id">' + vsub_id + '</parameter>'

                    ssap_msg = SSAP_MESSAGE_CONFIRM_TEMPLATE%(ssap_msg_dict["node_id"],
                                                              ssap_msg_dict["space_id"],
                                                              ssap_msg_dict["transaction_type"],
                                                              ssap_msg_dict["transaction_id"],
                                                              pars
                                                              )


                    tvs.send(ssap_msg)
                    tvs.close()
                    rs.close()
                    break
                                

            except socket.error:
                print colored("tpublisher>", "red", attrs=["bold"]) + "Socket error"
                
            
 
#main function
if __name__ == "__main__":

    subs = {}
    node_id = str(uuid.uuid4())

    if(len(sys.argv) < 5) :
        print colored("tpublisher> ", "red", attrs=["bold"]) + 'Usage : python publisher.py vsib_hostname vsib_port realsib_hostname realsib_port'
        sys.exit()
     
    # define hosts data
    vsib_host = sys.argv[1]
    vsib_port = int(sys.argv[2])
    realsib_host = sys.argv[3]
    realsib_port = int(sys.argv[4])
     
    # socket to the virtual sib
    vs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vs.settimeout(2)
     
    # connect to remote host
    try :
        vs.connect((vsib_host, vsib_port))
    except :
        print colored("tpublisher> ", "red", attrs=['bold']) + 'Unable to connect to the virtual SIB'
        sys.exit()    

    print colored("tpublisher> ", "blue", attrs=['bold']) + 'Connected to virtual SIB. Sending register request!'

    # building and sending the register request
    space_id = "X"
    transaction_id = random.randint(0, 1000)
    register_msg = SSAP_MESSAGE_REQUEST_TEMPLATE%(node_id,
                                                  space_id,
                                                  "REGISTER",
                                                  transaction_id, "")
    vs.send(register_msg)

    socket_list = [vs]
        
    while 1:
        # Get the list sockets which are readable
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [])
         
        for sock in read_sockets:
            #incoming message from remote server
            if sock == vs:
                ssap_msg = sock.recv(4096)
                if not ssap_msg :
                    print colored("tpublisher> ", "red", attrs=["bold"]) + 'Disconnected from the virtual SIB'
                    sys.exit()
                else :
                    print colored("tpublisher>", "blue", attrs=["bold"]) + 'Starting a new thread...'
                    thread.start_new_thread(handler, (sock, ssap_msg))

                
             
