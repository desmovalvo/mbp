#!/usr/bin/python

# requirements
from multiprocessing import Process
from output_helpers import *
from termcolor import *
import uuid
from SIBLib import SibLib
from smart_m3.m3_kp import *
from remoteSIB import *
from virtualMultiSIB import *
import threading
import thread
import random

rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
ns = "http://smartM3Lab/Ontology.owl#"

PREFIXES = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <""" + ns + ">"

#functions

def NewRemoteSIB(owner, sib_id, virtualiser_ip, threads, thread_id, virtualiser_id, ancillary_ip, ancillary_port, manager_ip, manager_port):
    # debug print
    print reqhandler_print(True) + "executing method " + colored("NewRemoteSIB", "cyan", attrs=["bold"])

    # virtual sib id
    if sib_id == "none":
        virtual_sib_id = str(uuid.uuid4())
    else:
        virtual_sib_id = sib_id

    # create two sockets
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # generating two random ports
    while True:
        kp_port = random.randint(10000, 11000)
        if s1.connect_ex(("localhost", kp_port)) != 0:
            print "estratta la porta %s"%(str(kp_port))
            break

    while True:
        pub_port = random.randint(10000, 11000)
        if pub_port != kp_port:
            if s2.connect_ex(("localhost", pub_port)) != 0:
                print "estratta la porta %s"%(str(pub_port))
                break        
    

    # start the virtual sib process
    threads[thread_id] = True
    p = Process(target=remoteSIB, args=(virtualiser_ip, kp_port, pub_port, virtual_sib_id, threads[thread_id], ancillary_ip, ancillary_port, manager_ip, manager_port))
    p.start()

    # build the reply
    virtual_sib_info = {}
    virtual_sib_info["return"] = "ok"
    virtual_sib_info["virtual_sib_id"] = str(virtual_sib_id)
    virtual_sib_info["virtual_sib_ip"] = str(virtualiser_ip)
    virtual_sib_info["virtual_sib_pub_port"] = pub_port
    virtual_sib_info["virtual_sib_kp_port"] = kp_port
    virtual_sib_info["virtual_sib_owner"] = str(owner)

    # return virtual sib info
    return virtual_sib_info


def DeleteRemoteSIB(virtual_sib_id, threads, t_id, virtualiser_id, ancillary_ip, ancillary_port):

    # killare il thread virtualiser lanciato all'interno del metodo NewRemoteSib
    threads[t_id[virtual_sib_id]] = False
    print colored("virtualiser_server> ", "blue", attrs=["bold"]) + 'Virtual Sib ' + virtual_sib_id + ' killed ' 
    confirm = {'return':'ok'}
    return confirm
                                        


############################################################
#
# NewVirtualMultiSIB
#
# This function handles the NewVirtualMultiSIB request and
# it's called by the virtualiser_server once it receives
# a NewVirtualMultiSIB request message
#
############################################################
    
def NewVirtualMultiSIB(sib_list, virtualiser_ip, virtualiser_id, threads, thread_id, ancillary_ip, ancillary_port):

    # debug print
    print reqhandler_print(True) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])
    print reqhandler_print(True) + "SIB list: " + str(sib_list)

    # virtual multi sib id
    virtual_multi_sib_id = str(uuid.uuid4())

    # create a socket
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # generating a random port
    while True:
        kp_port = random.randint(10000, 11000)
        if s1.connect_ex(("localhost", kp_port)) != 0:
            print reqhandler_print(True) + "generated port " + str(kp_port) + " for the new VMSIB"
            break
        
    # fill a dictionary with the info about the VMSIB
    virtual_multi_sib_info = {}
    virtual_multi_sib_info["virtual_multi_sib_id"] = str(virtual_multi_sib_id)
    virtual_multi_sib_info["virtual_multi_sib_ip"] = str(virtualiser_ip)
    virtual_multi_sib_info["virtual_multi_sib_kp_port"] = kp_port

    # start a virtual multi sib    
    threads[thread_id] = True
    p = Process(target=virtualMultiSIB, args=(virtualiser_ip, kp_port, virtual_multi_sib_id, threads[thread_id], sib_list, ancillary_ip, ancillary_port))
    p.start()

    # return the virtual multi sib info
    return virtual_multi_sib_info


############################################################
#
# Discovery
#
# This function handles the Discovery request and
# it's called by the virtualiser_server once it receives
# a Discovery request message
#
############################################################

def Discovery(ancillary_ip, ancillary_port):
    # debug print
    print reqhandler_print(True) + "executing method " + colored("Discovery", "cyan", attrs=["bold"])
    # query to the ancillary sib to get all the existing virtual sib 
    query = """
        SELECT ?s ?o
        WHERE {?s ns:hasKpIpPort ?o}
        """
    a = SibLib(ancillary_ip, ancillary_port)
    result = a.execute_sparql_query(query)
    
    virtual_sib_list = {}
    for i in result:
        sib_id = str(i[0][2].split('#')[1])
        virtual_sib_list[sib_id] = {} 
        sib_ip = virtual_sib_list[sib_id]["ip"] = str(i[1][2].split('#')[1]).split("-")[0]
        sib_port = virtual_sib_list[sib_id]["port"] = str(i[1][2].split('#')[1]).split("-")[1]
    return virtual_sib_list
