#!/usr/bin/python

# requirements
from termcolor import *
import uuid
from SIBLib import SibLib
from smart_m3.m3_kp import *
from virtualiser import *
import threading
import thread

ns = "http://smartM3Lab/Ontology.owl#"

#functions

def NewRemoteSIB():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewRemoteSIB", "cyan", attrs=["bold"])

    # virtual sib id
    virtual_sib_id = str(uuid.uuid4())

    # create two sockets
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # generating two random ports
    while True:
        kp_port = random.randint(10000, 11000)
        if sock.connect_ex(kp_port) == 0:
            print "estratta la porta %s"%(str(kp_port))
            break

    while True:
        pub_port = random.randint(10000, 11000)
        if sock.connect_ex(pub_port) == 0:
            print "estratta la porta %s"%(str(pub_port))
            break        

    # start a virtual sib
    thread.start_new_thread(virtualiser, ("localhost", kp_port, "localhost", pub_port))
    
    # insert information in the ancillary SIB
    a = SibLib("127.0.0.1", 10088)
    t = Triple(URI(virtual_sib_id), URI("hasIpPort"), URI("127.0.0.1-" + str(pub_port)))
    
    # insert information in the ancillary SIB
    a = SibLib("127.0.0.1", 10088)
    t = [Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIpPort"), URI(ns + "127.0.0.1-10011"))]
    t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpIpPort"), URI(ns + "127.0.0.1-10010")))
    a.insert(t)
    
    # return virtual sib id
    return virtual_sib_id

def NewVirtualMultiSIB(sib_list):
    print colored("request_handlers> ", "blue", attrs=["bold"]) + str(sib_list)
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])
    # virtual multi sib id
    virtual_multi_sib_id = str(uuid.uuid4())

    # # TODO start a virtual multi sib
    # thread.start_new_thread(virtualiser, (10010, 10011))
    
    # insert information in the ancillary SIB
    a = SibLib("127.0.0.1", 10088)
    t = []
    for i in sib_list:
        t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(ns + "composedBy"), URI(ns + str(i))))

    t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(ns + "hasKpIpPort"), URI(ns + "127.0.0.1-10010")))
    t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(ns + "hasSibIpPort"), URI(ns + "127.0.0.1-10010")))
    a.insert(t)
    
    # return virtual multi sib id 
    return virtual_multi_sib_id

def Discovery():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("Discovery", "cyan", attrs=["bold"])
    # query to the ancillary sib to get all the existing virtual sib 
    query = """
        SELECT ?s ?o
        WHERE {?s ns:hasKpIpPort ?o}
        """
    a = SibLib("127.0.0.1", 10088)
    result = a.execute_sparql_query(query)
    
    virtual_sib_list = {}
    for i in result:
        sib_id = str(i[0][2].split('#')[1])
        virtual_sib_list[sib_id] = {} 
        sib_ip = virtual_sib_list[sib_id]["ip"] = str(i[1][2].split('#')[1]).split("-")[0]
        sib_port = virtual_sib_list[sib_id]["port"] = str(i[1][2].split('#')[1]).split("-")[1]
    return virtual_sib_list
