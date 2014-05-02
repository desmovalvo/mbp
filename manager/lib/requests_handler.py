#!/usr/bin/python

# requirements
import uuid
import json
import thread
import threading
from random import *
from termcolor import *
from SIBLib import SibLib
from smart_m3.m3_kp import *
import sys

# constants
ns = "http://smartM3Lab/Ontology.owl#"
ancillary_ip = "127.0.0.1"
ancillary_port = 10088


#functions

def NewRemoteSIB(owner):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("NewRemoteSIB", "cyan", attrs=["bold"])
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)

    try:
        query = """SELECT DISTINCT ?s ?ip ?port
WHERE { ?s rdf:type ns:virtualiser .
        ?s ns:load ?o .
        ?s ns:hasIP ?ip .
        ?s ns:hasPort ?port .
        OPTIONAL { ?loaded rdf:type ns:virtualiser .
                   ?loaded ns:load ?oo .
                   FILTER (?oo < ?o)}
        FILTER(!bound (?loaded))
}
LIMIT 1"""
    
        try:
            result = a.execute_sparql_query(query)
        except SIBError:
            confirm = {'return':'fail', 'cause':' SIBError.'}
            return confirm

    except socket.error:
        print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the ancillary SIB'
        confirm = {'return':'fail', 'cause':' Unable to connect to the ancillary SIB.'}
        return confirm


    if len(result) > 0: # != None:
        virtualiser_id = result[0][0][2].split("#")[1]
        virtualiser_ip = result[0][1][2].split("#")[1]
        virtualiser_port = int(result[0][2][2].split("#")[1])        

        virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        virtualiser.settimeout(2)
        
        # connect to the virtualiser
        try :
            virtualiser.connect((virtualiser_ip, virtualiser_port))
        except :
            print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser'
            sys.exit()        
    
        print colored("requests_handler> ", "blue", attrs=['bold']) + 'Connected to the virtualiser. Sending ' + colored("NewRemoteSib", "cyan", attrs=["bold"]) + " request!"
    
        # build request message 
        request_msg = {"command":"NewRemoteSIB", "owner":owner}
        request = json.dumps(request_msg)
        virtualiser.send(request)
            
        while 1:
            confirm_msg = virtualiser.recv(4096)
            if confirm_msg:
                print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Received the following message:'
                print confirm_msg
                break
    
        confirm = json.loads(confirm_msg)
        if confirm["return"] == "fail":
            print colored("requests_handler> ", "red", attrs=["bold"]) + 'Creation failed!' + confirm["cause"]
            
        elif confirm["return"] == "ok":
            virtual_sib_id = confirm["virtual_sib_info"]["virtual_sib_id"]
            virtual_sib_ip = confirm["virtual_sib_info"]["virtual_sib_ip"]
            virtual_sib_pub_port = confirm["virtual_sib_info"]["virtual_sib_pub_port"]
            
            print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Virtual Sib ' + virtual_sib_id + ' started on ' + str(virtual_sib_ip) + ":" + str(virtual_sib_pub_port)
            
        virtualiser.close()
        return confirm

    # if the query returned 0 results
    else: 
        confirm = {'return':'fail', 'cause':' No virtualisers available.'}
        virtualiser.close()
        return confirm

def DeleteRemoteSIB(virtual_sib_id):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DeleteRemoteSIB", "cyan", attrs=["bold"])
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)

    try:
        print virtual_sib_id
        
        query = """SELECT ?ip ?port WHERE {?vid ns:hasRemoteSib ns:"""+ str(virtual_sib_id) + """ .
?vid ns:hasIP ?ip .
?vid ns:hasPort ?port}"""
        
        print query

        result = a.execute_sparql_query(query)  
        virtualiser_ip = (result[0][0][2]).split("#")[1]
        virtualiser_port = (result[0][1][2]).split("#")[1]
        print "Virtualiser ip " + str(virtualiser_ip)
        print "Virtualiser port " + str(virtualiser_port)
        
    except socket.error:
        print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the ancillary SIB'
        confirm = {'return':'fail', 'cause':' Unable to connect to the ancillary SIB.'}
        return confirm

    
    virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    virtualiser.settimeout(2)
        
    # connect to the virtualiser
    try :
        virtualiser.connect((virtualiser_ip, int(virtualiser_port)))
    except :
        print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser'
        sys.exit()        
    
    print colored("requests_handler> ", "blue", attrs=['bold']) + 'Connected to the virtualiser. Sending ' + colored("DeleteRemoteSib", "cyan", attrs=["bold"]) + " request!"
    
    # build request message 
    request_msg = {"command":"DeleteRemoteSIB", "virtual_sib_id":virtual_sib_id}
    request = json.dumps(request_msg)
    try:
        virtualiser.send(request)
    except:
        print colored("requests_handler> ", "red", attrs=['bold']) + 'Send failed'
        sys.exit()        
        
            
    while 1:
        confirm_msg = virtualiser.recv(4096)
        if confirm_msg:
            print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Received the following message:'
            print confirm_msg
            break
    
    confirm = json.loads(confirm_msg)
    if confirm["return"] == "fail":
        print colored("requests_handler> ", "red", attrs=["bold"]) + 'Deletion failed!' + confirm["cause"]
            
    elif confirm["return"] == "ok":
        print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Triples deleted!' 
        print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Virtual Sib ' + virtual_sib_id + ' killed ' 
        
    return confirm


def NewVirtualMultiSIB(sib_list):
    print colored("requests_handler> ", "blue", attrs=["bold"]) + str(sib_list)
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])
    # virtual multi sib id
    virtual_multi_sib_id = str(uuid.uuid4())

    # # TODO start a virtual multi sib
    # thread.start_new_thread(virtualiser, (virtual_multi_sib_id))
    
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
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("Discovery", "cyan", attrs=["bold"])
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
