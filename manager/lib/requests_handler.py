#!/usr/bin/python

# requirements
import sys
import uuid
import json
import thread
import threading
from query import *
from random import *
from termcolor import *
from SIBLib import SibLib
from smart_m3.m3_kp import *
import socket, select, string, sys

# constants
ns = "http://smartM3Lab/Ontology.owl#"
rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#"

PREFIXES = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <""" + ns + ">"




#functions
def RegisterPublicSIB(ancillary_ip, ancillary_port, owner, sib_ip, sib_port):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("RegisterPublicSIB", "cyan", attrs=["bold"])
    
    # sib id
    sib_id = str(uuid.uuid4())


    # insert information in the ancillary SIB
    try:
        a = SibLib(ancillary_ip, ancillary_port)
        # in realta' non e' una remote sib 
        t = [Triple(URI(ns + str(sib_id)), URI(rdf + "type"), URI(ns + "remoteSib"))]
        t.append(Triple(URI(ns + str(sib_id)), URI(ns + "hasKpIpPort"), URI(ns + str(sib_ip) + "-" + str(sib_port))))
        t.append(Triple(URI(ns + str(sib_id)), URI(ns + "hasOwner"), URI(ns + str(owner))))
        t.append(Triple(URI(ns + str(sib_id)), URI(ns + "hasStatus"), URI(ns + "online")))
        a.insert(t)
        
        sib_info = {}
        sib_info["return"] = "ok"
        sib_info["sib_id"] = str(sib_id)
        sib_info["sib_ip"] = str(sib_ip)
        sib_info["sib_port"] = sib_port
        sib_info["sib_owner"] = str(owner)

        # return sib info
        return sib_info

    except socket.error:
        sib_info = {}
        sib_info["return"] = "fail"
        sib_info["cause"] = "Connection to Ancillary Sib failed"
        return sib_info
    except Exception, e: #TODO catturare qui i sibError
        print 'ECCEZIONE: ' + str(e)
        sib_info = {}
        sib_info["return"] = "fail"
        sib_info["cause"] = "Sib Error"
        return sib_info



#functions
def NewRemoteSIB(ancillary_ip, ancillary_port, owner):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("NewRemoteSIB", "cyan", attrs=["bold"])
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)

    try:
        try:
            result = get_best_virtualiser(a)
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
        virtualiser.settimeout(15)
        
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
            try:
                confirm_msg = virtualiser.recv(4096)
            except socket.timeout:
                print colored("request_handler> ", "red", attrs=["bold"]) + 'Connection to the virtualiser timed out'
                confirm = {'return':'fail', 'cause':' Connection to the virtualiser timed out.'}
                virtualiser.close()
                return confirm
            
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

def DeleteSIB(ancillary_ip, ancillary_port, sib_id):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DeleteSIB", "cyan", attrs=["bold"])
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)


    # remove information from the ancillary SIB
    try:
#####################
        # check if the remote SIB is part of a virtual multi SIB
        t = Triple(None, URI(ns + "composedBy"), URI(ns + sib_id))
        result = a.execute_rdf_query(t)
        a.remove(result)
        
        # check if we have to set the multi SIBs offline
        for vmsib in result:
            t2 = Triple(URI(vmsib[0]), URI(ns + "composedBy"), None)
            r = a.execute_rdf_query(t2)
            if len(r) == 0:
                t3 = Triple(URI(vmsib[0]), URI(ns + "hasStatus"), None)
                t4 = Triple(URI(vmsib[0]), URI(ns + "hasStatus"), URI(ns + "offline"))
                r = a.execute_rdf_query(t3)
                a.remove(r)
                a.insert(t4)

        # remove the triples related to the remote SIB
        t = Triple(URI(ns + sib_id), None, None)
        result = a.execute_rdf_query(t)  
        #print result
        a.remove(result)

        confirm = {'return':'ok'}
        
    except socket.error:
        print colored("request_handlers> ", "red", attrs=['bold']) + 'Unable to connect to the ancillary SIB'
        confirm = {'return':'fail', 'cause':' Unable to connect to the ancillary SIB.'}

    return confirm
                                        


def DeleteRemoteSIB(ancillary_ip, ancillary_port, virtual_sib_id):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DeleteRemoteSIB", "cyan", attrs=["bold"])
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)

    try:
        print virtual_sib_id
        
        query = PREFIXES + """SELECT ?ip ?port WHERE {?vid ns:hasRemoteSib ns:"""+ str(virtual_sib_id) + """ .
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
    virtualiser.settimeout(15)
        
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


def NewVirtualMultiSIB(ancillary_ip, ancillary_port, sib_list):

    # debug info
    print colored("requests_handler> ", "blue", attrs=["bold"]) + str(sib_list)
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])

    # connection to the ancillary sib
    a = SibLib(ancillary_ip, ancillary_port)

    # check if the received sibs are all existing and alive
    # TODO: add an or clause to allow the use of others multisibs 
    for sib in sib_list:
        res = get_sib_ip_port(sib, a)
        if len(res) == 1:
            print "sib trovata"
        else:            
            return {'return':'fail', 'cause':'not all the SIBs are alive'}

    # select the virtualiser with the lowest load
    try:
        try:
            result = get_best_virtualiser(a)
        except SIBError:
            confirm = {'return':'fail', 'cause':' SIBError.'}
            return confirm

    except socket.error:
        print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the ancillary SIB'
        confirm = {'return':'fail', 'cause':' Unable to connect to the ancillary SIB.'}
        return confirm

    if len(result) > 0:
        virtualiser_id = result[0][0][2].split("#")[1]
        virtualiser_ip = result[0][1][2].split("#")[1]
        virtualiser_port = int(result[0][2][2].split("#")[1])                
        virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        virtualiser.settimeout(15)
        
        # connect to the virtualiser
        try :
            virtualiser.connect((virtualiser_ip, virtualiser_port))
        except :
            print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser'
            confirm = {'return':'fail', 'cause':' Unable to connect to the virtualiser.'}
            return confirm

        # build request message 
        request_msg = {"command":"NewVirtualMultiSIB", "sib_list":sib_list}
        request = json.dumps(request_msg)
        virtualiser.send(request)

        # wait for a reply
        while 1:
            try:
                confirm_msg = virtualiser.recv(4096)
            except socket.timeout:
                print colored("request_handler> ", "red", attrs=["bold"]) + 'Connection to the virtualiser timed out'
                confirm = {'return':'fail', 'cause':' Connection to the virtualiser timed out.'}
                virtualiser.close()
                return confirm
            
            if confirm_msg:
                print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Received the following message:'
                print confirm_msg
                break
    
        # parse the reply
        confirm = json.loads(confirm_msg)
        print "confirm: " + str(confirm)
        if confirm["return"] == "fail":
            print colored("requests_handler> ", "red", attrs=["bold"]) + 'Creation failed!' + confirm["cause"]
            
        elif confirm["return"] == "ok":
            virtual_multisib_id = confirm["virtual_multi_sib_info"]["virtual_multi_sib_id"]
            virtual_multisib_ip = confirm["virtual_multi_sib_info"]["virtual_multi_sib_ip"]
            virtual_multisib_kp_port = confirm["virtual_multi_sib_info"]["virtual_multi_sib_kp_port"]
            
            print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Virtual Multi SIB ' + virtual_multisib_id + ' started on ' + str(virtual_multisib_ip) + ":" + str(virtual_multisib_kp_port)
            
        virtualiser.close()
        return confirm

    
    # return the confirm message
    return {'return':'ok', 'virtual_multi_sib_id':virtual_multisib_id}



def DiscoveryAll(ancillary_ip, ancillary_port):
    # debug print
    print ancillary_ip
    print ancillary_port
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DiscoveryAll", "cyan", attrs=["bold"])
    # query to the ancillary sib to get all the existing virtual sib 
    query = PREFIXES + """
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

def DiscoveryWhere(ancillary_ip, ancillary_port, sib_profile):
    # debug print
    print ancillary_ip
    print ancillary_port
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DiscoveryWhere", "cyan", attrs=["bold"])
    
    key = str(sib_profile.split(":")[0])
    value = str(sib_profile.split(":")[1])

    # query to the ancillary sib to get all the reachable sibs with key value = value
    query = PREFIXES + """
SELECT ?s ?o
WHERE {?s ns:hasKpIpPort ?o .
       ?s ns:""" + key + """ ns:""" + value +"""}"""

    a = SibLib(ancillary_ip, int(ancillary_port))
    result = a.execute_sparql_query(query)
    
    virtual_sib_list = {}
    for i in result:
        sib_id = str(i[0][2].split('#')[1])
        virtual_sib_list[sib_id] = {} 
        sib_ip = virtual_sib_list[sib_id]["ip"] = str(i[1][2].split('#')[1]).split("-")[0]
        sib_port = virtual_sib_list[sib_id]["port"] = str(i[1][2].split('#')[1]).split("-")[1]
    return virtual_sib_list
