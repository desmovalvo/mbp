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


BUFSIZ = 1024

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
def NewRemoteSIB(ancillary_ip, ancillary_port, owner, sib_id):

    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("NewRemoteSIB", "cyan", attrs=["bold"])
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)
    print 'Connected to the ancillary sib'
    
    # search the best virtualiser querying the ancillary sib
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
    
    # if there are some virtualiser
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

    
        print colored("requests_handler> ", "blue", attrs=['bold']) + 'Connected to the virtualiser. Sending ' + colored("NewRemoteSib", "cyan", attrs=["bold"]) + " request!"
            
        # build request message and send it to the selected virtualiser
        request_msg = {"command":"NewRemoteSIB", "owner":owner, "sib_id":sib_id}
        request = json.dumps(request_msg)
        virtualiser.send(request)

        # we wait for the reply
        while 1:
            try:
                confirm_msg = virtualiser.recv(4096)
            except socket.timeout:
                print colored("request_handler> ", "red", attrs=["bold"]) + 'Connection to the virtualiser timed out'
                # TODO: e se il virtualiser ha gia' creato la virtual sib e non abbiamo ricevuto la risposta? mandare una delete remote sib?
                confirm = {'return':'fail', 'cause':' Connection to the virtualiser timed out.'}
                virtualiser.close()
                return confirm
            
            if confirm_msg:
                print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Received the following message:'
                print confirm_msg
                break
            
        # parsing of the reply
        confirm = json.loads(confirm_msg)

        if confirm["return"] == "fail":
            print colored("requests_handler> ", "red", attrs=["bold"]) + 'Creation failed!' + confirm["cause"]
            
        elif confirm["return"] == "ok":
            
            # extract all the information
            virtual_sib_id = confirm["virtual_sib_info"]["virtual_sib_id"]
            virtual_sib_ip = confirm["virtual_sib_info"]["virtual_sib_ip"]
            virtual_sib_pub_port = confirm["virtual_sib_info"]["virtual_sib_pub_port"]
            virtual_sib_kp_port = confirm["virtual_sib_info"]["virtual_sib_kp_port"]
            
            # insert information in the ancillary SIB
            try:
                a = SibLib(ancillary_ip, ancillary_port)

                # remove old triples, if any
                a.remove([Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpIpPort"), None), 
                          Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIpPort"), None),
                          Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasStatus"), None)])

                # add the new triples
                t = [Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIpPort"), URI(ns + str(virtualiser_ip) + "-" + str(virtual_sib_pub_port)))]
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(rdf + "type"), URI(ns + "remoteSib")))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpIpPort"), URI(ns + str(virtualiser_ip) + "-" + str(virtual_sib_kp_port))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasOwner"), URI(ns + str(owner))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasStatus"), URI(ns + "offline")))
                t.append(Triple(URI(ns + str(virtualiser_id)), URI(ns + "hasRemoteSib"), URI(ns + str(virtual_sib_id))))
                a.insert(t)
                
                #############################################
                ##                                         ##
                ## Update the load of selected virtualiser ##
                ##                                         ##
                #############################################
                # get old load
                query = PREFIXES + """SELECT ?load
WHERE { ns:""" + str(virtualiser_id) + """ ns:load ?load }"""

                result = a.execute_sparql_query(query)
                load = int(result[0][0][2])
                print "Old Load " + str(load)
                
                # remove triple
                t = []
                t.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(load))))
                a.remove(t)
                # insert new triple
                #new_load = int(load) + 1
                load += 1
                print "New Load " + str(load)
                t = []
                t.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(load))))
                a.insert(t)
                #############################################

            except socket.error:
                virtual_sib_info = {}
                virtual_sib_info["return"] = "fail"
                virtual_sib_info["cause"] = "Connection to Ancillary Sib failed"
                return virtual_sib_info
            except Exception, e: #TODO catturare qui i sibError
                print 'ECCEZIONE: ' + str(e)
                virtual_sib_info = {}
                virtual_sib_info["return"] = "fail"
                virtual_sib_info["cause"] = "Sib Error"
                return virtual_sib_info


                        
            print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Virtual Sib ' + virtual_sib_id + ' started on ' + str(virtual_sib_ip) + ":" + str(virtual_sib_pub_port)
            
        virtualiser.close()
        return confirm



    # if the query returned 0 results (no virtualiser found)
    else: 
        confirm = {'return':'fail', 'cause':' No virtualisers available.'}
#        virtualiser.close()
        return confirm

def DeleteSIB(ancillary_ip, ancillary_port, sib_id):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DeleteSIB", "cyan", attrs=["bold"])
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)


    # remove information from the ancillary SIB
    try:

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

        # get the virtualiser server related to that virtual sib
        query = PREFIXES + """SELECT ?ip ?port WHERE {?vid ns:hasRemoteSib ns:"""+ str(virtual_sib_id) + """ .
?vid ns:hasIP ?ip .
?vid ns:hasPort ?port}"""
        result = a.execute_sparql_query(query)  

        if len(result) > 0:
            virtualiser_ip = (result[0][0][2]).split("#")[1]
            virtualiser_port = (result[0][1][2]).split("#")[1]
            print "Virtualiser ip " + str(virtualiser_ip)
            print "Virtualiser port " + str(virtualiser_port)
        
    except socket.error:
        print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the ancillary SIB'
        confirm = {'return':'fail', 'cause':' Unable to connect to the ancillary SIB.'}
        return confirm

    if len(result) > 0:
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

    else:

        # The virtualiser is not online anymore, so we have to remove the triples here
        a.remove(Triple(URI(ns + virtual_sib_id), None, None))
        a.remove(Triple(None, None, URI(ns + virtual_sib_id)))
        confirm = {}
        confirm['return'] = 'ok'
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
    print " query to the ancillary sib to get all the existing virtual sib "
    query = PREFIXES + """
        SELECT ?s ?o
        WHERE {?s ns:hasKpIpPort ?o}
        """
    a = SibLib(ancillary_ip, ancillary_port)
    result = a.execute_sparql_query(query)
    print "query done"
    
    virtual_sib_list = {}
    for i in result:
        sib_id = str(i[0][2].split('#')[1])
        virtual_sib_list[sib_id] = {} 
        sib_ip = virtual_sib_list[sib_id]["ip"] = str(i[1][2].split('#')[1]).split("-")[0]
        sib_port = virtual_sib_list[sib_id]["port"] = str(i[1][2].split('#')[1]).split("-")[1]
    print "query results: " + str(virtual_sib_list)
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


def SetSIBStatus(ancillary_ip, ancillary_port, sib_id, new_status):
    
    print "request handler: set status"
    
    # Connecting to the ancillary SIB
    try:
        a = SibLib(ancillary_ip, int(ancillary_port))
    except:
        confirm = {'return':'fail', 'cause':' connection to the ancillary SIB failed.'}
        return confirm

    # Verify if the new status is different from the old one
    try:
        old_status = a.execute_rdf_query((URI(ns + str(sib_id)), URI(ns + "hasStatus"), None))
        old_status = str(old_status[0][2]).split("#")[1]
    except:
        confirm = {'return':'fail', 'cause':' query failed.'}
        return confirm

    # Setting the status
    try:
        if (old_status.lower() != new_status.lower()) and (new_status.lower() in ["online", "offline"]):
            a.update(Triple(URI(ns + str(sib_id)), URI(ns + "hasStatus"), URI(ns + new_status)),
                     Triple(URI(ns + str(sib_id)), URI(ns + "hasStatus"), URI(ns + old_status)))
        else:
            # Nothing to do ...
            confirm = {'return':'ok'}
            return confirm
    except:
        confirm = {'return':'fail', 'cause':' Connection to the virtualiser timed out.'}
        return confirm

    # Look for the VMSIBs using this SIB
    try:
        vmsibs = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?ipport
WHERE { ?vmsib_id ns:hasKpIpPort ?ipport .
?vmsib_id ns:composedBy ns:""" + sib_id + """ }""")
    except:
        # Impossible to contact the virtual multi sibs
        print colored("request_handler> ", "red", attrs = ["bold"]) + "failed to contact the virtual multi sibs"

    # Notify the status change to all the VMSIBs
    if len(vmsibs) > 0:
        
        print "request handler: inoltro alle vmsib"

        # Build the JSON message
        new_status_msg = {"command": "StatusChange", "sib_id": sib_id, "status": new_status}
        new_status_json_msg = json.dumps(new_status_msg)

        # Notify the status change to all the VMSIBs
        for vms in vmsibs:
         
            try:
                # connection to the vmsib
                vms_ip = str(vms[0][2].split("#")[1]).split("-")[0]
                vms_port = str(vms[0][2].split("#")[1]).split("-")[1]
                vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                vms_socket.connect((vms_ip, int(vms_port)))
                vms_socket.send(new_status_json_msg)

                # Wait for a reply
                while 1:
                    try:
                        print "aspetto conferma"
                        confirm_msg = vms_socket.recv(4096)
                        print "conferma ricevuta"
                        break
                    except socket.timeout:
                        print colored("request_handler> ", "red", attrs=["bold"]) + 'connection to the virtualiser timed out'            

                # Close the socket to the vmsib
                vms_socket.close()

            except:
                print colored("request_handler> ", "red", attrs=["bold"]) + 'impossible to contact the virtualsib'            

    # return
    confirm = {'return':'ok'}
    
    print "ritorno risposta"
    return confirm


def AddSIBtoVMSIB(ancillary_ip, ancillary_port, vmsib_id, sib_list):

    print 'ADDSIB'

    # check if the vmsib really exists
    a = SibLib(ancillary_ip, ancillary_port)
    print vmsib_id
    res = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(rdf + "type"), URI(ns + "virtualMultiSib")))
    print "RES"
    print res

    if len(res) == 1:

        # get the list of all the SIBs
        try:
            print "TRY1"
            SIBs = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s
WHERE {{ ?s rdf:type ns:remoteSib } UNION { ?s rdf:type ns:virtualMultiSib }}""")
        except:
            print "connection failed to the ancillary sib"

        # extract only the SIBs
        existing_sibs = []
        for k in SIBs:
            existing_sibs.append(str(k[0][2]).split("#")[1])
        print "existing_sibs"

        # check if the specified sibs really exist
        for s in sib_list:
            if not(s in existing_sibs):
                confirm = {'return':'fail', 'cause':' SIB ' + str(s) + ' does not exist.'}
                return confirm
        print "525"

        # build the json msg for the VirtualMultiSib
        msg = { "command" : "AddSIBtoVMSIB", "sib_list" : sib_list, "vmsib_id" : vmsib_id }
        jmsg = json.dumps(msg)
        print "530"

        # get the virtualmultisib parameters
        vms = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?o
WHERE { ns:""" + vmsib_id + """ ns:hasKpIpPort ?o }""")

        # send a message to the virtualiser
        try:
            print "TRY2"
            print "contatto la vmsib",
            # connection to the vmsib
            vms_ip = str(vms[0][0][2].split("#")[1]).split("-")[0]
            vms_port = str(vms[0][0][2].split("#")[1]).split("-")[1]
            vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            vms_socket.connect((vms_ip, int(vms_port)))
            vms_socket.send(jmsg)
            vms_socket.close()

            # update info into the ancillary
            for s in sib_list:
                print "Insert",
                a.insert(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + str(s))))
                print "Successful"

            # confirm
            confirm = {'return':'ok'}
            return confirm

        except:
            print sys.exc_info()
            print colored("request_handler> ", "red", attrs=["bold"]) + 'impossible to contact the VirtualMultiSib'      

    else:
        confirm = {'return':'fail', 'cause':' VirtualMultiSib does not exist.'}
        return confirm



def RemoveSIBfromVMSIB(ancillary_ip, ancillary_port, vmsib_id, sib_list):

    # check if the vmsib really exists
    a = SibLib(ancillary_ip, ancillary_port)
    print vmsib_id
    res = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(rdf + "type"), URI(ns + "virtualMultiSib")))
    print res

    if len(res) == 1:
        
        # get the list of all the SIBs
        try:
            SIBs = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s
WHERE {{ ?s rdf:type ns:remoteSib } UNION { ?s rdf:type ns:virtualMultiSib }}""")
        except:
            print "connection failed to the ancillary sib"

        # extract only the SIBs
        existing_sibs = []
        for k in SIBs:
            existing_sibs.append(str(k[0][2]).split("#")[1])
        print "existing_sibs"

        # check if the specified sibs really exist
        for s in sib_list:
            if not(s in existing_sibs):
                confirm = {'return':'fail', 'cause':' SIB ' + str(s) + ' does not exist.'}
                return confirm
            
        # build the json msg for the VirtualMultiSib
        msg = { "command" : "RemoveSIBfromVMSIB", "sib_list" : sib_list, "vmsib_id" : vmsib_id }
        jmsg = json.dumps(msg)

        # get the virtualmultisib parameters
        vms = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?o
WHERE { ns:""" + vmsib_id + """ ns:hasKpIpPort ?o }""")

        # send a message to the virtualiser
        try:
            print "contatto la vmsib",
            # connection to the vmsib
            vms_ip = str(vms[0][0][2].split("#")[1]).split("-")[0]
            vms_port = str(vms[0][0][2].split("#")[1]).split("-")[1]
            vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            vms_socket.connect((vms_ip, int(vms_port)))
            vms_socket.send(jmsg)
            vms_socket.close()

            # update info into the ancillary
            for s in sib_list:
                a.remove(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + str(s))))
                print "Successful"

            # confirm
            confirm = {'return':'ok'}
            return confirm

        except:
            print sys.exc_info()
            print colored("request_handler> ", "red", attrs=["bold"]) + 'impossible to contact the VirtualMultiSib'      

    else:
        confirm = {'return':'fail', 'cause':' VirtualMultiSib does not exist.'}
        return confirm


# def SetSIBStatus(ancillary_ip, ancillary_port, sib_id, status):
#     print "request handler del manager: ricevuta SetSIBStatus request"
#     confirms = None
#     # check if the sib with sib_id really exists
#     a = SibLib(ancillary_ip, ancillary_port)
        
#     # get the list of all the SIBs
#     try:
#         print "try"
#         SIBs = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX ns: <http://smartM3Lab/Ontology.owl#>
# SELECT ?s
# WHERE {{ ?s rdf:type ns:remoteSib } UNION { ?s rdf:type ns:virtualMultiSib }}""")
#     except:
#         print "connection failed to the ancillary sib"
        
#     # extract only the SIBs
#     existing_sibs = []
#     for k in SIBs:
#         existing_sibs.append(str(k[0][2]).split("#")[1])
    
#     print "costruita lista sibs"
#     print "\n\n\n" + str(existing_sibs) + "\n\n\n"
#     print "\n\n\n" + str(sib_id) + "\n\n\n"

    
    
#     # check if the specified sibs really exist
#     if not(sib_id in existing_sibs):
#         print "if not sib_id in existing sibs"
#         confirm = {'return':'fail', 'cause':' SIB ' + str(sib_id) + ' does not exist.'}
#         return confirm
            
#     # update info into the ancillary
#     res = a.execute_rdf_query(Triple(URI(ns + sib_id), URI(ns + "hasStatus"), None))
#     print "fatta query stato"
#     st = str(res[0][2]).split("#")[1]
#     a.remove(Triple(URI(ns + sib_id), URI(ns + "hasStatus"), URI(ns + st)))
#     if status == "online":
#         a.insert(Triple(URI(ns + sib_id), URI(ns + "hasStatus"), URI(ns + "online")))
#     else:
#         a.insert(Triple(URI(ns + sib_id), URI(ns + "hasStatus"), URI(ns + "offline")))
#     print "Successful"
    
#     confirms = True

#     #check if the sib is part of an or more multisibs
#     res = a.execute_rdf_query(Triple(None, URI(ns + "composedBy"), URI(ns + sib_id)))
#     vmsib = []
#     for i in res:
#         print "for multi sib"
#         vmsib.append(str(i[0]).split("#")[1])
#     jmsg = {"command":"ChangedSIBStatus", "sib_id":str(sib_id), "status":status}
#     msg = json.dumps(jmsg)
    
#     for i in vmsib:
#         # get the virtualmultisib parameters
#         vms = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX ns: <http://smartM3Lab/Ontology.owl#>
# SELECT ?o
# WHERE { ns:""" + i + """ ns:hasKpIpPort ?o }""")

#         # send a message to the virtualiser
#         try:
#             print "contatto la vmsib",
#             # connection to the vmsib
#             vms_ip = str(vms[0][0][2].split("#")[1]).split("-")[0]
#             vms_port = str(vms[0][0][2].split("#")[1]).split("-")[1]
#             vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             vms_socket.connect((vms_ip, int(vms_port)))
#             vms_socket.send(msg)
#             confirm = vms_socket.recv(BUFSIZ)
#             confirm = json.loads(confirm)
#             print confirm
#             if confirm["return"] == "failed":
#                 confirms = False
#                 break
#             else:
#                 confirms = True
                
#         except socket.error:
#             print "socket error 719 request handler del manager"
#             confirms = False
#             break
            
#     # confirm
#     if confirms == True:
#         confirm = {'return':'ok'}
            
#     else:
#         confirm = {'return':'failed'}
        
#     print "rh ------ confirm---- " + str(confirm) + "inoltrata al manager server"
#     return confirm
