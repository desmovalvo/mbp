#!/usr/bin/python

# requirements
import sys
import uuid
import json
import thread
import threading
import traceback
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
                print 'INFORMAZIONI INSERITE'
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
                
                # remove triple
                t = []
                t.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(load))))
                a.remove(t)
                # insert new triple
                #new_load = int(load) + 1
                load += 1
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
                virtual_sib_info = {}
                virtual_sib_info["return"] = "fail"
                virtual_sib_info["cause"] = "Sib Error"
                return virtual_sib_info


                        
            print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Virtual Sib ' + virtual_sib_id + ' started on ' + str(virtual_sib_ip) + ":" + str(virtual_sib_kp_port)
            
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

    # connection to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)

    # get the virtualiser
    query = PREFIXES + """SELECT ?ip ?port ?vid WHERE {?vid ns:hasRemoteSib ns:"""+ str(virtual_sib_id) + """ .
?vid ns:hasIP ?ip .
?vid ns:hasPort ?port}"""
    result = a.execute_sparql_query(query)
    
    if len(result) > 0:
        
        print "VIRTUALISER TROVATO"

        virtualiser_ip = (result[0][0][2]).split("#")[1]
        virtualiser_port = (result[0][1][2]).split("#")[1]
        virtualiser_id = (result[0][2][2]).split("#")[1]

        # Connect to the virtualiser
        virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # connect to the virtualiser
        try:
            virtualiser.connect((virtualiser_ip, int(virtualiser_port)))                  
            print "CONNESSO AL VIRTUALISER"
        except:
            print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser '
            confirm = {'return':'fail', 'cause':' Unable to connect to the virtualiser.'}
            return confirm
    
        # send the request to the virtualiser
        try:
            # build the message
            msg = json.dumps({"command" : "DeleteRemoteSIB", "virtual_sib_id" : str(virtual_sib_id)})
            virtualiser.send(msg)
            print "RICHIESTA INVIATA AL VIRTUALISER"
        except:
            print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to send the request to the virtualiser  '
            confirm = {'return':'fail', 'cause':' Unable to send the request to the virtualiser.'}
            return confirm

        # wait for a reply
        try:
            while 1:
                confirm_msg = virtualiser.recv(4096)
                if confirm_msg:
                    print colored("requests_handler> CONFIRM ", "blue", attrs=["bold"]) + 'Received the following message:'
                    virtualiser.close()
                    break                
        except:
            print colored("requests_handler> ", "red", attrs=['bold']) + 'Reply not received from the virtualiser '
            confirm = {'return':'fail', 'cause':' Reply not received from the virtualiser'}
            return confirm
        
        # check the confirm
        confirm = json.loads(confirm_msg)
        if confirm["return"] == "fail":            
            print colored("requests_handler> ", "red", attrs=["bold"]) + 'Deletion failed!' + confirm["cause"]
            return confirm

        else:

            print "CERCO LE MULTI SIB"

            # get the list of the virtual multi sib in which that virtual sib appear
            query = PREFIXES + """SELECT ?id ?ipport 
WHERE { ?id ns:composedBy ns:""" + str(virtual_sib_id) + """ . ?id ns:hasKpIpPort ?ipport }"""
            result = a.execute_sparql_query(query)
            if len(result) > 0:

                # build RemoveSIBfromVMSIB message
                msg = json.dumps({"command":"RemoveSIBfromVMSIB", "sib_list":[str(virtual_sib_id)]})

                # send the RemoveSIBfromVMSIB request to all the vmsibs

                for multisib in result:

                    # get vmsib parameters
                    vmsib_id = multisib[0][2].split("#")[1]
                    vmsib_ip = multisib[1][2].split("#")[1].split("-")[0]
                    vmsib_port = int(multisib[1][2].split("#")[1].split("-")[1])
                    
                    # connect to the vmsib
                    vmsib = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
                    # connect to the vmsib
                    try:
                        vmsib.connect((vmsib_ip, vmsib_port))                  
                    except:
                        print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the vmsib'
                        confirm = {'return':'fail', 'cause':' Unable to connect to the vmsib.'}
                        return confirm
                
                    # send the request to the vmsib
                    try:
                        # build the message
                        vmsib.send(msg)
                    except:
                        print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to send the request to the vmsib'
                        confirm = {'return':'fail', 'cause':' Unable to send the request to the vmsib.'}
                        return confirm
            
                    # wait for a reply
                    try:
                        while 1:
                            confirm_msg = vmsib.recv(4096)
                            confirm_msg = json.loads(confirm_msg)

                            if confirm_msg:
                                print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Received the following message:'
                                vmsib.close()
                                break                    

                    except:
                        print colored("requests_handler> ", "red", attrs=['bold']) + 'Reply not received from the vmsib'
                        confirm = {'return':'fail', 'cause':' Reply not received from the vmsib'}
                        return confirm                    

                    # eventually update the status
                    if confirm_msg["return"] == "fail":
                        print colored("requests_handler> ", "red", attrs=['bold']) + 'Negative reply from vmsib'
                        confirm = {'return':'fail', 'cause':' Negative reply from vmsib'}
                        return confirm                    
                    else:
                        a.remove(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + virtual_sib_id)))
                        r = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), None))
                        if len(r) == 0:
                            a.remove(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), None))
                            a.insert(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), URI(ns + "offline")))
                
            # Remove all the triples related to the virtual sib
            a.remove(Triple(URI(ns + virtual_sib_id), None, None))
            a.remove(Triple(None, None, URI(ns + virtual_sib_id)))

            # Update the virtualiser's load
            load_results = a.execute_rdf_query(Triple(URI(ns + virtualiser_id), URI(ns + "load"), None))
            print "LOAD" + str(load_results)
            virtualiser_load = int(str(load_results[0][2]))
            a.update(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(virtualiser_load-1))), Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(virtualiser_load))))
                
            confirm = {'return':'ok'}
            return confirm
       
    else:
        print "not found!"
        confirm = {'return':'fail', 'cause':' Virtualiser not found.'}
        return confirm


def NewVirtualMultiSIB(ancillary_ip, ancillary_port, sib_list):

    # debug info
    print colored("requests_handler> ", "blue", attrs=["bold"]) + str(sib_list)
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])

    # connection to the ancillary sib
    a = SibLib(ancillary_ip, ancillary_port)

    # check if the received sibs are all existing and alive
    sib_list_for_message = []
    for sib in sib_list:
        res = get_sib_ip_port(sib, a)
        if len(res) == 1:
            sib_dict = { "id" : sib, "ipport" : res[0][0][2]}
            sib_list_for_message.append(sib_dict)
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

    # a virtualiser has been selected
    if len(result) > 0:
        virtualiser_id = result[0][0][2].split("#")[1]
        virtualiser_ip = result[0][1][2].split("#")[1]
        virtualiser_port = int(result[0][2][2].split("#")[1])                
        virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # connect to the virtualiser
        try :
            virtualiser.connect((virtualiser_ip, virtualiser_port))
        except :
            print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser'
            confirm = {'return':'fail', 'cause':' Unable to connect to the virtualiser.'}
            return confirm

        # build request message 
        request_msg = {"command":"NewVirtualMultiSIB", "sib_list": sib_list_for_message}
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
                break
    
        # parse the reply
        confirm = json.loads(confirm_msg)
        if confirm["return"] == "fail":
            print colored("requests_handler> ", "red", attrs=["bold"]) + 'Creation failed!' + confirm["cause"]
            
        # the virtual multi sib has been created
        elif confirm["return"] == "ok":

            # get the virtual multi sib parameters
            virtual_multisib_id = confirm["virtual_multi_sib_info"]["virtual_multi_sib_id"]
            virtual_multisib_ip = confirm["virtual_multi_sib_info"]["virtual_multi_sib_ip"]
            virtual_multisib_kp_port = confirm["virtual_multi_sib_info"]["virtual_multi_sib_kp_port"]
            print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Virtual Multi SIB ' + virtual_multisib_id + ' started on ' + str(virtual_multisib_ip) + ":" + str(virtual_multisib_kp_port)

            # insert the triples into the ancillary sib        
            t = []
            print sib_list
            for i in sib_list:
                print i
                t.append(Triple(URI(ns + str(virtual_multisib_id)), URI(ns + "composedBy"), URI(ns + str(i))))    
            t.append(Triple(URI(ns + str(virtual_multisib_id)), URI(rdf + "type"), URI(ns + "virtualMultiSib")))
            t.append(Triple(URI(ns + str(virtual_multisib_id)), URI(ns + "hasKpIpPort"), URI(ns + str(virtualiser_ip) + "-" + str(virtual_multisib_kp_port))))
            t.append(Triple(URI(ns + str(virtual_multisib_id)), URI(ns + "hasStatus"), URI(ns + "online")))
            t.append(Triple(URI(ns + str(virtualiser_id)), URI(ns + "hasVirtualMultiSib"), URI(ns + str(virtual_multisib_id))))
            a.insert(t)

            for tripla in t:
                print tripla
            
            
        # close the connection with the virtualiser
        virtualiser.close()

        # return the confirm message
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
        WHERE {?s ns:hasKpIpPort ?o . ?s ns:hasStatus ns:online }
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
    
    print "SetSIBStatus " + "request handler: set status"
    
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
        print "SetSIBStatus " + colored("request_handler> ", "red", attrs = ["bold"]) + "failed to contact the virtual multi sibs"

    # Notify the status change to all the VMSIBs
    if len(vmsibs) > 0:
        
        print "SetSIBStatus " + "request handler: inoltro alle vmsib"

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
                        print "SetSIBStatus " + "aspetto conferma"
                        confirm_msg = vms_socket.recv(4096)
                        print "SetSIBStatus " + "conferma ricevuta"
                        break
                    except socket.timeout:
                        print "SetSIBStatus " + colored("request_handler> ", "red", attrs=["bold"]) + 'connection to the virtualiser timed out'            

                # Close the socket to the vmsib
                vms_socket.close()

            except:
                print "SetSIBStatus " + colored("request_handler> ", "red", attrs=["bold"]) + 'impossible to contact the virtualsib'            

    # return
    confirm = {'return':'ok'}
    
    print "SetSIBStatus " + "ritorno risposta (SetStatus)"
    return confirm


def AddSIBtoVMSIB(ancillary_ip, ancillary_port, vmsib_id, sib_list):

    # connect to the ancillary SIB
    a = SibLib(ancillary_ip, ancillary_port)

    # check if the vmsib really exists
    print "AddSIB: " + str(vmsib_id)
    res = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(rdf + "type"), URI(ns + "virtualMultiSib")))
    print "AddSIB: " + str(res)

    # if the vmsib exists
    if len(res) == 1:

        # get the list of all the SIBs
        SIBs = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s
WHERE {{ ?s rdf:type ns:remoteSib } UNION { ?s rdf:type ns:virtualMultiSib }}""")

        # extract only the SIBs
        existing_sibs = []
        for k in SIBs:
            existing_sibs.append(str(k[0][2]).split("#")[1])
        print "AddSIB: " + str(existing_sibs)

        # check if the specified sibs really exist and build a dict
        sib_list_for_message = {}
        for s in sib_list:
            if not(s in existing_sibs):
                confirm = {'return':'fail', 'cause':' SIB ' + str(s) + ' does not exist.'}
                print "AddSIB: a sib does not exist"
                return confirm
            else:
                r = a.execute_rdf_query(Triple(URI(ns + s), URI(ns + "hasKpIpPort"), None))
                sib_list_for_message[s] = {}
                sib_list_for_message[s]["ip"] = str(r[0][2]).split("-")[0]
                sib_list_for_message[s]["port"] = int(str(r[0][2]).split("-")[1])

        print 'AddSIB: all the sib exist'
        print 'AddSIB: ' + str(sib_list_for_message)

        # build the json msg for the VirtualMultiSib
        msg = { "command" : "AddSIBtoVMSIB", "sib_list" : sib_list_for_message, "vmsib_id" : vmsib_id }
        print 'AddSIB: ' + str(msg)
        jmsg = json.dumps(msg)

        # get the virtualmultisib parameters
        vms = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?o
WHERE { ns:""" + vmsib_id + """ ns:hasKpIpPort ?o }""")

        # send a message to the virtualmultisib
        try:
            # connection to the vmsib
            vms_ip = str(vms[0][0][2].split("#")[1]).split("-")[0]
            vms_port = str(vms[0][0][2].split("#")[1]).split("-")[1]
            vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print 'AddSIB: connecting to the vmsib',
            vms_socket.connect((vms_ip, int(vms_port)))
            print 'ok'
            print 'AddSIB: sending msg to the vmsib',
            vms_socket.send(jmsg)
            print 'ok'
            
            # wait for a reply
            while 1:
                try:
                    print 'AddSIB: waiting for a reply:',
                    confirm_msg = vms_socket.recv(4096)
                    print 'ok'
                    break
                except:
                    print colored("request_handler> ", "red", attrs=["bold"]) + 'Request to the vmsib failed'
                    confirm = {'return':'fail', 'cause':' Request to the vmsib failed.'}
                    vms_socket.close()
                    return confirm

            print 'AddSIB: ' + str(confirm_msg)

            # closing socket
            vms_socket.close()            

            # check the confirm content
            c = json.loads(confirm_msg)
            print 'AddSIB: ' + str(c)
            if c["return"] == "ok":

                # update the ancillary sib
                for s in sib_list:
                    a.insert(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + str(s))))
                
            # return
            return c

        except:
            print sys.exc_info()
            print colored("request_handler> ", "red", attrs=["bold"]) + 'request failed'      

    else:
        confirm = {'return':'fail', 'cause':' VirtualMultiSib does not exist.'}
        return confirm



def RemoveSIBfromVMSIB(ancillary_ip, ancillary_port, vmsib_id, sib_list):

    # connect to the ancillary sib
    a = SibLib(ancillary_ip, ancillary_port)

    # check if the vmsib really exists
    res = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(rdf + "type"), URI(ns + "virtualMultiSib")))

    # if vmsib exists
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

        # send a message to the vmsib
        try:
            print "contatto la vmsib",
            # connection to the vmsib
            vms_ip = str(vms[0][0][2].split("#")[1]).split("-")[0]
            vms_port = str(vms[0][0][2].split("#")[1]).split("-")[1]
            vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            vms_socket.connect((vms_ip, int(vms_port)))
            vms_socket.send(jmsg)

            # wait for a reply
            while 1:
                try:
                    print 'RemoveSIB: waiting for a reply:',
                    confirm_msg = vms_socket.recv(4096)
                    print 'ok'
                    break
                except:
                    print colored("request_handler> ", "red", attrs=["bold"]) + 'Request to the vmsib failed'
                    confirm = {'return':'fail', 'cause':' Request to the vmsib failed.'}
                    vms_socket.close()
                    return confirm

            print 'RemoveSIB: ' + str(confirm_msg)

            # closing socket
            vms_socket.close()            

            # check the confirm content
            c = json.loads(confirm_msg)
            print 'RemoveSIB: ' + str(c)
            if c["return"] == "ok":

                # update the ancillary sib
                for s in sib_list:
                    a.remove(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + str(s))))

                r = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), None))
                if len(r) == 0:
                    a.remove(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), None))
                    a.insert(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), URI(ns + "offline")))
                
            # return
            return c


        except:
            print sys.exc_info()
            print colored("request_handler> ", "red", attrs=["bold"]) + 'impossible to contact the VirtualMultiSib'      
            confirm = {'return':'fail', 'cause':' impossible to contact the vmsib.'}
            vms_socket.close()
            return confirm

    else:
        confirm = {'return':'fail', 'cause':' VirtualMultiSib does not exist.'}
        return confirm



def NewVirtualiser(ancillary_ip, ancillary_port, virtualiser_id, virtualiser_ip, virtualiser_port):

    # debug info
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing " + colored("NewVirtualiser", "cyan", attrs=["bold"]),

    # connection to the ancillary sib
    try:
        ancillary_sib = SibLib(ancillary_ip, ancillary_port)
    except:
        confirm = {"return":"fail", "cause":"Unable to connect to the ancillary SIB"}
        print confirm
        return confirm

    # build the triple to send
    triples = []
    triples.append(Triple(URI(ns + virtualiser_id), URI(rdf + "type"), URI(ns + "virtualiser")))
    triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(0))))
    triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasIP"), URI(ns + virtualiser_ip)))
    triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasPort"), URI(ns + str(virtualiser_port))))

    # insertion
    try:
        ancillary_sib.insert(triples)
    except:
        confirm = {"return":"fail", "cause":"Unable to insert the virtualiser triples into the ancillary SIB"}
        print confirm
        return confirm        

    # Everything's fine
    confirm = {"return":"ok"}
    print confirm
    return confirm

#########################################################################
#
# DeleteVirtualiser
#
#########################################################################

def DeleteVirtualiser(ancillary_ip, ancillary_port, virtualiser_id):

    # debug info
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing " + colored("DeleteVirtualiser", "cyan", attrs=["bold"]),

    # connection to the ancillary sib
    try:
        ancillary_sib = SibLib(ancillary_ip, ancillary_port)
    except:
        confirm = {"return":"fail", "cause":"Unable to connect to the ancillary SIB"}
        print confirm
        return confirm

    ##############################################################
    #
    #  Delete all the virtual sibs
    #
    ##############################################################
    
    # get the list of the virtual sibs started on that virtualiser
    vsibs_query = PREFIXES + """SELECT ?vsib_id
    WHERE { ns:""" + virtualiser_id + """ ns:hasRemoteSib ?vsib_id }"""
    vsibs = ancillary_sib.execute_sparql_query(vsibs_query)

    for vsib in vsibs:
        print vsib
        ancillary_sib.remove(Triple(URI(vsib[0][2]), None, None))
        ancillary_sib.remove(Triple(None, None, URI(vsib[0][2])))

        # Notify the deletion of the vsib to the vmsibs that use them
        # just like DeleteRemoteSIB does

        # get the list of the virtual multi sib in which that virtual sib appears
        query = PREFIXES + """SELECT ?id ?ipport 
WHERE { ?id ns:composedBy ns:""" + str(vsib[0][2].split("#")[1]) + """ . ?id ns:hasKpIpPort ?ipport }"""
        result = ancillary_sib.execute_sparql_query(query)
        if len(result) > 0:

            # build RemoveSIBfromVMSIB message
            msg = json.dumps({"command":"RemoveSIBfromVMSIB", "sib_list":[str(vsib[0][2].split("#")[1])]})

            # send the RemoveSIBfromVMSIB request to all the vmsibs

            for multisib in result:

                # get vmsib parameters
                vmsib_id = multisib[0][2].split("#")[1]
                vmsib_ip = multisib[1][2].split("#")[1].split("-")[0]
                vmsib_port = int(multisib[1][2].split("#")[1].split("-")[1])
                
                # connect to the vmsib
                vmsib = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
                # connect to the vmsib
                try:
                    vmsib.connect((vmsib_ip, vmsib_port))                  
                except:
                    print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the vmsib'
                    confirm = {'return':'fail', 'cause':' Unable to connect to the vmsib.'}
                    return confirm
            
                # send the request to the vmsib
                try:
                    # build the message
                    vmsib.send(msg)
                except:
                    print colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to send the request to the vmsib'
                    confirm = {'return':'fail', 'cause':' Unable to send the request to the vmsib.'}
                    return confirm
        
                # wait for a reply
                try:
                    while 1:
                        confirm_msg = vmsib.recv(4096)
                        confirm_msg = json.loads(confirm_msg)

                        if confirm_msg:
                            print colored("requests_handler> ", "blue", attrs=["bold"]) + 'Received the following message:'
                            vmsib.close()
                            break                    

                except:
                    print colored("requests_handler> ", "red", attrs=['bold']) + 'Reply not received from the vmsib'
                    confirm = {'return':'fail', 'cause':' Reply not received from the vmsib'}
                    return confirm                    

                # eventually update the status
                if confirm_msg["return"] == "fail":
                    print colored("requests_handler> ", "red", attrs=['bold']) + 'Negative reply from vmsib'
                    confirm = {'return':'fail', 'cause':' Negative reply from vmsib'}
                    return confirm                    
                else:
                    ancillary_sib.remove(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + vsib[0][2].split("#")[1])))
                    r = ancillary_sib.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), None))
                    if len(r) == 0:
                        ancillary_sib.remove(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), None))
                        ancillary_sib.insert(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), URI(ns + "offline")))

    ##############################################################
    #
    #  Delete all the virtual multi sibs
    #
    ##############################################################

    # get the list of the virtual sibs started on that virtualiser
    vmsibs_query = PREFIXES + """SELECT ?vmsib_id
    WHERE { ns:""" + virtualiser_id + """ ns:hasVirtualMultiSib ?vmsib_id }"""
    vmsibs = ancillary_sib.execute_sparql_query(vmsibs_query)

    # for each vmsib, recreate it on another virtualiser
#     for vmsib in vmsibs:

#         print "VirtualMultiSIB: " + str(vmsib[0][2])

#         component_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX ns: <http://smartM3Lab/Ontology.owl#>
# SELECT ?sibid
#     WHERE { """ + vmsib[0][2] + """ rdf:type ns:virtualMultiSib . """ + vmsib[0][2] + """ ns:composedBy ?sibid}"""
    
#         component_results = ancillary_sib.execute_sparql_query(component_query)
#         print component_results



    ##############################################################
    #
    #  Delete all the data related to that virtualiser
    #
    ##############################################################

    # get the triples related to that virtualiser
    triples = ancillary_sib.execute_rdf_query(Triple(URI(ns + virtualiser_id), None, None))
    for t in triples:
        print t

    # deletion
    try:
        print "Rimozione triple",
        ancillary_sib.remove(triples)
        print 'ok'

        print ancillary_sib.execute_rdf_query(triples[0])

    except:
        confirm = {"return":"fail", "cause":"Unable to delete the virtualiser triples from the ancillary SIB"}
        print sys.exc_info()
        print traceback.print_exc()
        print confirm
        return confirm        

    # Everything's fine
    confirm = {"return":"ok"}
    print confirm
    return confirm


