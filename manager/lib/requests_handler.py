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
from output_helpers import *
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
        t = [Triple(URI(ns + str(sib_id)), URI(rdf + "type"), URI(ns + "publicSib"))]
        t.append(Triple(URI(ns + str(sib_id)), URI(ns + "hasKpIp"), Literal(str(sib_ip))))
        t.append(Triple(URI(ns + str(sib_id)), URI(ns + "hasKpPort"), Literal(str(sib_port))))
        t.append(Triple(URI(ns + str(sib_id)), URI(ns + "hasOwner"), Literal(owner)))
        t.append(Triple(URI(ns + str(sib_id)), URI(ns + "hasStatus"), Literal("online")))
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
    except Exception, e: #TODO catch sib error!
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
        virtualiser_ip = result[0][1][2]
        virtualiser_port = int(result[0][2][2])

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
                a.remove([Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpIp"), None), 
                          Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpPort"), None), 
                          Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIp"), None),
                          Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubPort"), None),
                          Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasStatus"), None)])

                # add the new triples
                t = []
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIp"), Literal(str(virtualiser_ip))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubPort"), Literal(str(virtual_sib_pub_port))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpIp"), Literal(str(virtualiser_ip))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpPort"), Literal(str(virtual_sib_kp_port))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasOwner"), Literal(str(owner))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasStatus"), Literal("offline")))
                t.append(Triple(URI(ns + str(virtualiser_id)), URI(ns + "hasRemoteSib"), URI(ns + str(virtual_sib_id))))
                t.append(Triple(URI(ns + str(virtual_sib_id)), URI(rdf + "type"), URI(ns + "virtualSib")))
                a.insert(t)
                print 'INFORMAZIONI INSERITE'
                #############################################
                ##                                         ##
                ## Update the load of selected virtualiser ##
                ##                                         ##
                #############################################

                # get old load
                load = get_virtualiser_load(virtualiser_id, a)
                
                # remove triple
                t = []
                t.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(load))))
                a.remove(t)
                # insert new triple
                load += 1
                t = []
                t.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(load))))
                a.insert(t)

            except socket.error:
                virtual_sib_info = {}
                virtual_sib_info["return"] = "fail"
                virtual_sib_info["cause"] = "Connection to Ancillary Sib failed"
                return virtual_sib_info
            except Exception, e: #TODO catch sib Error
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
                t4 = Triple(URI(vmsib[0]), URI(ns + "hasStatus"), Literal("offline"))
                r = a.execute_rdf_query(t3)
                a.remove(r)
                a.insert(t4)

        # remove the triples related to the public SIB
        t = Triple(URI(ns + sib_id), None, None)
        result = a.execute_rdf_query(t)  
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
    info = get_virtualiser_info(virtual_sib_id, a)
    if info != None:
        
        virtualiser_ip = info["virtualiser_ip"]
        virtualiser_port = info["virtualiser_port"]
        virtualiser_id = info["virtualiser_id"]


        # Connect to the virtualiser
        virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # connect to the virtualiser
        try:
            virtualiser.connect((virtualiser_ip, int(virtualiser_port)))                  
        except:
            print "DeleteRemoteSIB " + colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser '
            clear_ancillary(ancillary_ip, ancillary_port, virtual_sib_id, virtualiser_id)
    
        # send the request to the virtualiser
        try:
            # build the message
            msg = json.dumps({"command" : "DeleteRemoteSIB", "virtual_sib_id" : str(virtual_sib_id)})
            virtualiser.send(msg)
        except:
            print "DeleteRemoteSIB " + colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to send the request to the virtualiser  '

        # wait for a reply
        try:
            while 1:
                confirm_msg = virtualiser.recv(4096)
                if confirm_msg:
                    print colored("requests_handler> CONFIRM ", "blue", attrs=["bold"]) + 'Received the following message:'
                    virtualiser.close()
                    break                
        except:
            print "DeleteRemoteSIB " + colored("requests_handler> ", "red", attrs=['bold']) + 'Reply not received from the virtualiser '
        
        # check the confirm
        confirm = json.loads(confirm_msg)
        if confirm["return"] == "fail":            
            print colored("requests_handler> ", "red", attrs=["bold"]) + 'Deletion failed!' + confirm["cause"]
            return confirm

        else:
            print "DeleteRemoteSIB " + "Virtualsib killed  "
            
            reply = manage_multi_sib(ancillary_ip, ancillary_port, virtual_sib_id)
            if reply["return"] == "fail":
                # esiste una virtual multi sib composta dalla virtual
                # sib in questione, ma non sono riuscito per quanche
                # motivo ad aggiornare: che faccio? proseguo comunque
                # con l'eliminazione delle triple della virtual sib? E
                # poi che succede con le triple "composed by" rimaste
                # nell'ancillary? La multi rimarra' esistente e
                # disponibile ma ci sono delle sottosib inesistenti!
                confirm = {'return':'fail', 'cause':'ancillary not update!'}
                return confirm
            else:
                reply = clear_ancillary(ancillary_ip, ancillary_port, virtual_sib_id, virtualiser_id)
                if reply["return"] == "ok":
                    confirm = {'return':'ok'}
                    return confirm
                else:
                    confirm = {'return':'fail', 'cause':'ancillary not update!'}
                    return confirm
    else:
        print "DeleteRemoteSIB " + "not found!"

        reply = manage_multi_sib(ancillary_ip, ancillary_port, virtual_sib_id)
        if reply["return"] == "fail":
            # esiste una virtual multi sib composta dalla virtual
            # sib in questione, ma non sono riuscito per quanche
            # motivo ad aggiornare: che faccio? proseguo comunque
            # con l'eliminazione delle triple della virtual sib? E
            # poi che succede con le triple "composed by" rimaste
            # nell'ancillary? La multi rimarra' esistente e
            # disponibile ma ci sono delle sottosib inesistenti!
            confirm = {'return':'fail', 'cause':'ancillary not update!'}
            return confirm
        
        else:
            reply = clear_ancillary(ancillary_ip, ancillary_port, virtual_sib_id, None)
            if reply["return"] == "ok":
                confirm = {'return':'ok'}
                return confirm
            else:
                confirm = {'return':'fail', 'cause':'ancillary not update!'}
                return confirm



def manage_multi_sib(ancillary_ip, ancillary_port, virtual_sib_id):

    a = SibLib(ancillary_ip, ancillary_port)
    # get the list of the virtual multi sib in which that virtual sib appear
    vmsib_list = get_vmsib_list(virtual_sib_id, a)

    if vmsib_list != None:

        for multisib in vmsib_list:
            vmsib_id = multisib
            vmsib_ip = multisib["vmsib_ip"]
            vmsib_port = int(multisib["vmsib_port"])
        
            # build RemoveSIBfromVMSIB message
            msg = json.dumps({"command":"RemoveSIBfromVMSIB", "sib_list":[str(virtual_sib_id)], "vmsib_id" : vmsib_id })

            # connect to the vmsib
            print "DeleteRemoteSIB " + "connecting to the vmsib  "
            vmsib = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
            # connect to the vmsib
            try:
                vmsib.connect((vmsib_ip, vmsib_port))                  
                print "DeleteRemoteSIB " + "Connected to the vmsib"
            except:
                print "DeleteRemoteSIB " + colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to connect to the vmsib'
        
            # send the request to the vmsib
            try:
                # build the message
                vmsib.send(msg)
                print "DeleteRemoteSIB " + "RemoveSIBfromVMSIB request sent to the vmsib"
            except:
                print "DeleteRemoteSIB " + colored("requests_handler> ", "red", attrs=['bold']) + 'Unable to send the request to the vmsib'
    
            # wait for a reply
            try:
                while 1:
                    confirm_msg = vmsib.recv(4096)
                    confirm_msg = json.loads(confirm_msg)
                    print "DeleteRemoteSIB " + "Response received"
                    if confirm_msg:
                        print "DeleteRemoteSIB " + colored("requests_handler> ", "blue", attrs=["bold"]) + 'Received the following message:'
                        vmsib.close()
                        break                    

            except:
                print "DeleteRemoteSIB " + colored("requests_handler> ", "red", attrs=['bold']) + 'Reply not received from the vmsib'

            # eventually update the status
            if confirm_msg["return"] == "fail":
                print "DeleteRemoteSIB " + colored("requests_handler> ", "red", attrs=['bold']) + 'Negative reply from vmsib'
                confirm = {'return':'fail', 'cause':' Negative reply from vmsib'}
                return confirm                    
            else:
                a.remove(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + virtual_sib_id)))
                r = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), None))
                if len(r) == 0:
                    a.remove(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), None))
                    a.insert(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), URI(ns + "offline")))


        confirm = {'return':'ok'}
        return confirm

    # if don't exists any multi sib composed by this virtual sib
    else:
        confirm = {'return':'ok'}
        return confirm


def clear_ancillary(ancillary_ip, ancillary_port, virtual_sib_id, virtualiser_id):
    try:
        a = SibLib(ancillary_ip, ancillary_port)
                          
        # Remove all the triples related to the virtual sib
        a.remove(Triple(URI(ns + virtual_sib_id), None, None))
        a.remove(Triple(None, None, URI(ns + virtual_sib_id)))
        
        # Update the virtualiser's load
        load_results = a.execute_rdf_query(Triple(URI(ns + virtualiser_id), URI(ns + "load"), None))
        virtualiser_load = int(str(load_results[0][2]))
        print "DeleteRemoteSIB " + "Virtualiser old load: " + str(virtualiser_load)
        print "DeleteRemoteSIB " + "Virtualiser new load: " + str(int(virtualiser_load)-1)
        a.update(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(virtualiser_load-1))), Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(virtualiser_load))))
        confirm = {'return':'ok'}
        return confirm
    except:
        confirm = {'return':'fail', 'cause':'unabled to connect to the ancillary sib'}
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
            sib_dict = { "id" : sib, "ipport" : str(res[0][0][2] + "-" + res[0][1][2])}
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
        virtualiser_ip = result[0][1][2]
        virtualiser_port = int(result[0][2][2])
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
            t.append(Triple(URI(ns + str(virtual_multisib_id)), URI(ns + "hasKpIp"), Literal(str(virtualiser_ip))))
            t.append(Triple(URI(ns + str(virtual_multisib_id)), URI(ns + "hasKpPort"), Literal(str(virtual_multisib_kp_port))))
            t.append(Triple(URI(ns + str(virtual_multisib_id)), URI(ns + "hasStatus"), Literal("online")))
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
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DiscoveryAll", "cyan", attrs=["bold"])
    # query to the ancillary sib to get all the existing virtual sib, public sib and virtual multi sib
    a = SibLib(ancillary_ip, ancillary_port)
    sib_list = get_all_online_sibs(a)
    return sib_list


def DiscoveryWhere(ancillary_ip, ancillary_port, sib_profile):
    # debug print
    print colored("requests_handler> ", "blue", attrs=["bold"]) + "executing method " + colored("DiscoveryWhere", "cyan", attrs=["bold"])
    
    key = str(sib_profile.split(":")[0])
    value = str(sib_profile.split(":")[1])

    # query to the ancillary sib to get all the reachable sibs with key value = value
    a = SibLib(ancillary_ip, ancillary_port)
    sib_list = get_all_online_sibs_by_key(a, key, value)
    return sib_list
    

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
        old_status = old_status[0][2]
    except:
        confirm = {'return':'fail', 'cause':' query failed.'}
        return confirm

    # Setting the status
    try:
        if (str(old_status).lower() != str(new_status).lower()) and (str(new_status).lower() in ["online", "offline"]):
            new_triple = Triple(URI(ns + str(sib_id)), URI(ns + "hasStatus"), Literal(new_status))
            old_triple = Triple(URI(ns + str(sib_id)), URI(ns + "hasStatus"), Literal(old_status))
            a.update(new_triple, old_triple)
        else:
            # Nothing to do ...
            confirm = {'return':'ok'}
            return confirm
    except:
        confirm = {'return':'fail', 'cause':' Connection to the virtualiser timed out.'}
        return confirm

    # Look for the VMSIBs using this SIB
    try:
        vmsib_list = get_vmsib_list(sib_id, a)
    except:
        # Query failed
        print "SetSIBStatus " + colored("request_handler> ", "red", attrs = ["bold"]) + "Query failed"

    if vmsib_list != None:
        
        # Build the JSON message
        new_status_msg = {"command": "StatusChange", "sib_id": sib_id, "status": new_status}
        new_status_json_msg = json.dumps(new_status_msg)

        
        for multisib in vmsib_list:
            vms_id = multisib
            vms_ip = vmsib_list[multisib]["vmsib_ip"]
            vms_port = int(vmsib_list[multisib]["vmsib_port"])
            try:
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
    return confirm



def AddSIBtoVMSIB(ancillary_ip, ancillary_port, vmsib_id, sib_list):

    # connect to the ancillary SIB
    a = SibLib(ancillary_ip, ancillary_port)

    # check if the vmsib really exists
    res = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(rdf + "type"), URI(ns + "virtualMultiSib")))

    # if the vmsib exists
    if len(res) == 1:

        # get the list of all the SIBs
        existing_sibs = get_all_sibs(a)

        # check if the specified sibs really exist and build a dict
        sib_list_for_message = {}
        for s in sib_list:
            if not(s in existing_sibs):
                confirm = {'return':'fail', 'cause':' SIB ' + str(s) + ' does not exist.'}
                return confirm
            else:
                ip = a.execute_rdf_query(Triple(URI(ns + s), URI(ns + "hasKpIp"), None))[0][2]
                port = a.execute_rdf_query(Triple(URI(ns + s), URI(ns + "hasKpPort"), None))[0][2]
                sib_list_for_message[s] = {"ip" : str(ip), "port" : str(port) }


        # build the json msg for the VirtualMultiSib
        msg = { "command" : "AddSIBtoVMSIB", "vmsib_id" : vmsib_id, "sib_list" : sib_list_for_message }
        jmsg = json.dumps(msg)
        
        # get the virtualmultisib parameters
        vms = get_sib_info(vmsib_id, a)

        try:
            # connection to the vmsib
            vms_ip = vms["sib_ip"]
            vms_port = vms["sib_port"]
            vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            vms_socket.connect((vms_ip, int(vms_port)))
            vms_socket.send(jmsg)

            # wait for a reply
            while 1:
                try:
                    confirm_msg = vms_socket.recv(4096)
                    break
                except:
                    print colored("request_handler> ", "red", attrs=["bold"]) + 'Request to the vmsib failed'
                    confirm = {'return':'fail', 'cause':' Request to the vmsib failed.'}
                    vms_socket.close()
                    return confirm

            # closing socket
            vms_socket.close()            

            # check the confirm content
            c = json.loads(confirm_msg)
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

            existing_sibs = get_all_sibs(a)
        except:
            print "connection failed to the ancillary sib"

        # check if the specified sibs really exist
        for s in sib_list:
            if not(s in existing_sibs):
                confirm = {'return':'fail', 'cause':' SIB ' + str(s) + ' does not exist.'}
                return confirm
            
        # build the json msg for the VirtualMultiSib
        msg = { "command" : "RemoveSIBfromVMSIB", "sib_list" : sib_list, "vmsib_id" : vmsib_id }
        jmsg = json.dumps(msg)

        # get the virtualmultisib parameters
        vms_info = get_sib_info(vmsib_id, a)
        vms_ip = vms_info["sib_ip"]
        vms_port = vms_info["sib_port"]

        # send a message to the vmsib
        try:
            # connection to the vmsib
            vms_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            vms_socket.connect((vms_ip, int(vms_port)))
            vms_socket.send(jmsg)

            # wait for a reply
            while 1:
                try:
                    confirm_msg = vms_socket.recv(4096)
                    break
                except:
                    print colored("request_handler> ", "red", attrs=["bold"]) + 'Request to the vmsib failed'
                    confirm = {'return':'fail', 'cause':' Request to the vmsib failed.'}
                    vms_socket.close()
                    return confirm

            # closing socket
            vms_socket.close()            

            # check the confirm content
            c = json.loads(confirm_msg)
            if c["return"] == "ok":

                # update the ancillary sib
                for s in sib_list:
                    a.remove(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), URI(ns + str(s))))

                r = a.execute_rdf_query(Triple(URI(ns + vmsib_id), URI(ns + "composedBy"), None))
                if len(r) == 0:
                    a.remove(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), None))
                    a.insert(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), Literal("offline")))
                
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


#########################################################################
#
# NewVirtualiser
#
#########################################################################

def NewVirtualiser(ancillary_ip, ancillary_port, virtualiser_id, virtualiser_ip, virtualiser_port):

    # debug info
    print requests_print(True) + "executing " + command_print("NewVirtualiser")

    # connection to the ancillary sib
    try:
        ancillary_sib = SibLib(ancillary_ip, ancillary_port)
    except:
        confirm = {"return":"fail", "cause":"Unable to connect to the ancillary SIB"}
        return confirm

    # build the triple to send
    triples = []
    triples.append(Triple(URI(ns + virtualiser_id), URI(rdf + "type"), URI(ns + "virtualiser")))
    triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(0))))
    triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasIP"), Literal(virtualiser_ip)))
    triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasPort"), Literal(str(virtualiser_port))))

    # insertion
    try:
        ancillary_sib.insert(triples)
    except:
        confirm = {"return":"fail", "cause":"Unable to insert the virtualiser triples into the ancillary SIB"}
        return confirm        

    # Everything's fine
    confirm = {"return":"ok"}
    return confirm

#########################################################################
#
# DeleteVirtualiser
#
#########################################################################

def DeleteVirtualiser(ancillary_ip, ancillary_port, virtualiser_id):

    # debug info
    print requests_print(True) + "executing " + command_print("DeleteVirtualiser")

    # connection to the ancillary sib
    try:
        ancillary_sib = SibLib(ancillary_ip, ancillary_port)
    except:
        confirm = {"return":"fail", "cause":"Unable to connect to the ancillary SIB"}
        return confirm

    ##############################################################
    #
    #  Delete all the virtual sibs from the vmsibs and from the ancillary
    #
    ##############################################################
    
    # debug print
    print requests_print(True) + "removing all the virtual sibs running on the virtualiser"

    # get the list of the virtual sibs started on that virtualiser
    vsibs = get_sibs_on_virtualiser(virtualiser_id, ancillary_sib)
    for vsib in vsibs:
        ancillary_sib.remove(Triple(URI(vsib), None, None))
        ancillary_sib.remove(Triple(None, None, URI(vsib)))
    
        # get the list of the virtual multi sib in which that virtual sib appears
        vmsib_list = get_vmsib_list(str(vsib.split("#")[1]), ancillary_sib)
        
        if vmsib_list != None:
            # build RemoveSIBfromVMSIB message
            msg = json.dumps({"command":"RemoveSIBfromVMSIB", "sib_list":[str(vsib[0][2].split("#")[1])]})

            # send the RemoveSIBfromVMSIB request to all the vmsibs
            for multisib in vmsib_list:

                # get vmsib parameters
                vmsib_id = multisib
                vmsib_ip = vmsib_list[multisib]["vmsib_ip"]
                vmsib_port = int(vmsib_list[multisib]["vmsib_port"])
                
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
                        ancillary_sib.insert(Triple(URI(ns + vmsib_id), URI(ns + "hasStatus"), Literal("offline")))

    ##############################################################
    #
    #  Delete all the virtual multi sibs
    #
    ##############################################################

    # debug print
    print requests_print(True) + "removing all the virtual multi sib running on the virtualiser"

    # get the list of the virtual multi sibs started on that virtualiser
    vmsibs = get_multisibs_on_virtualiser(virtualiser_id, ancillary_sib)

    # for all vmsib in vmsibs, delete it!
    for vmsib in vmsibs:
        
        # get all the triples related to that virtual multi sib
        ancillary_sib.remove(Triple(URI(vmsib), None, None))

    ##############################################################
    #
    #  Delete all the data related to that virtualiser
    #
    ##############################################################

    # debug print
    print requests_print(True) + "removing all the triples related to the virtualiser"

    # get the triples related to that virtualiser
    try:
        ancillary_sib.remove(Triple(URI(ns + virtualiser_id), None, None))

    except:
        confirm = {"return":"fail", "cause":"Unable to delete the virtualiser triples from the ancillary SIB"}
        print confirm
        return confirm        

    # Everything's fine
    confirm = {"return":"ok"}
    print confirm
    return confirm



def GetSIBStatus(ancillary_ip, ancillary_port, sib_id):

    # debug info
    print requests_print(True) + "executing " + command_print("GetSIBStatus")    
    
    # Connecting to the ancillary SIB
    try:
        a = SibLib(ancillary_ip, int(ancillary_port))
    except:
        confirm = {'return':'fail', 'cause':' connection to the ancillary SIB failed.'}
        return confirm

    # Executing a query
    res = a.execute_rdf_query(Triple(URI(ns + sib_id), URI(ns + "hasStatus"), None))

    # Checking results
    if len(res) == 0:
        
        # the sib does not exist
        confirm = {"return":"ok", "status":"none"}

    else:

        # the sib exists
        confirm = {"return":"ok", "status":str(res[0][2]).split("#")[1]}

    # Return
    return confirm



def MultiSIBInfo(ancillary_ip, ancillary_port, multi_sib_id):

    multisib_info = {}
    multisib_info["components"] = {}
    multisib_info["others"] = {}

    a = SibLib(ancillary_ip, int(ancillary_port))
    
    try:
        # Query to get all the components of the multi sib
        t = Triple(URI(ns + str(multi_sib_id)), URI(ns + "composedBy"), None)
        result = a.execute_rdf_query(t)
        for i in result:
            id = str(i[2]).split("#")[1]
            t = Triple(URI(ns + str(id)), URI(ns + "hasOwner"), None)
            res = a.execute_rdf_query(t)
            if len(res)==0:
                owner = "VM SIB"
            else:
                owner = str(res[0][2])
            multisib_info["components"][id] = owner
    
    
        # Query to get all the sibs and multi sib but not the multi sib with msib_id
        query = PREFIXES + """ SELECT ?id ?owner 
        WHERE {{?id ns:hasKpIp ?ip . 
                ?id ns:hasKpPort ?port .
                ?id ns:hasStatus "online" . 
                ?id rdf:type ns:publicSib . 
                ?id ns:hasOwner ?owner .
                }
              UNION
               {?id ns:hasKpIp ?ip . 
                ?id ns:hasKpPort ?port .
                ?id ns:hasStatus "online" . 
                ?id rdf:type ns:virtualSib . 
                ?id ns:hasOwner ?owner .
                }
              UNION
               {?id ns:hasKpIp ?ip .
                ?id ns:hasKpPort ?port . 
                ?id ns:hasStatus "online" . 
                ?id rdf:type ns:virtualMultiSib . 
               	FILTER(?id != ns:""" + str(multi_sib_id) + """)}}"""
       
        result = a.execute_sparql_query(query)
    
        for i in result:
            id = str(i[0][2]).split("#")[1]
    
            # get owner
            if i[1][2] == None:
                owner = "VM SIB"
            else:
                owner = str(i[1][2])
    
            if id not in multisib_info["components"]:
    
                multisib_info["others"][id] = owner

        confirm = {"return":"ok", "multisib_info":multisib_info}
        return confirm 

    except:
        # the sib does not exist
        confirm = {"return":"fail", "cause":"Failed to connect to the ancillary sib"}
        return confirm
    

