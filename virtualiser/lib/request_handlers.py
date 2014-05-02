#!/usr/bin/python

# requirements
from multiprocessing import Process
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

#functions

def NewRemoteSIB(owner, virtualiser_ip, threads, thread_id, virtualiser_id, ancillary_ip, ancillary_port):
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
        if s1.connect_ex(("localhost", kp_port)) != 0:
            print "estratta la porta %s"%(str(kp_port))
            break

    while True:
        pub_port = random.randint(10000, 11000)
        if pub_port != kp_port:
            if s2.connect_ex(("localhost", pub_port)) != 0:
                print "estratta la porta %s"%(str(pub_port))
                break        
    
    # insert information in the ancillary SIB
    try:
        a = SibLib(ancillary_ip, ancillary_port)
        t = [Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIpPort"), URI(ns + str(virtualiser_ip) + "-" + str(pub_port)))]
        t.append(Triple(URI(ns + str(virtual_sib_id)), URI(rdf + "type"), URI(ns + "remoteSib")))
        t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpIpPort"), URI(ns + str(virtualiser_ip) + "-" + str(kp_port))))
        t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasOwner"), URI(ns + str(owner))))
        t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasStatus"), URI(ns + "offline")))
        t.append(Triple(URI(ns + str(virtualiser_id)), URI(ns + "hasRemoteSib"), URI(ns + str(virtual_sib_id))))
        a.insert(t)
        
        virtual_sib_info = {}
        virtual_sib_info["return"] = "ok"
        virtual_sib_info["virtual_sib_id"] = str(virtual_sib_id)
        virtual_sib_info["virtual_sib_ip"] = str(virtualiser_ip)
        virtual_sib_info["virtual_sib_pub_port"] = pub_port
        virtual_sib_info["virtual_sib_kp_port"] = kp_port
        virtual_sib_info["virtual_sib_owner"] = str(owner)

        # start a virtual sib (nel try, in quanto va fatto solo se
        # l'inserimento delle informazioni e' andato a buon fine)
        
        ### thread.start_new_thread(virtualiser, (kp_port, pub_port, virtual_sib_id))
        threads[thread_id] = True
        p = Process(target=remoteSIB, args=(virtualiser_ip, kp_port, pub_port, virtual_sib_id, threads[thread_id], ancillary_ip, ancillary_port))
        p.start()

        # t = thread.start_new_thread(virtualiser, (kp_port, pub_port, virtual_sib_id, threads[thread_id]))
        
        # return virtual sib id
        return virtual_sib_info

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


def DeleteRemoteSIB(virtual_sib_id, threads, t_id, virtualiser_id, ancillary_ip, ancillary_port):
    try:
        # remove virtual sib info from the ancillary sib
        a = SibLib(ancillary_ip, ancillary_port)

        t = Triple(URI(ns + virtual_sib_id), None, None)
        result = a.execute_rdf_query(t)  
        print result
        a.remove(result)

        t = [Triple(URI(ns + str(virtualiser_id)), URI(ns + "hasRemoteSib"), URI(ns + virtual_sib_id))]
        a.remove(t)
        print colored("virtualiser_server> ", "blue", attrs=["bold"]) + 'Triples deleted!'

        #killare il thread virtualiser lanciato all'interno del metodo NewRemoteSib
        threads[t_id[virtual_sib_id]] = False
        print colored("virtualiser_server> ", "blue", attrs=["bold"]) + 'Virtual Sib ' + virtual_sib_id + ' killed ' 



        #############################################
        ##                                         ##
        ## Update the load of selected virtualiser ##
        ##                                         ##
        #############################################
        # get old load
        try:
            query = """SELECT ?load
WHERE { ns:""" + str(virtualiser_id) + """ ns:load ?load }"""

            result = a.execute_sparql_query(query)
            load = int(result[0][0][2])
            print "Old Load " + str(load)
            
            # remove triple
            t = []
            t.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(load))))
            a.remove(t)
            # insert new triple
            load -= 1
            print "New Load " + str(load)
            t = []
            t.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(load))))
            a.insert(t)
        except socket.error:
            print colored("request_handlers> ", "red", attrs=['bold']) + 'Unable to connect to the ancillary SIB'
            confirm = {'return':'fail', 'cause':' Unable to connect to the ancillary SIB.'}
            return confirm

        #############################################
        #############################################


        confirm = {'return':'ok'}
        
    except socket.error:
        print colored("request_handlers> ", "red", attrs=['bold']) + 'Unable to connect to the ancillary SIB'
        confirm = {'return':'fail', 'cause':' Unable to connect to the ancillary SIB.'}

    return confirm
                                        

    
def NewVirtualMultiSIB(sib_list, virtualiser_ip, virtualiser_id, threads, thread_id, ancillary_ip, ancillary_port):
    print colored("request_handlers> ", "blue", attrs=["bold"]) + str(sib_list)
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])
    # virtual multi sib id
    virtual_multi_sib_id = str(uuid.uuid4())

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

    
    
    # insert information in the ancillary SIB
    try:
        a = SibLib(ancillary_ip, ancillary_port)
        t = []
        for i in sib_list:
            t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(ns + "composedBy"), URI(ns + str(i))))
        
        t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(rdf + "type"), URI(ns + "virtualMultiSib")))
        t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(ns + "hasKpIpPort"), URI(ns + str(virtualiser_ip) + "-" + str(kp_port))))
        t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(ns + "hasPubIpPort"), URI(ns + str(virtualiser_ip) + "-" + str(pub_port))))
        t.append(Triple(URI(ns + str(virtualiser_id)), URI(ns + "hasRemoteSib"), URI(ns + str(virtual_multi_sib_id))))
        t.append(Triple(URI(ns + str(virtual_multi_sib_id)), URI(ns + "hasStatus"), URI(ns + "offline")))
        t.append(Triple(URI(ns + str(virtualiser_id)), URI(ns + "hasVirtualMultiSib"), URI(ns + str(virtual_multi_sib_id))))
        a.insert(t)

        virtual_multi_sib_info = {}
        virtual_multi_sib_info["virtual_multi_sib_id"] = str(virtual_multi_sib_id)
        virtual_multi_sib_info["virtual_multi_sib_ip"] = str(virtualiser_ip)
        virtual_multi_sib_info["virtual_multi_sib_kp_port"] = kp_port

        # start a virtual multi sib (nel try, in quanto va fatto solo se
        # l'inserimento delle informazioni e' andato a buon fine)
        
        threads[thread_id] = True
        p = Process(target=virtualMultiSIB, args=(virtualiser_ip, kp_port, pub_port, virtual_multi_sib_id, threads[thread_id], sib_list, ancillary_ip, ancillary_port))
        p.start()

        # return virtual multi sib id
        return virtual_multi_sib_info

    except socket.error:
        virtual_multi_sib_info = {}
        virtual_multi_sib_info["return"] = "fail"
        virtual_multi_sib_info["cause"] = "Connection to Ancillary Sib failed"
        return virtual_multi_sib_info
    except Exception, e: #TODO catturare qui i sibError
        print 'ECCEZIONE: ' + str(e)
        virtual_multi_sib_info = {}
        virtual_multi_sib_info["return"] = "fail"
        virtual_multi_sib_info["cause"] = "Sib Error"
        return virtual_multi_sib_info

def Discovery(ancillary_ip, ancillary_port):
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("Discovery", "cyan", attrs=["bold"])
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
