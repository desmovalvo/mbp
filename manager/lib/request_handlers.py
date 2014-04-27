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
ancillary_ip = "10.143.250.58"
ancillary_port = 10088
#functions

def NewRemoteSIB():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewRemoteSIB", "cyan", attrs=["bold"])
    
    # TODO: mandare messaggio json al server meno carico:
    # # query all'ancillary sib per sapere qual e' il serer meno carico
    # # virtualiser: server scelto
    # # virtualiser_ip e vitualiser_port: ip e porta del server scelto
    # # invio richiesta json di creazione della virtual sib
    # socket to the virtualiser process
    
    # query to the ancillary SIB 
    a = SibLib(ancillary_ip, ancillary_port)
    
    # questa query restituisce il server meno carico. Potrebbero
    # esserci piu' server con lo stesso carico restituiti: ne
    # scegliamo uno a caso
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
    
    result = a.execute_sparql_query(query)
    if result != None:
        virtualiser_id = result[0][0][2].split("#")[1]
        virtualiser_ip = result[0][1][2].split("#")[1]
        virtualiser_port = int(result[0][2][2].split("#")[1])
        print result
        print virtualiser_id 
        print virtualiser_ip 
        print str(virtualiser_port)
        

    # virtualiser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # virtualiser.settimeout(2)
         
    # # connect to the virtualiser
    # try :
    #     virtualiser.connect((virtualiser_ip, virtualiser_port))
    # except :
    #     print colored("request_handlers> ", "red", attrs=['bold']) + 'Unable to connect to the virtualiser'
    #     sys.exit()        

    # print colored("request_handlers> ", "blue", attrs=['bold']) + 'Connected to the virtualiser. Sending NewRemoteSib request!'

    # # build request message 
    # request_msg = {"command":"NewRemoteSIB", "owner":owner}
    # request = json.dumps(request_msg)
    # virtualiser.send(request)
        
    # while 1:
    #     confirm_msg = virtualiser.recv(4096)
    #     if confirm_msg:
    #         print colored("request_handlers> ", "red", attrs=["bold"]) + 'Received the following message:'
    #         print confirm_msg
    #         break

    # confirm = json.loads(confirm_msg)
    # if confirm["return"] == "fail":
    #     print colored("request_handlers> ", "red", attrs=["bold"]) + 'Creation failed!' + confirm["cause"]
        
    #     return None
        
    # elif confirm["return"] == "ok":
    #     virtual_sib_id = confirm["virtual_sib_id"]
    #     virtual_sib_ip = confirm["virtual_sib_ip"]
    #     virtual_sib_port = confirm["virtual_sib_port"]
        
    #     print colored("request_handlers> ", "red", attrs=["bold"]) + 'Virtual Sib ' + virtual_sib_id + 'starded!' 
    #     print "IP: " + virtual_sib_ip
    #     print "PORT: " + virtual_sib_port        
        
    #     virtual_sib = virtual_sib_id + '#' + virtual_sib_ip + '#' + virtual_sib_port
        
#        return virtual_sib
        return 0       

    # Il server scelto oltre a far partire il thread (cioe' una
    # virtual sib) inserira' anche le informazioni nell'ancillary sib
    # start a virtual sib
    # thread.start_new_thread(virtualiser, (10010, 10011))
    # # virtual sib id
    # virtual_sib_id = str(uuid.uuid4())
    # # insert information in the ancillary SIB
    # a = SibLib("127.0.0.1", 10088)
    # t = [Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasPubIpPort"), URI(ns + "127.0.0.1-10011"))]
    # t.append(Triple(URI(ns + str(virtual_sib_id)), URI(ns + "hasKpIpPort"), URI(ns + "127.0.0.1-10010")))
    # a.insert(t)
    

    # return virtual sib id
#    return virtual_sib_id

def NewVirtualMultiSIB(sib_list):
    print colored("request_handlers> ", "blue", attrs=["bold"]) + str(sib_list)
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])
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
