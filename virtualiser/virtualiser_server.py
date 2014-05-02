#!/usr/bin/python

# requirements
from lib.request_handlers import *
from collections import Counter
from lib.SIBLib import SibLib
from termcolor import *
import SocketServer
import logging
import random
import json
import time
import uuid
import sys

# namespaces
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
ns = "http://smartM3Lab/Ontology.owl#"

threads = {}
t_id = {}

# logging configuration
LOG_DIRECTORY = "log/"
LOG_FILE = LOG_DIRECTORY + str(time.strftime("%Y%m%d-%H%M-")) + "virtualiser_server.log"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)

# available commands
# this is a dictionary in which the keys are the available commands,
# while the values are lists of available parameters for that command
COMMANDS = {
    "NewRemoteSIB" : ["owner"],
    "DeleteRemoteSIB" : ["virtual_sib_id"],
    "NewVirtualMultiSIB": ["sib_list"]
    }

# classes
class VirtualiserServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True

class VirtualiserServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            # Output the received message
            print colored("Virtualiser> ", "blue", attrs=["bold"]) + "incoming connection, received the following message:"
            self.server.logger.info(" Incoming connection, received the following message:")
            data = json.loads(self.request.recv(1024).strip())
            print data
            self.server.logger.info(" " + str(data))
            
            # Decode the request
            if data.has_key("command"):
                
                if data["command"] in COMMANDS.keys():
                    # debug print
                    print colored("Virtualiser> ", "blue", attrs=["bold"]) + "received the command " + colored(data["command"], "cyan", attrs=['bold'])
                    self.server.logger.info(" Received the command " + str(data))

                    # check the number of arguments
                    if len(data.keys())-1 == len(COMMANDS[data["command"]]):

                        # check the arguments
                        cd = data.keys()
                        cd.remove("command")
                        if Counter(cd) == Counter(COMMANDS[data["command"]]):

                            # decode 
                            print colored("Virtualiser> ", "blue", attrs=["bold"]) + "calling the proper method"
                            
                            if data["command"] == "DeleteRemoteSIB":
                                confirm = globals()[data["command"]](data["virtual_sib_id"], threads, t_id, virtualiser_id, self.server.ancillary_ip, self.server.ancillary_port)

                                if confirm["return"] == "fail":
                                    print colored("virtualiser_server> ", "red", attrs=["bold"]) + 'Deletion failed!' + confirm["cause"]

                                    try:
                                        self.request.sendall(json.dumps({'return':'fail', 'cause':confirm["cause"]}))
                                    except socket.error:
                                        print colored("Virtualiser> ", "red", attrs=["bold"]) + "Error message forwarding failed!"
                                        pass

                                elif confirm["return"] == "ok":
                                        
                                    # send a reply
                                    try:
                                        self.request.sendall(json.dumps({'return':'ok'}))
                                    except socket.error:
                                        print colored("Virtualiser> ", "red", attrs=["bold"]) + "Confirm message forwarding failed!"                          

                            elif data["command"] == "NewRemoteSIB":
                                #Passiamo al metodo NewRemoteSIB
                                #l'owner della sib in modo che
                                #inserisca nell'ancillary sib anche
                                #questo dato
                                thread_id = str(uuid.uuid4())
                                virtual_sib_info = globals()[data["command"]](data["owner"], self.server.virtualiser_ip, threads, thread_id, virtualiser_id, self.server.ancillary_ip, self.server.ancillary_port)
                                
                                if virtual_sib_info["return"] == "fail":
                                    # send a reply
                                    try:
                                        self.request.sendall(json.dumps({'return':'fail', 'cause':virtual_sib_info["cause"]}))
                                        # Il thread virtualiser in
                                        # questo caso non e' stato
                                        # neppure creato, quindi non
                                        # va killato
                                    except socket.error:
                                        print colored("Virtualiser> ", "red", attrs=["bold"]) + "Error message forwarding failed!"
                                        pass

                                
                                else: #virtual_sib_info["return"] = "ok"
                                    
                                    t_id[virtual_sib_info["virtual_sib_id"]] = thread_id
                                    
                                    print colored("Virtualiser_server> ", "blue", attrs=["bold"]) + "Updating the load of virtualiser  " + virtualiser_id
                                    
                                    #############################################
                                    ##                                         ##
                                    ## Update the load of selected virtualiser ##
                                    ##                                         ##
                                    #############################################
                                    # get old load
                                    try:
                                        a = SibLib(self.server.ancillary_ip, self.server.ancillary_port)
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
                                        #new_load = int(load) + 1
                                        load += 1
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


                                    # send a reply
                                    try:
                                        self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_info':virtual_sib_info}))
                                    except socket.error:
                                        # remove virtual sib info from the ancillary sib
                                        a = SibLib(self.server.ancillary_ip, self.server.ancillary_port)
                                        t = [Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasPubIpPort"), URI(ns + virtual_sib_info["virtual_sib_ip"] + "-" + str(virtual_sib_info["virtual_sib_pub_port"]) ))]
                                        t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasKpIpPort"), URI(ns + virtual_sib_info["virtual_sib_ip"] + "-" + str(virtual_sib_info["virtual_sib_kp_port"]))))
                                        t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasOwner"), URI(ns + virtual_sib_info["owner"])))
                                        t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasStatus"), URI(ns + "online")))
                                        a.remove(t)
                                        
                                        #killare il thread virtualiser lanciato all'interno del metodo NewRemoteSib
                                        threads[thread_id] = False
                                        del t_id[virtual_sib_info["virtual_sib_id"]]
                                        
                                        print colored("Virtualiser> ", "red", attrs=["bold"]) + "Confirm message forwarding failed!"
                                                                                
                                        
                            elif data["command"] == "Discovery":
                                virtual_sib_list = globals()[data["command"]]()
                                # send a reply
                                self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))
                                
                            elif data["command"] == "NewVirtualMultiSIB":
                                sib_list = data['sib_list']
                                thread_id = str(uuid.uuid4())
                                virtual_multi_sib_info = globals()[data["command"]](sib_list, self.server.virtualiser_ip, self.server.virtualiser_id, threads, thread_id, self.server.ancillary_ip, self.server.ancillary_port)
                                # send a reply
                                print "ritornato dalla funzione"
                                self.request.sendall(json.dumps({'return':'ok', 'virtual_multi_sib_info':virtual_multi_sib_info}))
                            
                        else:

                            # debug print
                            print colored("Virtualiser> ", "red", attrs=["bold"]) + "wrong arguments"
                            self.server.logger.info(" Wrong arguments, skipping message...")

                            # send a reply
                            self.request.sendall(json.dumps({'return':'fail', 'cause':'wrong arguments'}))                                                

                    else:
                        # debug print
                        print colored("Virtualiser> ", "red", attrs=["bold"]) + "wrong number of arguments"
                        self.server.logger.info(" Wrong number of arguments, skipping message...")

                        # send a reply
                        self.request.sendall(json.dumps({'return':'fail', 'cause':'wrong number of arguments'}))                    

                else:
                    # debug print
                    print colored("Virtualiser> ", "red", attrs=["bold"]) + "invalid command! Skipping message..."
                    self.server.logger.info(" Invalid command, skipping message...")

                    # send a reply
                    self.request.sendall(json.dumps({'return':'fail', 'cause':'invalid command'}))
                
            else:
                # debug print
                print colored("Virtualiser> ", "red", attrs=["bold"]) + "no command supplied, skipping message"
                self.server.logger.info(" No command supplied, skipping message")

                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':'no command supplied'}))

        except ZeroDivisionError:# Exception, e:
            print colored("Virtualiser> ", "red", attrs=["bold"]) + "Exception while receiving message: "# + str(e)
            self.server.logger.info(" Exception while receiving message: ")# + str(e))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

    if len(sys.argv) < 5:    
        print colored("Virtualiser> ", "red", attrs=["bold"]) + """You must specify:
* virtualiser ip
* virtualiser port
* ancillary ip
* ancillary port"""
        sys.exit()
    else:
        virtualiser_id = str(uuid.uuid4())
        virtualiser_ip = sys.argv[1]
        virtualiser_port = int(sys.argv[2])
        ancillary_ip = sys.argv[3]
        ancillary_port = int(sys.argv[4])

    try:
        # Create a logger object
        logger = logging.getLogger("virtualiser_server")
        
        # Insert the virtualiser informations into the Ancillary SIB
        ancillary_sib = SibLib(ancillary_ip, ancillary_port)
        ancillary_sib.join_sib()
        triples = []
        triples.append(Triple(URI(ns + virtualiser_id), URI(rdf + "type"), URI(ns + "virtualiser")))
        triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(0))))
        triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasIP"), URI(ns + virtualiser_ip)))
        triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasPort"), URI(ns + str(virtualiser_port))))
        ancillary_sib.insert(triples)
        
    except socket.error:
        print colored("Virtualiser> ", "red", attrs=["bold"]) + "Unable to connect to the ancillary SIB!"
        sys.exit()

    try:
        # Start the manager server
        server = VirtualiserServer((virtualiser_ip, int(virtualiser_port)), VirtualiserServerHandler)
        server.virtualiser_id = virtualiser_id
        server.virtualiser_ip =  virtualiser_ip
        server.virtualiser_port = virtualiser_port 
        server.ancillary_ip = ancillary_ip
        server.ancillary_port = ancillary_port
        server.logger = logger
        server.logger.info(" Starting server on IP " + virtualiser_ip + " Port " + str(virtualiser_port))
        print colored("Virtualiser> ", "blue", attrs=["bold"]) + "sib virtualiser started on " + virtualiser_ip + ":" + str(virtualiser_port) + " with ID " + virtualiser_id
        server.serve_forever()
    
    except KeyboardInterrupt:
        # cleaning the ancillary SIB
        ancillary_sib.remove(triples)
        print colored("Virtualiser> ", "blue", attrs=["bold"]) + "Goodbye!"

