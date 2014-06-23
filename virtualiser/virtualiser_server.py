#!/usr/bin/python

# requirements
from lib.connection_helpers import *
from lib.request_handlers import *
from lib.output_helpers import *
from collections import Counter
from lib.SIBLib import SibLib
from lib.command import *
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

PREFIXES = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <""" + ns + ">"

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
    "NewRemoteSIB" : ["owner", "sib_id"],
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
            print virtserver_print(True) + "incoming connection, received the following message:",
            self.server.logger.info(" Incoming connection, received the following message:")
            data = json.loads(self.request.recv(1024).strip())
            print data
            self.server.logger.info(" " + str(data))

            # Check if the command is valid
            cmd = Command(data)
            if cmd.valid:
            
                # decode 
                
                if data["command"] == "DeleteRemoteSIB":
                    confirm = globals()[cmd.command](cmd.virtual_sib_id, threads, t_id, virtualiser_id)

                    if confirm["return"] == "fail":
                        print virtserver_print(False) + 'Deletion failed!' + confirm["cause"]

                        try:
                            self.request.sendall(json.dumps({'return':'fail', 'cause':confirm["cause"]}))
                        except socket.error:
                            print virtserver_print(False) + "Error message forwarding failed!"
                            pass

                    elif confirm["return"] == "ok":
                            
                        # send a reply
                        try:
                            self.request.sendall(json.dumps({'return':'ok'}))
                        except socket.error:
                            print virtserver_print(False) + "Confirm message forwarding failed!"                          

                elif data["command"] == "NewRemoteSIB":

                    thread_id = str(uuid.uuid4())

                    virtual_sib_info = globals()[cmd.command](cmd.owner, cmd.sib_id, self.server.virtualiser_ip, threads, thread_id, virtualiser_id, self.server.manager_ip, self.server.manager_port)

                    
                    if virtual_sib_info["return"] == "fail":
                        # send a reply
                        try:
                            self.request.sendall(json.dumps({'return':'fail', 'cause':virtual_sib_info["cause"]}))
                            # Il thread virtualiser in
                            # questo caso non e' stato
                            # neppure creato, quindi non
                            # va killato
                        except socket.error:
                            print virtserver_print(False) + "Error message forwarding failed!"
                            pass

                    
                    else: #virtual_sib_info["return"] = "ok"
                        
                        t_id[virtual_sib_info["virtual_sib_id"]] = thread_id
                        
                        # send a reply
                        try:
                            self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_info':virtual_sib_info}))
                        except socket.error:
                            # remove virtual sib info from the ancillary sib
                            # a = SibLib(self.server.ancillary_ip, self.server.ancillary_port)
                            # t = [Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasPubIpPort"), URI(ns + virtual_sib_info["virtual_sib_ip"] + "-" + str(virtual_sib_info["virtual_sib_pub_port"]) ))]
                            # t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasKpIpPort"), URI(ns + virtual_sib_info["virtual_sib_ip"] + "-" + str(virtual_sib_info["virtual_sib_kp_port"]))))
                            # t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasOwner"), URI(ns + virtual_sib_info["owner"])))
                            # t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasStatus"), URI(ns + "online")))
                            # a.remove(t)
                            
                            #killare il thread virtualiser lanciato all'interno del metodo NewRemoteSib
                            threads[thread_id] = False
                            del t_id[virtual_sib_info["virtual_sib_id"]]
                            
                            print virtserver_print(False) + "Confirm message forwarding failed!"

                   
                ##########################################################################
                #
                # NewVirtualMultiSIB
                #
                ##########################################################################
                    
                elif cmd.command == "NewVirtualMultiSIB":

                    # calling the proper method as a thread
                    thread_id = str(uuid.uuid4())
                    virtual_multi_sib_info = NewVirtualMultiSIB(cmd.sib_list, 
                                                                self.server.virtualiser_ip, 
                                                                self.server.virtualiser_id, 
                                                                threads, 
                                                                thread_id)
                    # send a reply
                    self.request.sendall(json.dumps({'return':'ok', 'virtual_multi_sib_info':virtual_multi_sib_info}))

                                
            else:
                # debug print
                print virtserver_print(False) + cmd.invalid_cause
                self.server.logger.info(" Error while parsing the json message, " + cmd.invalid_cause)

                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':cmd.invalid_cause}))

        except ZeroDivisionError: #Exception, e:
            print virtserver_print(False) + "Exception while receiving message: " + str(e)
            self.server.logger.info(" Exception while receiving message: " + str(e))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

    if len(sys.argv) < 5:    
        print virtserver_print(False) + """You must specify:
* virtualiser ip
* virtualiser port
* manager ip
* manager port"""
        sys.exit()
    else:
        virtualiser_id = str(uuid.uuid4())
        virtualiser_ip = sys.argv[1]
        virtualiser_port = int(sys.argv[2])
        manager_ip = sys.argv[3]
        manager_port = int(sys.argv[4])

    # Create a logger object
    logger = logging.getLogger("virtualiser_server")

    # build the NewVirtualiser request
    msg = {"command":"NewVirtualiser", "ip":virtualiser_ip, "port":str(virtualiser_port), "id":virtualiser_id}
    
    # send the request to the manager
    confirm = manager_request(manager_ip, manager_port, msg)
    if confirm['return'] == "ok":
        
        try:
            # Start the virtualiser server
            server = VirtualiserServer((virtualiser_ip, int(virtualiser_port)), VirtualiserServerHandler)
            server.virtualiser_id = virtualiser_id
            server.virtualiser_ip =  virtualiser_ip
            server.virtualiser_port = virtualiser_port 
            server.manager_ip = manager_ip
            server.manager_port = manager_port
            server.logger = logger
            server.logger.info(" Starting server on IP " + virtualiser_ip + " Port " + str(virtualiser_port))
            print virtserver_print(True) + "sib virtualiser started on " + virtualiser_ip + ":" + str(virtualiser_port) + " with ID " + virtualiser_id
            server.serve_forever()
        
        # CTRL-C pressed
        except KeyboardInterrupt:
            
            # debug print
            print virtserver_print(True) + "sending " + colored("DeleteVirtualiser", "cyan", attrs=["bold"]) + " request"

            # build the NewVirtualiser request
            msg = {"command":"DeleteVirtualiser", "id":virtualiser_id}
    
            # send the request to the manager
            confirm = manager_request(manager_ip, manager_port, msg)

            # debug print
            print virtserver_print(True) + "Goodbye!"
    
    else:
        print virtserver_print(False) + "unable to start the virtualiser. Cause: " + str(confirm["cause"])
        sys.exit(0)
