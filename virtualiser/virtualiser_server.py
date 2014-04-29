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
# TODO - define a proper namespace
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
owl = "http://www.w3.org/2002/07/owl#"
xsd = "http://www.w3.org/2001/XMLSchema#"
rdfs = "http://www.w3.org/2000/01/rdf-schema#"
ns = "http://smartM3Lab/Ontology.owl#"

threads = {}

# logging configuration
LOG_DIRECTORY = "log/"
LOG_FILE = LOG_DIRECTORY + str(time.strftime("%Y%m%d-%H%M-")) + "virtualiser_server.log"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)

# available commands
# this is a dictionary in which the keys are the available commands,
# while the values are lists of available parameters for that command
COMMANDS = {
    "NewRemoteSIB" : ["owner"],
    "NewVirtualMultiSIB": ["sib_list"],
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
                            if data["command"] == "NewRemoteSIB":
                                #Passiamo al metodo NewRemoteSIB
                                #l'owner della sib in modo che
                                #inserisca nell'ancillary sib anche
                                #questo dato
                                thread_id = str(uuid.uuid4())
                                virtual_sib_info = globals()[data["command"]](data["owner"], virtualiser_ip, threads, thread_id)
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

                                
                                else:
                                    # send a reply
                                    try:
                                        self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_info':virtual_sib_info}))
                                    except socket.error:
                                        # remove virtual sib info from the ancillary sib
                                        a = SibLib("127.0.0.1", 10088)
                                        t = [Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasPubIpPort"), URI(ns + virtual_sib_info["virtual_sib_ip"] + "-" + str(virtual_sib_info["virtual_sib_pub_port"]) ))]
                                        t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasKpIpPort"), URI(ns + virtual_sib_info["virtual_sib_ip"] + "-" + str(virtual_sib_info["virtual_sib_kp_port"]))))
                                        t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasOwner"), URI(ns + virtual_sib_info["owner"])))
                                        t.append(Triple(URI(ns + virtual_sib_info["virtual_sib_id"]), URI(ns + "hasStatus"), URI(ns + "online")))
                                        a.remove(t)
                                        
                                        #killare il thread virtualiser lanciato all'interno del metodo NewRemoteSib
                                        threads[thread_id] = False
                                        
                                        print colored("Virtualiser> ", "red", attrs=["bold"]) + "Confirm message forwarding failed!"
                                                                                
                                        
                            elif data["command"] == "Discovery":
                                virtual_sib_list = globals()[data["command"]]()
                                # send a reply
                                self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))
                                
                            elif data["command"] == "NewVirtualMultiSIB":
                                sib_list = data['sib_list']
                                virtual_multi_sib_id = globals()[data["command"]](sib_list)
                                # send a reply
                                print "ritornato dalla funzione"
                                self.request.sendall(json.dumps({'return':'ok', 'virtual_multi_sib_id':virtual_multi_sib_id}))
                            
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

    try:
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
            triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "load"), Literal(str(random.randint(0, 100)))))
            triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasIP"), URI(ns + virtualiser_ip)))
            triples.append(Triple(URI(ns + virtualiser_id), URI(ns + "hasPort"), URI(ns + str(virtualiser_port))))
            ancillary_sib.insert(triples)
            
        except socket.error:
            print colored("Virtualiser> ", "red", attrs=["bold"]) + "Unable to connect to the ancillary SIB!"
            sys.exit()

        try:
            # Start the manager server
            server = VirtualiserServer(('127.0.0.1', int(virtualiser_port)), VirtualiserServerHandler)
            server.logger = logger
            server.logger.info(" Starting server on IP 127.0.0.1, Port " + str(virtualiser_port))
            print colored("Virtualiser> ", "blue", attrs=["bold"]) + "sib virtualiser started on port " + str(virtualiser_port) + " with ID " + virtualiser_id
            server.serve_forever()
        
        except KeyboardInterrupt:
            # cleaning the ancillary SIB
            ancillary_sib.remove(triples)

            print colored("Virtualiser> ", "blue", attrs=["bold"]) + "Goodbye!"

    except IndexError:
        print colored("Virtualiser> ", "red", attrs=["bold"]) + "You must specify ip and port number for the virtualiser, and ip and port for the ancillary SIB"
        sys.exit()
