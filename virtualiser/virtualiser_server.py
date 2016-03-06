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
import getopt
import random
import json
import time
import uuid
import sys
import os

# namespaces and prefixes
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
ns = "http://smartM3Lab/Ontology.owl#"
PREFIXES = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <""" + ns + ">"

threads = {}
t_id = {}

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
            if self.server.debug_enabled:
                self.server.logger.info(" Incoming connection, received the following message:")

            msg = self.request.recv(1024).strip()
            if len(msg) == 0:
                return

            data = json.loads(msg)
            print data

            if self.server.debug_enabled:
                self.server.logger.info(" " + str(data))

            # Check if the command is valid
            cmd = Command(data)
            if cmd.valid:
            
                print "comando valido"

                # decode 
                
                if data["command"] == "DeleteRemoteSIB":
                    print "deleteremotesib"
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

                   
                ##########################################################################
                #
                # NewRemoteSIB
                #
                ##########################################################################

                elif data["command"] == "NewRemoteSIB":

                    thread_id = str(uuid.uuid4())

                    # calling NewRemoteSIB function
                    virtual_sib_info = globals()[cmd.command](cmd.owner, 
                                                              cmd.sib_id, 
                                                              self.server.virtualiser_ip, 
                                                              threads, 
                                                              thread_id, 
                                                              virtualiser_id, 
                                                              self.server.manager_ip, 
                                                              self.server.manager_port,
                                                              self.server.config_file)

                    if virtual_sib_info["return"] == "fail":
                        # send a reply
                        try:
                            self.request.sendall(json.dumps({'return':'fail', 'cause':virtual_sib_info["cause"]}))
                        except socket.error:
                            print virtserver_print(False) + "Error message forwarding failed!"
                            pass
                
                    else: #virtual_sib_info["return"] = "ok"
                        
                        t_id[virtual_sib_info["virtual_sib_id"]] = thread_id
                        
                        # send a reply
                        try:
                            self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_info':virtual_sib_info}))

                        except socket.error:                            

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
                                                                thread_id,
                                                                self.server.config_file)
                    print virtual_multi_sib_info
                    t_id[virtual_multi_sib_info["virtual_multi_sib_id"]] = thread_id
                    # send a reply
                    self.request.sendall(json.dumps({'return':'ok', 'virtual_multi_sib_info':virtual_multi_sib_info}))

                                
            else:
                # debug print
                print virtserver_print(False) + cmd.invalid_cause
                if self.server.debug_enabled:
                    self.server.logger.info(" Error while parsing the json message, " + cmd.invalid_cause)

                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':cmd.invalid_cause}))

        except Exception, e:
            print virtserver_print(False) + "Exception while receiving message: " + str(e)
            if self.server.debug_enabled:
                self.server.logger.info(" Exception while receiving message: " + str(e))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

    try:
        # initial setup
        virtualiser_ip = None
        virtualiser_port = None
        manager_ip = None
        manager_port = None
        config_file = "virtualiser.conf"
        
        # read command line arguments
        opts, args = getopt.getopt(sys.argv[1:], "v:m:c:", ["manager=", "virtualiser=", "config="])
        for opt, arg in opts:

            if opt in ("-v", "--virtualiser"):
                virtualiser_ip = arg.split(":")[0]
                try:
                    virtualiser_port = int(arg.split(":")[1])
                except:
                    print virtserver_print(False) + "Usage: python virtualiser_server.py -m manager_ip:port -v virtualiser_ip:port"
                    sys.exit()            

            elif opt in ("-m", "--manager"):
                manager_ip = arg.split(":")[0]
                try:
                    manager_port = int(arg.split(":")[1])
                except IndexError:
                    print virtserver_print(False) + "Usage: python virtualiser_server.py -m manager_ip:port -v virtualiser_ip:port"
                    sys.exit()            
                    
            elif opt in ("-c", "--config"):
                config_file = arg

            else:
                print virtualiser_print(False) + "unrecognized option " + str(opt)

        if not(manager_ip and manager_port and virtualiser_ip and virtualiser_port):
            print virtserver_print(False) + "Usage: python virtualiser_server.py -m manager_ip:port -v virtualiser_ip:port"
            sys.exit()            

    except getopt.GetoptError:
        print virtserver_print(False) + "Usage: python virtualiser_server.py -m manager_ip:port -v virtualiser_ip:port"
        sys.exit()

    # read the configuration file 
    config_file_stream = open(config_file, "r")
    conf = json.load(config_file_stream)
    config_file_stream.close()

    # configure the logger
    debug_enabled = conf["debug"]    
    debug_level = conf["debug_level"]
    log_directory = conf["directory"]
    log_file = log_directory + str(time.strftime("%Y%m%d-%H%M-")) + "virtualiser_server.log"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    logging.basicConfig(filename=log_file, level=debug_level)
    logger = logging.getLogger("virtualiser_server")

    # now we can begin
    virtualiser_id = str(uuid.uuid4())
    
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
            server.config_file = config_file
            server.debug_enabled = debug_enabled
            server.debug_level = debug_level
            server.manager_ip = manager_ip
            server.manager_port = manager_port
            server.logger = logger
            
            # debug
            if server.debug_enabled:
                server.logger.info(" Starting server on IP " + virtualiser_ip + " Port " + str(virtualiser_port))
            print virtserver_print(True) + "sib virtualiser started on " + virtualiser_ip + ":" + str(virtualiser_port) + " with ID " + virtualiser_id

            # start serving
            server.serve_forever()
        
        # Wrong virtualiser server address specified
        except socket.gaierror:

            # debug print
            print virtserver_print(False) + "wrong address for virtualiser server"
            if server.debug_enabled:
                server.logger.info(" wrong address for virtualiser server")

            # build the NewVirtualiser request
            msg = {"command":"DeleteVirtualiser", "id":virtualiser_id}
    
            # send the request to the manager
            confirm = manager_request(manager_ip, manager_port, msg)            

        # CTRL-C pressed
        except KeyboardInterrupt:
            
            # debug print
            print virtserver_print(True) + "sending " + colored("DeleteVirtualiser", "cyan", attrs=["bold"]) + " request"
            if server.debug_enabled:
                server.logger.info(" CTRL-C pressed. Sending DeleteVirtualiser request")

            # build the NewVirtualiser request
            msg = {"command":"DeleteVirtualiser", "id":virtualiser_id}
    
            # send the request to the manager
            confirm = manager_request(manager_ip, manager_port, msg)

            # debug print
            print virtserver_print(True) + "Goodbye!"
    
    else:
        if debug_enabled:
            logger.info(" Unable to start the virtualiser. Cause: " + str(confirm["cause"]))
        print virtserver_print(False) + "unable to start the virtualiser. Cause: " + str(confirm["cause"])
        sys.exit(0)
