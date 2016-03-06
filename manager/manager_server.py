# requirements
from lib.requests_handler import *
from lib.ancillary_lib import *
from collections import Counter
from lib.command import *
from threading import *
from termcolor import *
import SocketServer
import logging
import getopt
import json
import time
import sys
import os

# available commands
# this is a dictionary in which the keys are the available commands,
# while the values are lists of available parameters for that command
COMMANDS = {
    "RegisterPublicSIB" : ["owner", "ip", "port"],
    "NewRemoteSIB" : ["owner", "sib_id"],
    "NewVirtualMultiSIB": ["sib_list"],
    "NewVirtualiser": ["id", "ip", "port"],
    "DiscoveryAll" : [],
    "DiscoveryWhere" : ["sib_profile"],
    "DeleteRemoteSIB" : ["virtual_sib_id"],
    "DeleteSIB": ["sib_id"],
    "DeleteVirtualiser" : ["id"],
    "SetSIBStatus": ["sib_id", "status"],
    "GetSIBStatus": ["sib_id"],
    "AddSIBtoVMSIB": ["vmsib_id", "sib_list"],
    "RemoveSIBfromVMSIB": ["vmsib_id", "sib_list"],
    "MultiSIBInfo": ["multi_sib_id"],
    "GetVirtualPublicSIBs": [],
    "GetVirtualMultiSIBs": [],
    "GetVirtualisers": []
    }

# classes
class ManagerServer(SocketServer.ThreadingTCPServer):
    print manager_server_print(True) + "sib manager started!"
    allow_reuse_address = True

class ManagerServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            # Output the received message
            if self.server.debug_enabled:
                self.server.logger.info(" Incoming connection")
            data = json.loads(self.request.recv(1024).strip())            
            print manager_server_print(True) + "received the following message:"
            print data

            # Check if the command is valid
            cmd = Command(data)
            if cmd.valid:

                # RegisterPublicSIB request
                if cmd.command == "RegisterPublicSIB":
                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.owner, cmd.ip, cmd.port)

                    # send a reply
                    try:
                        self.request.sendall(json.dumps(confirm))
                    except:
                        if confirm["return"] == "fail":
                            # nothing to do
                            print "Send Failed"
                        else:
                            # Send DeleteRemoteSIB request to the virtualiser to remove the virtual sib just created
                            confirm = globals()["DeleteSIB"](confirm["sib_info"]["sib_id"])


                # NewRemoteSIB request
                elif cmd.command == "NewRemoteSIB":

                    # Acquire the lock
                    self.server.lock.acquire()

                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.owner, cmd.sib_id)
                    
                    # send a reply
                    try:
                        self.request.sendall(json.dumps(confirm))
                    except:
                        if confirm["return"] == "fail":
                            # nothing to do
                            print "Send Failed"
                        else:
                            # Send DeleteRemoteSIB request to the virtualiser to remove the virtual sib just created
                            confirm = globals()["DeleteRemoteSIB"](confirm["virtual_sib_info"]["virtual_sib_id"])

                    # release the lock
                    self.server.lock.release()

                # NewVirtualiser request
                elif cmd.command == "NewVirtualiser":
                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.id, cmd.ip, cmd.port)
                    
                    # send a reply
                    self.request.sendall(json.dumps(confirm))

                # DeleteRemoteSIB request
                elif cmd.command == "DeleteRemoteSIB":
                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.virtual_sib_id)
                    print "CONFIRM " + str(confirm)
                    # send a reply
                    self.request.sendall(json.dumps(confirm))

                # DeleteSIB request
                elif cmd.command == "DeleteSIB":
                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.sib_id)
                                
                    # send a reply
                    self.request.sendall(json.dumps(confirm))      

                # DeleteVirtualiser request
                elif cmd.command == "DeleteVirtualiser":

                    # acquire the lock
                    self.server.lock.acquire()
                    
                    # calling DeleteVirtualiser
                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.id)
                                
                    # send a reply
                    self.request.sendall(json.dumps(confirm))     
                    
                    # release the lock
                    self.server.lock.release()

                # DiscoveryAll request
                elif cmd.command == "DiscoveryAll":
                    virtual_sib_list = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port)
                    
                    # send a reply
                    self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))

                # DiscoveryWhere request
                elif cmd.command == "DiscoveryWhere":
                    virtual_sib_list = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.sib_profile)
                    
                    # send a reply
                    self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))
                                
                # NewVirtualMultiSIB request
                elif cmd.command == "NewVirtualMultiSIB":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_list)
                
                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))

                # SetSIBStatus
                elif cmd.command == "SetSIBStatus":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_id, cmd.status)

                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))                    

                # GetSIBStatus
                elif cmd.command == "GetSIBStatus":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_id)

                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))                    


                # AddSIBtoVMSIB
                elif cmd.command == "AddSIBtoVMSIB":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.vmsib_id, cmd.sib_list)
                
                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))                    


                # RemoveSIBfromVMSIB
                elif cmd.command == "RemoveSIBfromVMSIB":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.vmsib_id, cmd.sib_list)

                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))                    

                # SetSIBStatus
                elif cmd.command == "SetSIBStatus":
                    print "manager: set sib status request"
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_id, cmd.status)

                    # send a reply                    
                    print "manager server -------- " + str(confirm)
                    self.request.sendall(json.dumps(confirm))                    

                # MultiSIBInfo
                elif cmd.command == "MultiSIBInfo":
                    print "manager: multi sib info request"
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.multi_sib_id)

                    # send a reply                    
                    print "manager server -------- " + str(confirm)
                    self.request.sendall(json.dumps(confirm))                    


                # GetVirtualPublicSIBs
                elif data["command"] == "GetVirtualPublicSIBs":
                    print "manager: get virtual public sib request"
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port)

                    # send a reply                    
                    print "manager server -------- " + str(confirm)
                    self.request.sendall(json.dumps(confirm))                    

                # GetVirtualMultiSIBs
                elif data["command"] == "GetVirtualMultiSIBs":
                    print "manager: get virtual multi sib request"
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port)

                    # send a reply                    
                    print "manager server -------- " + str(confirm)
                    self.request.sendall(json.dumps(confirm))                    


                # GetVirtualisers
                elif data["command"] == "GetVirtualisers":
                    print "manager: get virtualiser request"
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port)

                    # send a reply                    
                    print "manager server -------- " + str(confirm)
                    self.request.sendall(json.dumps(confirm))                    


            else:
                # debug print
                print manager_server_print(False) + cmd.invalid_cause
                if self.server.debug_enabled:
                    self.server.logger.info(" " + cmd.invalid_cause)
                
                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':cmd.invalid_cause}))                                                
                
        except ZeroDivisionError: #Exception, e:
            print manager_server_print(False) + "Exception while receiving message: " + str(e)
            if self.server.debug_enabled:
                self.server.logger.info(" Exception while receiving message: " + str(e))
            self.request.sendall(json.dumps({'return':'fail', 'cause':str(e)}))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

    # initial setup
    config_file = "manager.conf"
    
    # read command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:", ["config="])
        for opt, arg in opts:
    
            if opt in ("-c", "--config"):
                config_file = arg
                
            else:
                print manager_server_print(False) + "unrecognized option " + str(opt)
    
    except getopt.GetoptError:
        print manager_server_print(False) + "Usage: python manager_server.py [-c config_file]"
        sys.exit()

    # read the configuration file
    config_file_stream = open(config_file, "r")
    conf = json.load(config_file_stream)
    config_file_stream.close()

    # ancillary_sib configuration
    ancillary_ip = conf["ancillary_ip"]
    ancillary_port = int(conf["ancillary_port"])

    # manager configuration
    manager_ip = "0.0.0.0"
    manager_port = int(conf["manager_port"])

    # configure the logger
    debug_enabled = conf["debug"]    
    debug_level = conf["debug_level"]
    log_directory = conf["directory"]
    log_file = log_directory + str(time.strftime("%Y%m%d-%H%M-")) + "manager_server.log"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    logging.basicConfig(filename=log_file, level=debug_level)
    logger = logging.getLogger("manager_server")

    # main
    try:
        # Create a logger object
        logger = logging.getLogger("manager_server")

        # Preliminary check
        print manager_server_print(True) + "cleaning ancillary SIB from old triples"
        ancillary_check(ancillary_ip, ancillary_port)
       
        # Start the manager server
        server = ManagerServer((manager_ip, manager_port), ManagerServerHandler)
        server.debug_enabled = debug_enabled
        server.debug_level = debug_level
        server.logger = logger
        server.lock = Lock()
        
        # debug print
        if server.debug_enabled:
            server.logger.info(" Starting server on " + manager_ip + ", Port " + str(manager_port))
        
        # Parameters needed to connect to manager and ancillary sib
        server.manager_ip = manager_ip
        server.manager_port = manager_port
        server.ancillary_ip = ancillary_ip
        server.ancillary_port = ancillary_port
       
        # Serve!
        server.serve_forever()
        
    except KeyboardInterrupt:
        print colored("manager_server> ", "blue", attrs=["bold"]) + "Goodbye!"
        if server.debug_enabled:
            server.logger.info(" CTRL-C pressed, closing manager server.")
