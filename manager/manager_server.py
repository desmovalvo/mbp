# requirements
from lib.requests_handler import *
from collections import Counter
from lib.command import *
from termcolor import *
import SocketServer
import logging
import json
import time
import sys

# logging configuration
LOG_DIRECTORY = "log/"
LOG_FILE = LOG_DIRECTORY + str(time.strftime("%Y%m%d-%H%M-")) + "manager_server.log"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)

# available commands
# this is a dictionary in which the keys are the available commands,
# while the values are lists of available parameters for that command
COMMANDS = {
    "RegisterPublicSIB" : ["owner", "ip", "port"],
    "NewRemoteSIB" : ["owner", "sib_id"],
    "NewVirtualMultiSIB": ["sib_list"],
    "DiscoveryAll" : [],
    "DiscoveryWhere" : ["sib_profile"],
    "DeleteRemoteSIB" : ["virtual_sib_id"],
    "DeleteSIB": ["sib_id"],
    "SetSIBStatus": ["sib_id", "status"],
    "AddSIBtoVMSIB": ["vmsib_id", "sib_list"],
    "RemoveSIBfromVMSIB": ["vmsib_id", "sib_list"]
    }

# classes
class ManagerServer(SocketServer.ThreadingTCPServer):
    print colored("SIBmanager> ", "blue", attrs=["bold"]) + "sib manager started!"
    allow_reuse_address = True

class ManagerServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            # Output the received message
            print colored("SIBmanager> ", "blue", attrs=["bold"]) + "incoming connection"
            self.server.logger.info(" Incoming connection")
            data = json.loads(self.request.recv(1024).strip())            
            print colored("SIBmanager> ", "blue", attrs=["bold"]) + "received the following message:"
            print data

            # Check if the command is valid
            cmd = Command(data)
            if cmd.valid:
                print colored("SIBmanager> ", "blue", attrs=["bold"]) + "calling the proper method"

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
                if cmd.command == "NewRemoteSIB":
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

                # DeleteRemoteSIB request
                elif data["command"] == "DeleteRemoteSIB":
                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.virtual_sib_id)
                                
                    # send a reply
                    self.request.sendall(json.dumps(confirm))

                # DeleteSIB request
                elif data["command"] == "DeleteSIB":
                    confirm = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.sib_id)
                                
                    # send a reply
                    self.request.sendall(json.dumps(confirm))      

                # DiscoveryAll request
                elif data["command"] == "DiscoveryAll":
                    virtual_sib_list = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port)
                    
                    # send a reply
                    self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))

                # DiscoveryWhere request
                elif data["command"] == "DiscoveryWhere":
                    virtual_sib_list = globals()[cmd.command](self.server.ancillary_ip, self.server.ancillary_port, cmd.sib_profile)
                    
                    # send a reply
                    self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))
                                
                # NewVirtualMultiSIB request
                elif data["command"] == "NewVirtualMultiSIB":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_list)
                
                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))

                # SetSIBStatus
                elif data["command"] == "SetSIBStatus":
                    print "manager server: ricevuta set status"
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_id, cmd.status)
                    print confirm
                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))                    

                # AddSIBtoVMSIB
                elif data["command"] == "AddSIBtoVMSIB":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.vmsib_id, cmd.sib_list)
                
                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))                    


                # RemoveSIBfromVMSIB
                elif data["command"] == "RemoveSIBfromVMSIB":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.vmsib_id, cmd.sib_list)

                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))                    

                # SetSIBStatus
                elif data["command"] == "SetSIBStatus":
                    print "manager: set sib status request"
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_id, cmd.status)

                    # send a reply                    
                    print "manager server -------- " + str(confirm)
                    self.request.sendall(json.dumps(confirm))                    



            else:
                # debug print
                print colored("SIBmanager> ", "red", attrs=["bold"]) + cmd.invalid_cause
                self.server.logger.info(" " + cmd.invalid_cause)
                
                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':cmd.invalid_cause}))                                                
                
        except ZeroDivisionError: #Exception, e:
            print colored("SIBmanager> ", "red", attrs=["bold"]) + "Exception while receiving message: " + str(e)
            self.server.logger.info(" Exception while receiving message: " + str(e))
            self.request.sendall(json.dumps({'return':'fail', 'cause':str(e)}))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

    # Parameters needed to connect to manager and ancillary sib
    if len(sys.argv) == 5:
        manager_ip = sys.argv[1]
        manager_port = int(sys.argv[2])
        ancillary_ip = sys.argv[3]
        ancillary_port = int(sys.argv[4])
    else:
        manager_port = 17714
        manager_ip = "0.0.0.0"
        ancillary_ip = "localhost"
        ancillary_port = 10088
        
    try:
        # Create a logger object
        logger = logging.getLogger("manager_server")
       
        # Start the manager server
        server = ManagerServer((manager_ip, manager_port), ManagerServerHandler)
        server.logger = logger
        server.logger.info(" Starting server on " + manager_ip + ", Port " + str(manager_port))
        
        # Parameters needed to connect to manager and ancillary sib
        server.manager_ip = manager_ip
        server.manager_port = manager_port
        server.ancillary_ip = ancillary_ip
        server.ancillary_port = ancillary_port
       
        # Serve!
        server.serve_forever()
        
    except KeyboardInterrupt:
        print colored("SIBmanager> ", "blue", attrs=["bold"]) + "Goodbye!"
