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
    "NewRemoteSIB" : ["owner"],
    "NewVirtualMultiSIB": ["sib_list"],
    "Discovery" : [],
    "DeleteRemoteSIB" : ["virtual_sib_id"]
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

                # NewRemoteSIB request
                if cmd.command == "NewRemoteSIB":
                    confirm = globals()[cmd.command](cmd.owner)
                    
                    # send a reply
                    try:
                        self.request.sendall(json.dumps(confirm))
                    except:
                        if corfirm["return"] == "fail":
                            # nothing to do
                            print "Send Failed"
                        else:
                            # Send DeleteRemoteSIB request to the virtualiser to remove the virtual sib just created
                            confirm = globals()["DeleteRemoteSIB"](confirm["virtual_sib_info"]["virtual_sib_id"])

                # DeleteRemoteSIB request
                elif data["command"] == "DeleteRemoteSIB":
                    confirm = globals()[cmd.command](cmd.virtual_sib_id)
                                
                    # send a reply
                    self.request.sendall(json.dumps(confirm))

                # Discovery request
                elif data["command"] == "Discovery":
                    virtual_sib_list = globals()[cmd.command]()
                    
                    # send a reply
                    self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))
                                
                # NewVirtualMultiSIB request
                elif data["command"] == "NewVirtualMultiSIB":
                    confirm = globals()[cmd.command](ancillary_ip, ancillary_port, cmd.sib_list)
                
                    # send a reply                    
                    self.request.sendall(json.dumps(confirm))

            else:
                # debug print
                print colored("SIBmanager> ", "red", attrs=["bold"]) + cmd.invalid_cause
                self.server.logger.info(" " + cmd.invalid_cause)
                
                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':cmd.invalid_cause}))                                                
                

        except ZeroDivisionError:#Exception, e:
            print colored("SIBmanager> ", "red", attrs=["bold"]) + "Exception while receiving message: " + str(e)
            self.server.logger.info(" Exception while receiving message: " + str(e))
            self.request.sendall(json.dumps({'return':'fail', 'cause':str(e)}))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

   if len(sys.argv) == 5:
       sib_manager_ip = sys.argv[1]
       sib_manager_port = int(sys.argv[2])
       ancillary_ip = sys.argv[3]
       ancillary_port = int(sys.argv[4])
   else:
       sib_manager_port = 17714
       sib_manager_ip = "0.0.0.0"
       ancillary_ip = "localhost"
       ancillary_port = 10088

   try:
       # Create a logger object
       logger = logging.getLogger("manager_server")
       
       # Start the manager server
       server = ManagerServer((sib_manager_ip, sib_manager_port), ManagerServerHandler)
       server.logger = logger
       server.logger.info(" Starting server on " + sib_manager_ip + ", Port " + str(sib_manager_port))
       server.serve_forever()
        
   except KeyboardInterrupt:
       print colored("SIBmanager> ", "blue", attrs=["bold"]) + "Goodbye!"
