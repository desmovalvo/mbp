# requirements
from lib.requests_handler import *
from collections import Counter
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
    "Discovery" : []
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
            
            # Decode the request
            if data.has_key("command"):

                if data["command"] in COMMANDS.keys():

                    # debug print
                    print colored("SIBmanager> ", "blue", attrs=["bold"]) + "received the command " + colored(data["command"], "cyan", attrs=['bold'])
                    self.server.logger.info(" Received the command " + str(data))

                    # check the number of arguments
                    if len(data.keys())-1 == len(COMMANDS[data["command"]]):

                        # check the arguments
                        cd = data.keys()
                        cd.remove("command")

                        # if we received a command with the right arguments...
                        if Counter(cd) == Counter(COMMANDS[data["command"]]):

                            print colored("SIBmanager> ", "blue", attrs=["bold"]) + "calling the proper method"

                            # NewRemoteSIB request
                            if data["command"] == "NewRemoteSIB":
                                confirm = globals()[data["command"]](data["owner"])
                                
                                # send a reply
                                self.request.sendall(json.dumps(confirm))

                            # Discovery request
                            elif data["command"] == "Discovery":
                                virtual_sib_list = globals()[data["command"]]()

                                # send a reply
                                self.request.sendall(json.dumps({'return':'ok', 'virtual_sib_list':virtual_sib_list}))
                                
                            # NewVirtualMultiSIB request
                            elif data["command"] == "NewVirtualMultiSIB":
                                sib_list = data['sib_list']
                                virtual_multi_sib_id = globals()[data["command"]](sib_list)

                                # send a reply
                                print "ritornato dalla funzione"
                                self.request.sendall(json.dumps({'return':'ok', 'virtual_multi_sib_id':virtual_multi_sib_id}))
                            
                        # if the received command has wrong arguments...
                        else:

                            # debug print
                            print colored("SIBmanager> ", "red", attrs=["bold"]) + "wrong arguments"
                            self.server.logger.info(" Wrong arguments, skipping message...")

                            # send a reply
                            self.request.sendall(json.dumps({'return':'fail', 'cause':'wrong arguments'}))                                                

                    # if the received command has a wrong number of arguments...
                    else:
                        # debug print
                        print colored("SIBmanager> ", "red", attrs=["bold"]) + "wrong number of arguments"
                        self.server.logger.info(" Wrong number of arguments, skipping message...")

                        # send a reply
                        self.request.sendall(json.dumps({'return':'fail', 'cause':'wrong number of arguments'}))                    

                # if we received an invalid command
                else:
                    # debug print
                    print colored("SIBmanager> ", "red", attrs=["bold"]) + "invalid command! Skipping message..."
                    self.server.logger.info(" Invalid command, skipping message...")

                    # send a reply
                    self.request.sendall(json.dumps({'return':'fail', 'cause':'invalid command'}))

            # if the received message does not contain a command
            else:
                # debug print
                print colored("SIBmanager> ", "red", attrs=["bold"]) + "no command supplied, skipping message"
                self.server.logger.info(" No command supplied, skipping message")

                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':'no command supplied'}))

        except Exception, e:
            print colored("SIBmanager> ", "red", attrs=["bold"]) + "Exception while receiving message: " + str(e)
            self.server.logger.info(" Exception while receiving message: " + str(e))
            self.request.sendall(json.dumps({'return':'fail', 'cause':str(e)}))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

    if len(sys.argv) == 3:
        sib_manager_ip = sys.argv[1]
        sib_manager_port = int(sys.argv[2])
    else:
        sib_manager_port = 17714
        sib_manager_ip = "0.0.0.0"

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
