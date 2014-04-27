# requirements
from lib.request_handlers import *
from collections import Counter
from termcolor import *
import SocketServer
import logging
import json
import time

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
    print colored("Manager> ", "blue", attrs=["bold"]) + "sib manager started!"
    allow_reuse_address = True

class ManagerServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            # Output the received message
            print colored("Manager> ", "blue", attrs=["bold"]) + "incoming connection, received the following message:"
            self.server.logger.info(" Incoming connection, received the following message:")
            data = json.loads(self.request.recv(1024).strip())
            print data
            self.server.logger.info(" " + str(data))
            
            # Decode the request
            if data.has_key("command"):

                if data["command"] in COMMANDS.keys():
                    # debug print
                    print colored("Manager> ", "blue", attrs=["bold"]) + "received the command " + colored(data["command"], "cyan", attrs=['bold'])
                    self.server.logger.info(" Received the command " + str(data))

                    # check the number of arguments
                    if len(data.keys())-1 == len(COMMANDS[data["command"]]):

                        # check the arguments
                        cd = data.keys()
                        cd.remove("command")
                        if Counter(cd) == Counter(COMMANDS[data["command"]]):

                            # decode 
                            print colored("Manager> ", "blue", attrs=["bold"]) + "calling the proper method"
                            if data["command"] == "NewRemoteSIB":
                                #TODO: passare al metodo NewRemoteSIB
                                #l'owner della sib e fargli inserire
                                #nell'ancillary sib anche questo dato
                                virtual_sib = globals()[data["command"]]()
                                # send a reply
                                if virtual_sib != None:
                                    self.request.sendall(json.dumps({'return':'ok', 'virtual_sib':virtual_sib}))
                                else:
                                    self.request.sendall(json.dumps({'return':'fail', 'cause':'virtual sib not created!'}))

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
                            print colored("Manager> ", "red", attrs=["bold"]) + "wrong arguments"
                            self.server.logger.info(" Wrong arguments, skipping message...")

                            # send a reply
                            self.request.sendall(json.dumps({'return':'fail', 'cause':'wrong arguments'}))                                                

                    else:
                        # debug print
                        print colored("Manager> ", "red", attrs=["bold"]) + "wrong number of arguments"
                        self.server.logger.info(" Wrong number of arguments, skipping message...")

                        # send a reply
                        self.request.sendall(json.dumps({'return':'fail', 'cause':'wrong number of arguments'}))                    

                else:
                    # debug print
                    print colored("Manager> ", "red", attrs=["bold"]) + "invalid command! Skipping message..."
                    self.server.logger.info(" Invalid command, skipping message...")

                    # send a reply
                    self.request.sendall(json.dumps({'return':'fail', 'cause':'invalid command'}))

                # debug print
                print colored("Manager> ", "blue", attrs=["bold"]) + "received the command " + colored(data["command"], "cyan", attrs=['bold'])
                self.server.logger.info(" Received the command " + str(data))

                # send a reply
                self.request.sendall(json.dumps({'return':'ok'}))
                
            else:
                # debug print
                print colored("Manager> ", "red", attrs=["bold"]) + "no command supplied, skipping message"
                self.server.logger.info(" No command supplied, skipping message")

                # send a reply
                self.request.sendall(json.dumps({'return':'fail', 'cause':'no command supplied'}))

        except ZeroDivisionError:# Exception, e:
            print colored("Manager> ", "red", attrs=["bold"]) + "Exception while receiving message: "# + str(e)
            self.server.logger.info(" Exception while receiving message: ")# + str(e))
                self.request.sendall(json.dumps({'return':'fail'}))

        except Exception, e:
            print colored("Manager> ", "red", attrs=["bold"]) + "Exception while receiving message: " + str(e)
            self.server.logger.info(" Exception while receiving message: " + str(e))


##############################################################
#
# main program
#
##############################################################

if __name__=='__main__':

    try:
        # Create a logger object
        logger = logging.getLogger("manager_server")
        
        # Start the manager server
        server = ManagerServer(('127.0.0.1', 17714), ManagerServerHandler)
        server.logger = logger
        server.logger.info(" Starting server on IP 127.0.0.1, Port 17714")
        server.serve_forever()
        
    except KeyboardInterrupt:
        print colored("Manager> ", "blue", attrs=["bold"]) + "Goodbye!"
