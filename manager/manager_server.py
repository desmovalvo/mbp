# requirements
from termcolor import *
import SocketServer
import logging
import json
import time

# logging configuration
LOG_DIRECTORY = "log/"
LOG_FILE = LOG_DIRECTORY + str(time.strftime("%Y%m%d-%H%M-")) + "manager_server.log"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)

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
        server = ManagerServer(('127.0.0.1', 13373), ManagerServerHandler)
        server.logger = logger
        server.logger.info(" Starting server on IP 127.0.0.1, Port 13373")
        server.serve_forever()
        
    except KeyboardInterrupt:
        print colored("Manager> ", "blue", attrs=["bold"]) + "Goodbye!"
