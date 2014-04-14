#!/usr/bin/python

# requirements
from termcolor import *
import uuid
from SIBLib import SibLib
from smart_m3.m3_kp import *
from virtualiser import *
import threading
import thread


#functions

def NewRemoteSIB():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewRemoteSIB", "cyan", attrs=["bold"])

    # virtual sib id
    virtual_sib_id = str(uuid.uuid4())

    # create two sockets
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # generating two random ports
    while True:
        kp_port = random.randint(10000, 11000)
        if sock.connect_ex(kp_port) == 0:
            print "estratta la porta %s"%(str(kp_port))
            break

    while True:
        pub_port = random.randint(10000, 11000)
        if sock.connect_ex(pub_port) == 0:
            print "estratta la porta %s"%(str(pub_port))
            break        

    # start a virtual sib
    thread.start_new_thread(virtualiser, ("localhost", kp_port, "localhost", pub_port))
    
    # insert information in the ancillary SIB
    a = SibLib("127.0.0.1", 10088)
    t = Triple(URI(virtual_sib_id), URI("hasIpPort"), URI("127.0.0.1-" + str(pub_port)))
    a.insert(t)
    
    # return virtual sib id
    return virtual_sib_id

def NewVirtualMultiSIB():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])

def Discovery():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("Discovery", "cyan", attrs=["bold"])
