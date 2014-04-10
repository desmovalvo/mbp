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

    # start a virtual sib
    thread.start_new_thread(virtualiser, ("localhost", 10010, "localhost", 10011))
    
    # insert information in the ancillary SIB
    a = SibLib("127.0.0.1", 10088)
    t = Triple(URI(virtual_sib_id), URI("hasIpPort"), URI("127.0.0.1-10011"))
    a.insert(t)
    

    # return virtual sib id
    return virtual_sib_id

def NewVirtualMultiSIB():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("NewVirtualMultiSIB", "cyan", attrs=["bold"])

def Discovery():
    # debug print
    print colored("request_handlers> ", "blue", attrs=["bold"]) + "executing method " + colored("Discovery", "cyan", attrs=["bold"])
