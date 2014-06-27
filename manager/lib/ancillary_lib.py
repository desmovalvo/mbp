#!/usr/bin/python

# requirements
import socket
from query import *
from SIBLib import *
from smart_m3.m3_kp import *
from connection_helpers import *

def ancillary_check(ip, port):

    """Empty the ancillary SIB from useless triples"""

    # connect to the ancillary_sib
    ancillary = SibLib(ip, int(port))

    # Find all the online virtualisers
    virtualisers = get_virtualisers(ancillary)
    for v in virtualisers:
        
        # retrieve virtualiser's data
        v_id = v[0][2].split("#")[1]
        v_ip = v[1][2]
        v_port = int(v[2][2])
        
        # try to connect
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((v_ip, v_port))
            s.close()

        except socket.error:

            # delete virtualiser's data:
            # we must remove the virtual (multi) SIBs and the virtualiser

            # get and delete all the sibs registered to the given virtualiser
            sibs = get_all_sibs_on_virtualiser(v_id, ancillary)
            for s in sibs:

                # retrieve sib's data

                # check if it's part of a multi sib, then send it RemoveSIBfromVMSIB 
                r = get_multisib_of_a_sib(s.split("#")[1], ancillary)
                for vmsib in r:

                    vmsib_ip = vmsib[1][2]
                    vmsib_port = int(vmsib[2][2])

                    # if the virtual multi sib is not on the same virtualiser
                    # then we send a RemoveSIBfromVMSIB 
                    # otherwise the vmsib will also be deleted
                    msg = {"command":"RemoveSIBfromVMSIB", "sib_list":[s.split("#")[1]], "vmsib_id" : vmsib[0][2].split("#")[1] }
                    virtualiser_request(vmsib_ip, vmsib_port, msg)                    
                    
                # delete the virtual sib
                ancillary.remove(Triple(None, None, URI(s)))
                ancillary.remove(Triple(URI(s), None, None))
                
            # delete the virtualiser
            ancillary.remove(Triple(URI(ns + v_id), None, None))
