#!/usr/bin/env python

from termcolor import colored
from xml.etree import ElementTree as ET
import subprocess
import sys
import time

# constants
CONF_FILE = "../realsibs.xml"

# find and kill existing sibs
subprocess.call(["killall", "-9", "sib-tcp"])
subprocess.call(["killall", "-9", "redsibd"])

# read the configuration file to see the real SIBs
tree = ET.parse(CONF_FILE)
root = tree.getroot()

# create and fill a dict to store the info about real SIBs
rsib = {}
for r in root.findall('SIB'):
    sib_name = r.find('name').text
    rsib[sib_name] = {}
    rsib[sib_name]["IP"] = r.find('IP').text
    rsib[sib_name]["port"] = int(r.find('port').text)
    rsib[sib_name]["type"] = r.find('type').text

nodes = {}
for r in rsib.keys():

    # starting redsib
    command = "redsibd"
    print colored("realsib_starter> ", "green", attrs=["bold"]) + "Starting redsibd"
    subprocess.Popen(command)

    # a little pause...
    time.sleep(25)

    # starting sib-tcp
    print colored("realsib_starter> ", "green", attrs=["bold"]) + "Starting sib-tcp for sib " + r
    command = ["sib-tcp", "-p", str(rsib[r]["port"])]
    subprocess.Popen(command)
	
# waiting for the quit signal
while True:
    try:
        cmd = raw_input('> ')
        if cmd == "quit":
            
            # find and kill existing sibs
            subprocess.call(["killall", "-9", "sib-tcp"])
            subprocess.call(["killall", "-9", "redsibd"])
            sys.exit(0)

    except EOFError:
        
        # find and kill existing sibs
        subprocess.call(["killall", "-9", "sib-tcp"])
        subprocess.call(["killall", "-9", "redsibd"])
        sys.exit(0)
    
