#!/usr/bin/python

# requirements
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from lib import SIBLib
import sys
import json
from lib.connection_helpers import *

# constants
ns = "http://smartM3Lab/Ontology.owl#"

# Open the configuration file to read ip and port of the manager server
conf_file = open("web_exp.conf", "r")
conf = json.load(conf_file)

# Manager parameters
manager_ip = conf["manager"]["ip"]
manager_port = conf["manager"]["port"]
        
# Closing the configuration file
conf_file.close()

# templates
virtualiser_template = """
    <li>%s</li><br>
    <ul>
        <li><b>IP:</b> %s</li>
        <li><b>Port:</b> %s</li>
        <li><b>Load:</b> %s</li>
    </ul><p>
"""

virtualpublic_sib_template = """
    <li>%s</li><br>
    <ul>
        <li><b>IP:</b> %s</li>
        <li><b>Port:</b> %s</li>
        <li><b>Owner:</b> %s</li>
    </ul><p>
"""

vmsib_template = """
    <li>%s</li><br>
    <ul>
        <li><b>IP:</b> %s</li>
        <li><b>Port:</b> %s</li>
        <li><b>Composed by:</b> %s</li>
    </ul><p>
"""

# functions
def get_sib_content(ip, port):

    # get all the triples from the given sib

    b = SIBLib.SibLib(ip, port)
    query = """SELECT ?s ?p ?o WHERE { ?s ?p ?o }"""    
    res = b.execute_sparql_query(query)
    tlist = "<table border=0>"
    for t in res:
        tripla = "<tr><td>" + str(t[0][2]) + "</td><td>" + str(t[1][2]) + "</td><td>" + str(t[2][2]) + "</td></tr>"
        tlist = tlist + tripla
    tlist = tlist + "</table>"
    return tlist
    


def show_virtualisers(virtualiser_list):
    print "SHOW VIRTUALISER " + str(virtualiser_list)
    # create html code
    v = "<h2>Virtualisers</h2><ul>"
    for el in virtualiser_list:
        virtualiser_id = str(el)
        ip = virtualiser_list[virtualiser_id]["virtualiser_ip"]
        port = virtualiser_list[virtualiser_id]["virtualiser_port"]
        load = virtualiser_list[virtualiser_id]["virtualiser_load"]

        v = v + virtualiser_template%(virtualiser_id,
                                      str(ip),
                                      str(port),
                                      str(load))

    v = v + """</ul>"""
    
    # return value
    return v


def show_sibs(sib_list):
    print "SHOW SIBs " + str(sib_list)
    # create html code
    v = "<h2>Virtual/Public SIBs</h2><ul>"
    for el in sib_list:
        sib_id = str(el)
        ip = sib_list[sib_id]["sib_ip"]
        port = sib_list[sib_id]["sib_port"]
        owner = sib_list[sib_id]["sib_owner"]

        v = v + virtualpublic_sib_template%(sib_id,
                                            str(ip),
                                            str(port),
                                            str(owner))
    v = v + """</ul>"""
    
    # return value
    return v


def show_multi_sibs(multi_sib_list):
    print "SHOW SIBs " + str(multi_sib_list)



    v = "<h2>Virtual Multi SIBs</h2><ul>"
    for el in multi_sib_list:
        multi_sib_id = str(el)
        l = "<ul>"
        for sib in multi_sib_list[multi_sib_id]["list"]:
            l = l + "<li>" + sib.split("#")[1] + "</li>"
        l = l + "</ul>"
        v = v + vmsib_template%(multi_sib_id, multi_sib_list[multi_sib_id]["ip"], multi_sib_list[multi_sib_id]["port"], l)

    v = v + """</ul>"""
    
    # return value
    return v


# BaseHTTPRequestHandler re-implementation
class AncillaryRequestHandler(BaseHTTPRequestHandler):

    #handle GET command
    def do_GET(s):
        """Respond to a GET request."""

        # get the list of the virtualisers
        # send GetVirtualisers request to the manager
        cmd = {"command": "GetVirtualisers"}
        cnf = None
        while cnf == None:
            cnf = manager_request(manager_ip, manager_port, cmd)

        virtualiser_list = cnf["virtualiser_list"]
        v = show_virtualisers(virtualiser_list)
        
        # get the list of the virtual and public SIBs"
        # send GetVirtualPublicSIBs request to the manager
        cmd = {"command": "GetVirtualPublicSIBs"}
        cnf = None
        while cnf == None:
            cnf = manager_request(manager_ip, manager_port, cmd)
        
        sib_list = cnf["sib_list"]
        r = show_sibs(sib_list)
        
        # get the list of the virtual multi SIBs
        # send GetVirtualMultSIBs request to the manager
        cmd = {"command": "GetVirtualMultiSIBs"}
        cnf = None
        while cnf == None:
            cnf = manager_request(manager_ip, manager_port, cmd)
        
        multi_sib_list = cnf["multi_sib_list"]
        vm = show_multi_sibs(multi_sib_list)

        # output the informations
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()

        # head
        s.wfile.write("<html><head><title>Ancillary SIB Explorer</title>")

        # style
        s.wfile.write("<style>")
        s.wfile.write("body { background:#222333; }")
        s.wfile.write("body { font-size:10pt; }")
        s.wfile.write("body { font-family:arial; }")
        s.wfile.write("table { font-family:arial; }")
        s.wfile.write("table { font-size:7pt; }")
        s.wfile.write("table { border-spacing: 20px; }")
        s.wfile.write("header { color:#ffffff; }")
        s.wfile.write("header { padding:20px; }")
        s.wfile.write("article { border:1px solid; }")
        s.wfile.write("article { padding:20px; }")
        s.wfile.write("article { margin:20px; }")
        s.wfile.write("article { background:#ffffff; }")
        s.wfile.write("article { border-radius:15px; }")
        s.wfile.write("article { box-shadow: 10px 10px 5px #000000; }")
        s.wfile.write("</style>")

        # head end
        s.wfile.write("</head>")

        # body
        s.wfile.write("<body><header><h1>Ancillary SIB Explorer</h1></header>")
        s.wfile.write("<article>%s</article>" % str(v))
        s.wfile.write("<article>%s</article>" % str(r))
        s.wfile.write("<article>%s</article>" % str(vm))
        s.wfile.write("</body></html>")



def run():
        
    server_address = ('0.0.0.0', 8000)
    httpd = HTTPServer(server_address, AncillaryRequestHandler)
    print('Ancillary explorer started on http://localhost:8000 ...')
    httpd.serve_forever()
    

if __name__ == '__main__':
    run()

