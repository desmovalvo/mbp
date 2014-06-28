#!/usr/bin/python

# requirements
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from lib import SIBLib
import sys
import json
from lib.connection_helpers import *

# constants
ns = "http://smartM3Lab/Ontology.owl#"
anc_ip = "10.143.250.250"
anc_port = 10088

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
    

def get_virtualisers(a):
    # List of the available virtualisers
    virtualisers_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s ?ip ?port ?load 
    WHERE { ?s rdf:type ns:virtualiser .
          	?s ns:hasIP ?ip .
          	?s ns:hasPort ?port .
          	?s ns:load ?load }
"""

    # execute query
    res = a.execute_sparql_query(virtualisers_query)

    # create html code
    v = "<h2>Virtualisers</h2><ul>"
    for el in res:
        v = v + virtualiser_template%(str(el[0][2]),
                                      str(el[1][2]),
                                      str(el[2][2]),
                                      str(el[3][2]))

    v = v + """</ul>"""
    
    # return value
    return v


def show_virtualisers(virtualiser_list):

    # create html code
    v = "<h2>Virtualisers</h2><ul>"
    for el in virtualiser_list:
        v = v + virtualiser_template%(str(el),
                                      str(el[0]),
                                      str(el[1]),
                                      str(el[2]))

    v = v + """</ul>"""
    
    # return value
    return v



def get_virtualpublicSIBs(a):
    # List of the available virtualisers

    virtualpublicSIBs_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?sib ?ip ?port ?owner
WHERE {?sib ns:hasKpIp ?ip . ?sib ns:hasKpPort ?port . ?sib ns:hasOwner ?owner . ?sib ns:hasStatus "online" }"""

    # execute query
    res = a.execute_sparql_query(virtualpublicSIBs_query)

    # create html code
    v = "<h2>Virtual/Public SIBs</h2><ul>"
    for el in res:
        # print el[0]
        # ip = str(el[3][2].replace(ns, "")).split("-")[0]
        # port = int(str(el[3][2].replace(ns, "")).split("-")[1])
        
    #     if el[2][2].replace(ns, "") == "online":
    #         c = get_sib_content(ip, port)
    #     else:
    #         c = ""

        v = v + virtualpublic_sib_template%(str(el[0][2].replace(ns, "")),
                                            str(el[1][2]),
                                            str(el[2][2]),
                                            str(el[3][2]))
    v = v + """</ul>"""
    
    # return value
    return v


def get_vmSIBs(a):
    # List of the available virtualisers
    vmSIBs_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?vmsibid ?ip ?port ?sib_id
    WHERE { ?vmsibid rdf:type ns:virtualMultiSib .
            ?vmsibid ns:hasStatus "online" .
            ?vmsibid ns:hasKpIp ?ip  .
            ?vmsibid ns:hasKpPort ?port  .
            ?vmsibid ns:composedBy ?sib_id}"""

    # execute query
    res = a.execute_sparql_query(vmSIBs_query)

    # create html code
    v = "<h2>Virtual Multi SIBs</h2><ul>"
    vmsib_dict = {}
    for el in res:
        print el
        if not vmsib_dict.has_key(el[0][2]):
            vmsib_dict[el[0][2]] = {}
            vmsib_dict[el[0][2]]["ip"] = el[1][2]
            vmsib_dict[el[0][2]]["list"] = []
            vmsib_dict[el[0][2]]["port"] = el[2][2]
        vmsib_dict[el[0][2]]["list"].append(el[3][2])
        # v = v + "<li>" + el[0][2] + "</li>"
        # v = v + vmsib_template%(str(el[0][2].replace(ns, "")),
        #                              str(el[1][2].replace(ns, "")),
        #                              str(el[2][2].replace(ns, ""))
        #     )

    for vmsib in vmsib_dict.keys():
        l = "<ul>"
        for sib in vmsib_dict[vmsib]["list"]:
            l = l + "<li>" + sib.split("#")[1] + "</li>"
        l = l + "</ul>"
        v = v + vmsib_template%(vmsib.split("#")[1], vmsib_dict[vmsib]["ip"], vmsib_dict[vmsib]["port"], l)

    v = v + """</ul>"""
    
    # return value
    return v


# BaseHTTPRequestHandler re-implementation
class AncillaryRequestHandler(BaseHTTPRequestHandler):

    #handle GET command
    def do_GET(s):
        """Respond to a GET request."""

        # connection to the Ancillary SIB
        a = SIBLib.SibLib(anc_ip, anc_port)
        
        # get the informations from the Ancillary SIB  
        print "--- GET the list of the virtualisers"
        # v = get_virtualisers(a)
        #####
        cmd = {"command": "GetVirtualisers"}
        cnf = None
        while cnf == None:
            cnf = manager_request(manager_ip, manager_port, cmd)

        virtualiser_list = cnf["virtualiser_list"]
        v = show_virtualisers(virtualiser_list)
        #####
        
        # get the remote SIBs
        print "--- GET the list of the remote SIBs"
        r = get_virtualpublicSIBs(a)
        # #####
        # cmd = {"command": "GetVirtualPublicSIBs"}
        # cnf = manager_request(manager_ip, manager_port, cmd)
        # v = cnf["sib_list"]
        #####


        # get the virtual multi SIBs
        print "--- GET the list of the VMSIBs"
        vm = get_vmSIBs(a)
        # #####
        # cmd = {"command": "GetVirtualMultiSIBs"}
        # cnf = manager_request(manager_ip, manager_port, cmd)
        # v = cnf["vmsib_list"]
        # #####

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

    if len(sys.argv) == 3:
	global anc_ip
        anc_ip = sys.argv[1]
        global anc_port 
	anc_port = int(sys.argv[2])
        
    server_address = ('0.0.0.0', 8000)
    httpd = HTTPServer(server_address, AncillaryRequestHandler)
    print('Ancillary explorer started on http://localhost:8000 ...')
    httpd.serve_forever()
    

if __name__ == '__main__':
    run()

