#!/usr/bin/python

# requirements
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from lib import SIBLib
import sys

# constants
ns = "http://smartM3Lab/Ontology.owl#"

anc_ip = "localhost"
anc_port = 10088

# templates
virtualiser_template = """
    <li>%s</li><br>
    <ul>
        <li><b>IP:</b> %s</li>
        <li><b>Port:</b> %s</li>
        <li><b>Load:</b> %s</li>
    </ul><p>
"""

remote_sib_template = """
    <li>%s</li><br>
    <ul>
        <li><b>Owner:</b> %s</li>
        <li><b>Status:</b> %s</li>
        <li><b>KP ip and port:</b> %s</li>
        <li><b>Pub ip and port:</b> %s</li>
    </ul><p>
"""

# functions
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
        v = v + virtualiser_template%(str(el[0][2].replace(ns, "")),
                                      str(el[1][2].replace(ns, "")), 
                                      str(el[2][2].replace(ns, "")), 
                                      str(el[3][2].replace(ns, "")))
    v = v + """</ul>"""
    
    # return value
    return v


def get_remoteSIBs(a):
    # List of the available virtualisers
    remoteSIBs_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s ?owner ?status ?kpipport ?pubipport
    WHERE { ?s ns:hasOwner ?owner .
      		?s ns:hasStatus ?status .
          	?s ns:hasKpIpPort ?kpipport .
          	?s ns:hasPubIpPort ?pubipport }
"""

    # execute query
    res = a.execute_sparql_query(remoteSIBs_query)

    # create html code
    v = "<h2>Remote SIBs</h2><ul>"
    for el in res:
        v = v + remote_sib_template%(str(el[0][2].replace(ns, "")),
                                     str(el[1][2].replace(ns, "")),
                                     str(el[2][2].replace(ns, "")),
                                     str(el[3][2].replace(ns, "")),
                                     str(el[4][2].replace(ns, "")),
            )
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
        a.join_sib()
        
        # get the informations from the Ancillary SIB  
        print "--- GET the list of the virtualisers"
        v = get_virtualisers(a)
        
        # get the remote SIB
        print "--- GET the list of the remote SIBs"
        r = get_remoteSIBs(a)
        
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
        s.wfile.write("</body></html>")



def run():

    if len(sys.argv) == 3:
	anc_ip = sys.argv[1]
	anc_port = sys.argv[2]

    server_address = ('0.0.0.0', 8000)
    httpd = HTTPServer(server_address, AncillaryRequestHandler)
    print('Ancillary explorer started on http://localhost:8000 ...')
    httpd.serve_forever()
    

if __name__ == '__main__':
    run()

