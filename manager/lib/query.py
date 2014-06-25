#!/usr/bin/python

# This file contains the most frequently used queries

def get_sib_ip_port(sib_id, ancillary):

    query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?ip ?port
WHERE { ns:""" + sib_id + """ ns:hasStatus "online" .
ns:""" + sib_id + """ ns:hasKpIp ?ip . ns:""" + sib_id + """ ns:hasKpPort ?port }"""

    results = ancillary.execute_sparql_query(query)
    return results 


def get_best_virtualiser(ancillary):
    query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT DISTINCT ?s ?ip ?port
WHERE { ?s rdf:type ns:virtualiser .
        ?s ns:load ?o .
        ?s ns:hasIP ?ip .
        ?s ns:hasPort ?port .
        OPTIONAL { ?loaded rdf:type ns:virtualiser .
                   ?loaded ns:load ?oo .
        FILTER (?oo < ?o)}
        FILTER(!bound (?loaded))
}
LIMIT 1"""
    
    result = ancillary.execute_sparql_query(query)
    return result
