#!/usr/bin/python

from smart_m3.m3_kp import *
import xml.etree.cElementTree as ET

def extract_sparql_triples(doc):
    # Get the namespace
    for parameter in doc.findall("parameter"):
        if parameter.attrib["name"] == "results":
            sparql = parameter.getchildren()[0]
            namespace = sparql.tag[:-6]
    
    # The following code built the sparql result list
    rs = []
    for result in doc.iter('{http://www.w3.org/2005/sparql-results#}result'):
        r = []
        for binding in result.getchildren():
    
            field = binding.getchildren()[0]
            b = [unicode(binding.attrib["name"]), unicode(field.tag.replace(namespace, '')), unicode(field.text)]
            r.append(b)
    
        rs.append(r)
    
    return rs

# The following code built the sparql result list
def extract_rdf_triples(doc):

    result = []
    for triple in doc.iter('triple_list'):
        for field in triple.getchildren():
            tl = {}
            for f in field.getchildren():
                if f.attrib.has_key("type"):
                    if f.attrib["type"].lower() == "uri":
                        el = URI(f.text)
                    elif f.attrib["type"].lower() == "literal":
                        el = Literal(f.text)
                    else:
                        el = URI(f.text)
                else:
                    el = URI(f.text)
                tl[f.tag] = el
            result.append(Triple(tl["subject"], tl["predicate"], tl["object"]))
    return result 


def build_dict(doc):
    d = {} 
    d["message_type"] = doc.find("message_type").text
    d["transaction_type"] = doc.find("transaction_type").text
    d["space_id"] = doc.find("space_id").text
    d["node_id"] = doc.find("node_id").text
    d["transaction_id"] = doc.find("transaction_id").text
    for p in doc.findall("parameter"):
        if p.attrib["name"] == "status":
            d["status"] = p.text
        if p.attrib["name"] == "subscription_id":
            d["subscription_id"] = p.text 
        if p.attrib["name"] == "type":
            d["query_type"] = p.text

    return d
