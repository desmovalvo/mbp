# from smart_m3.m3_kp import *
from smart_m3.m3_kp_api import *
import uuid

rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
owl = "http://www.w3.org/2002/07/owl#"
xsd = "http://www.w3.org/2001/XMLSchema#"
rdfs = "http://www.w3.org/2000/01/rdf-schema#"
ns = "http://smartM3Lab/Ontology.owl#"

class SibLib():

    # __init__: constructor method
    def __init__(self, server_ip, server_port):
        self.node = m3_kp_api(False, server_ip, server_port)

    # join_sib: method to join the sib
    # since this method is no longer required, it only calls pass()
    def join_sib(self):
        pass

    # leave_sib: method to leave the sib
    def leave_sib(self):
        self.node.leave()
                
    # insert: method to insert a triple in the sib
    def insert(self, triples):

        # build a list if the parameter is a Triple
        if type(triples).__name__ != 'list':
            t = [triples]
        else:
            t = triples

        # insert
        self.node.load_rdf_insert(t)

    # remove: method to remove a triple from the sib
    def remove(self, triples):

        # build a list if the parameter is a Triple
        if type(triples).__name__ != 'list':
            t = [triples]
        else:
            t = triples
            
        # remove
        self.node.load_rdf_remove(t)

    # update: method to update a triple in the sib
    def update(self, triple_to_insert, triple_to_remove):

        # build a list if the parameter is a Triple
        if type(triple_to_insert).__name__ != 'list':
            ti = [triple_to_insert]
        else:
            ti = triple_to_insert

        # build a list if the parameter is a Triple
        if type(triple_to_remove).__name__ != 'list':
            tr = [triple_to_remove]
        else:
            tr = triple_to_remove
        
        # remove
        self.node.load_rdf_remove(tr)

        # insert
        self.node.load_rdf_insert(ti)

    # create_rdf_subscription: method to subscribe with rdf to a triple
    def create_rdf_subscription(self, triple, HandlerClass):

        # build a list if the parameter is a Triple
        if type(triple).__name__ != 'list':
            t = [triple]
        else:
            t = triple

        # subscription
        return self.node.load_subscribe_RDF(t, HandlerClass)

    # rdf initial results
    def rdf_initial_results(self):

        return self.node.result_RDF_first_sub

    # create_sparql_subscription: method to subscribe with SPARQL to a triple
    def create_sparql_subscription(self, query, HandlerClass):
        
        return self.node.load_subscribe_sparql(query, HandlerClass)

    # sparql initial results
    def sparql_initial_results(self):

        return self.node.result_sparql_first_sub

    # execute_sparql_query: method to execute a sparql query, given a string
    def execute_sparql_query(self, query):
        self.node.load_query_sparql(query)
        return self.node.result_sparql_query        

    # execute_rdf_query: method to execute a rdf query, given a triple
    def execute_rdf_query(self, triple):
        self.node.load_query_rdf(triple)
        return self.node.result_rdf_query

    # unsubscribe
    def unsubscribe(self, sub):
        self.node.load_unsubscribe(sub)
