#!/usr/bin/python

# This file contains the most frequently used queries

# constants
ns = "http://smartM3Lab/Ontology.owl#"
rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#"

PREFIXES = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <""" + ns + ">"


def get_virtualisers(ancillary):

    query = PREFIXES + """SELECT ?id ?ip ?port
WHERE { ?id rdf:type ns:virtualiser . ?id ns:hasIP ?ip . ?id ns:hasPort ?port }"""

    return ancillary.execute_sparql_query(query)



def get_multisib_of_a_sib(sib_id, ancillary):

    query = PREFIXES + """SELECT ?s ?ip ?port
    WHERE { ?s ns:composedBy ns:""" + sib_id + """ . ?s ns:hasKpIp ?ip . ?s ns:hasKpPort ?port }"""
    return ancillary.execute_sparql_query(query)



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


def get_virtualiser_load(virtualiser_id, a):
    query = PREFIXES + """SELECT ?load
    WHERE { ns:""" + str(virtualiser_id) + """ ns:load ?load }"""

    result = a.execute_sparql_query(query)
    load = int(result[0][0][2])
    return load


def get_virtualiser_info(virtual_sib_id, a):
    query = PREFIXES + """SELECT ?ip ?port ?vid WHERE {?vid ns:hasRemoteSib ns:"""+ str(virtual_sib_id) + """ .
    ?vid ns:hasIP ?ip .
    ?vid ns:hasPort ?port}"""
    result = a.execute_sparql_query(query)
    
    # if the virtualiser exists
    if len(result) > 0:
        info = {}
        info["virtualiser_ip"] = result[0][0][2]
        info["virtualiser_port"] = result[0][1][2]
        info["virtualiser_id"] = (result[0][2][2]).split("#")[1]
        return info
    else:
        return None


def get_vmsib_list(virtual_sib_id, a):
    query = PREFIXES + """SELECT ?id ?ip ?port 
WHERE { ?id ns:composedBy ns:""" + str(virtual_sib_id) + """ . ?id ns:hasKpIp ?ip . ?id ns:hasKpPort ?port }"""

    result = a.execute_sparql_query(query)
    
    # exist some multi sib composed by this virtual sib
    if len(result) > 0:

        # send the RemoveSIBfromVMSIB request to all the vmsibs

        vmsib_list = {}
        for multisib in result:
            # get vmsib parameters
            vmsib_id = multisib[0][2].split("#")[1]
            vmsib_list[vmsib_id] = {} 
            vmsib_list[vmsib_id]["vmsib_ip"] = multisib[1][2]
            vmsib_list[vmsib_id]["vmsib_port"] = int(multisib[2][2])
        return vmsib_list

    else:
        return None


def get_all_sibs(a):
    SIBs = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                     PREFIX owl: <http://www.w3.org/2002/07/owl#>
                                     PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                                     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                                     PREFIX ns: <http://smartM3Lab/Ontology.owl#>
                      SELECT ?s
                      WHERE {{ ?s rdf:type ns:virtualSib } UNION { ?s rdf:type ns:publicSib } UNION { ?s rdf:type ns:virtualMultiSib }}""")


    # extract only the SIBs
    existing_sibs = []
    for k in SIBs:
        existing_sibs.append(str(k[0][2]).split("#")[1])
        
    return existing_sibs


def get_all_online_sibs(a):
    query = PREFIXES + """ SELECT ?id ?ip ?port ?owner 
        WHERE {{?id ns:hasKpIp ?ip . 
                ?id ns:hasKpPort ?port . 
                ?id ns:hasStatus "online" . 
                ?id rdf:type ns:virtualSib . 
                ?id ns:hasOwner ?owner}
                UNION 
		{?id ns:hasKpIp ?ip . 
                 ?id ns:hasKpPort ?port . 
                 ?id ns:hasStatus "online" . 
                 ?id rdf:type ns:virtualMultiSib }
                UNION
                {?id ns:hasKpIp ?ip . 
                ?id ns:hasKpPort ?port . 
                ?id ns:hasStatus "online" . 
                ?id rdf:type ns:publicSib . 
                ?id ns:hasOwner ?owner}}"""

    result = a.execute_sparql_query(query)

    sib_list = {}
    for i in result:
        sib_id = str(i[0][2].split('#')[1])
        sib_list[sib_id] = {} 
        sib_ip = sib_list[sib_id]["ip"] = str(i[1][2])
        sib_port = sib_list[sib_id]["port"] = str(i[2][2])
        if i[3][2] == None:
            sib_owner = sib_list[sib_id]["owner"] = "Virtual Multi SIB" 
        else:
            sib_owner = sib_list[sib_id]["owner"] = str(i[3][2]) 

    return sib_list 


def get_all_online_sibs_by_key(a, key, value):
    
    query = PREFIXES + """ SELECT ?id ?ip ?port ?owner 
        WHERE {{?id ns:hasKpIp ?ip . 
                ?id ns:hasKpPort ?port .
                ?id ns:hasStatus "online" . 
                ?id rdf:type ns:virtualSib . 
                ?id ns:hasOwner ?owner .
                ?id ns:""" + str(key) + '"' + str(value) + '"' +  """} 
                UNION 
		{?id ns:hasKpIp ?ip .
                 ?id ns:hasKpPort ?port . 
                 ?id ns:hasStatus "online" . 
                 ?id rdf:type ns:virtualMultiSib .
                 ?id ns:""" + str(key) + '"' + str(value) + '"' +  """}
                UNION
                {?id ns:hasKpIp ?ip . 
                ?id ns:hasKpPort ?port . 
                ?id ns:hasStatus "online" . 
                ?id rdf:type ns:publicSib . 
                ?id ns:hasOwner ?owner . 
                ?id ns:""" + str(key) + '"' + str(value) + '"' +  """}}"""

    result = a.execute_sparql_query(query)
    
    sib_list = {}
    for i in result:
        sib_id = str(i[0][2].split('#')[1])
        sib_list[sib_id] = {} 
        sib_ip = sib_list[sib_id]["ip"] = str(i[1][2])
        sib_port = sib_list[sib_id]["port"] = str(i[2][2])
        if i[3][2] == None:
            sib_owner = sib_list[sib_id]["owner"] = "Virtual Multi SIB" 
        else:
            sib_owner = sib_list[sib_id]["owner"] = str(i[3][2]) 

    return sib_list


def get_sib_info(sib_id, a):
    sib = a.execute_sparql_query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          PREFIX owl: <http://www.w3.org/2002/07/owl#>
          PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
          PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          PREFIX ns: <http://smartM3Lab/Ontology.owl#>
          SELECT ?ip ?port
          WHERE { ns:""" + sib_id + """ ns:hasKpIp ?ip . ns:""" + sib_id + """ ns:hasKpPort ?port }""")

    sib_info = {}
    # send a message to the virtualmultisib
    sib_info["sib_ip"] = str(sib[0][0][2])
    sib_info["sib_port"] = str(sib[0][1][2])
    return sib_info


def get_sibs_on_virtualiser(virtualiser_id, a):
    # get the list of the virtual sibs started on that virtualiser
    vsibs_query = PREFIXES + """SELECT ?vsib_id
    WHERE { ns:""" + virtualiser_id + """ ns:hasRemoteSib ?vsib_id }"""

    vsibs = a.execute_sparql_query(vsibs_query)

    sib_list = []
    for vsib in vsibs:
        sib_list.append(vsib[0][2])
        
    return sib_list

def get_all_sibs_on_virtualiser(virtualiser_id, a):
    # get the list of the virtual sibs started on that virtualiser
    vsibs_query = PREFIXES + """SELECT ?vsib_id
    WHERE {{ns:""" + virtualiser_id + """ ns:hasRemoteSib ?vsib_id} UNION {ns:""" + virtualiser_id + """ ns:hasVirtualMultiSib ?vsib_id}}"""

    vsibs = a.execute_sparql_query(vsibs_query)

    sib_list = []
    for vsib in vsibs:
        sib_list.append(vsib[0][2])
        
    return sib_list

def get_multisibs_on_virtualiser(virtualiser_id, a):
    # get the list of the virtual multi sibs started on that virtualiser

    vmsibs_query = PREFIXES + """SELECT ?vmsib_id
    WHERE { ns:""" + virtualiser_id + """ ns:hasVirtualMultiSib ?vmsib_id }"""
    vmsibs = a.execute_sparql_query(vmsibs_query)

    sib_list = []
    for vsib in vmsibs:
        sib_list.append(vsib[0][2])
        
    return sib_list


def get_public_sibs(a):
    
    q = PREFIXES + """SELECT ?id ?ip ?port WHERE { ?id rdf:type ns:publicSib . ?id ns:hasKpIp ?ip . ?id ns:hasKpPort ?port }"""
    return a.execute_sparql_query(q)
