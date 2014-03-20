#!/usr/bin/python

############################################################
#
# SSAP message templates
#
############################################################

SSAP_MESSAGE_CONFIRM_TEMPLATE = '''<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>%s</transaction_type>
<message_type>CONFIRM</message_type>
<transaction_id>%s</transaction_id>
%s
</SSAP_message>'''

SSAP_SUCCESS_PARAM_TEMPLATE = '<parameter name = "status">%s</parameter>'

SSAP_BNODES_PARAM_TEMPLATE = '<parameter name = "bnodes"><urllist>%s</urllist></parameter>'

### Templates used to build query results
SSAP_RESULTS_SPARQL_PARAM_TEMPLATE = """
<parameter name="status">m3:Success</parameter>
<parameter name="results">
<sparql xmlns="http://www.w3.org/2005/sparql-results#">    
%s
</sparql>
</parameter>
"""

SSAP_HEAD_TEMPLATE = """<head>
%s</head>"""

SSAP_VARIABLE_TEMPLATE = """<variable name="%s"/>
"""

SSAP_RESULTS_TEMPLATE = """<results>
%s</results>
"""

SSAP_RESULT_TEMPLATE = """<result>
%s</result>
"""

SSAP_BINDING_TEMPLATE = """<binding name="%s"><uri>%s</uri>
</binding>
"""

SSAP_MESSAGE_REQUEST_TEMPLATE = '''<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>%s</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>
%s
</SSAP_message>'''

SSAP_SUCCESS_PARAM_TEMPLATE = '<parameter name = "status">%s</parameter>'

SSAP_RESULTS_RDF_PARAM_TEMPLATE = """
<parameter name="status">m3:Success</parameter>
<parameter name="results">
%s
</parameter>
"""

SSAP_TRIPLE_TEMPLATE = """
<triple>
<subject type="uri">%s</subject>
<predicate>%s</predicate>
<object type="uri">%s</object>
</triple>
"""

SSAP_TRIPLE_LIST_TEMPLATE = """
<triple_list>
%s
</triple_list>
"""
