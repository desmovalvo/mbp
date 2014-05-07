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


# <SSAP_message>
# <node_id>a286ddc6-1398-463d-83bf-f5fada42b47f-6736c3fe-a2b3-4a8a-a159-fa13688f76ab</node_id>
# <space_id>X</space_id>
# <transaction_type>UNSUBSCRIBE</transaction_type>
# <message_type>REQUEST</message_type>
# <transaction_id>5</transaction_id>
# <parameter name = "subscription_id">http://subscribe_graph#a286ddc6-1398-463d-83bf-f5fada42b47f-6736c3fe-a2b3-4a8a-a159-fa13688f76ab_3</parameter>
# </SSAP_message>


SSAP_SUCCESS_PARAM_TEMPLATE = '<parameter name = "status">%s</parameter>'

SSAP_RESULTS_RDF_PARAM_TEMPLATE = """
<parameter name="status">m3:Success</parameter>
<parameter name="results">
%s
</parameter>
"""

SSAP_RESULTS_SUB_RDF_PARAM_TEMPLATE = """
<parameter name="status">m3:Success</parameter>
<parameter name="subscription_id">%s</parameter>
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

SSAP_INDICATION_TEMPLATE = """
<SSAP_message>
<message_type>INDICATION</message_type>
<transaction_type>SUBSCRIBE</transaction_type>
<space_id>%s</space_id>
<node_id>%s</node_id>
<transaction_id>%s</transaction_id>
<parameter name="ind_sequence">%s</parameter>
<parameter name="subscription_id">%s</parameter>
<parameter name="new_results">%s</parameter>
<parameter name="obsolete_results">%s</parameter>
</SSAP_message>
"""
