#!/usr/bin/python

############################################################
#
# SSAP message templates
#
############################################################

SSAP_MESSAGE_TEMPLATE = '''<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>%s</transaction_type>
<message_type>CONFIRM</message_type>
<transaction_id>%s</transaction_id>
%s
</SSAP_message>'''

SSAP_SUCCESS_PARAM_TEMPLATE = '<parameter name = "status">%s</parameter>'

SSAP_BNODES_PARAM_TEMPLATE = '<parameter name = "bnodes"><urllist>%s</urllist></parameter>'

