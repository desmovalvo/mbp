#!/usr/bin/pyhton

# requirements
import uuid

# the Subreq class
class Subreq:

    # initialization (called on SUBSCRIBE REQUEST)
    def __init__(self, connection, info):#node_id, request_transaction_id):
        self.conn = connection # this is the connection to the kp
        self.node_id = info["node_id"]
        self.space_id = info["space_id"]
        self.request_transaction_id = info["transaction_id"]
        #self.subscription_id = uuid.uuid4()
        self.subscription_id = None
        self.real_sib_id = {}
        self.result = []

    # received_confirm: when we receive a SUBSCRIBE CONFIRM we store
    # the corrispondence between real_sib and the subscription_id that
    # it desires. It is called after each SUBSCRIBE CONFIRM
    def received_confirm(self, socket, subscription_id):
        self.real_sib_id[str(socket)] = subscription_id

    # when we receive an UNSUBSCRIBE REQUEST we have to transform the
    # virtual_subscription_id that the KP puts in the message with the
    # real_subscription_id that the real sib expects
    def get_real_subscription_id(self, socket):
        if self.real_sib_id.has_key(str(socket)):
            return self.real_sib_id[str(socket)]

    # when we receive an UNSUBSCRIBE CONFIRM, in the message we only
    # have the real_sib_id, so, given a real_subscription_id, this
    # method checks whether it belongs to this instance and eventually
    # returns the virtual_sub_id
    def get_virtual_subscription_id(self, socket, real_subscription_id):
        if self.real_sib_id.has_key(str(socket)):
            if self.real_sib_id[str(socket)] == real_subscription_id:
                return self.virtual_subscription_id
            else:
                return False
