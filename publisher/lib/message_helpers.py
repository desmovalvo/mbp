#!/usr/bin/python

# tags
START_TAG = "<SSAP_message>"
END_TAG = "</SSAP_message>"


######################################################
#
# complete_message_present
#
######################################################

def complete_message_present(ssap_message):

    """This function verifies if ssap_message contains a
    complete message"""

    if (ssap_message.find(START_TAG) >= 0) and (ssap_message.find(END_TAG) > 0):
        return True
    else:
        return False


######################################################
#
# extract_first_message
#
######################################################

def extract_first_message(ssap_message):

    """This function extracts the first message and returns it,
    followed by the remaining part"""

    # position of the first and the last chars
    start_msg = ssap_message.find(START_TAG)
    end_msg = ssap_message.find(END_TAG) + len(END_TAG)

    # the first message is...
    first_msg = ssap_message[start_msg:end_msg]

    # and the last part is...
    remaining = ssap_message.replace(first_msg, '')

    # return the first message
    return [first_msg, remaining]


######################################################
#
# extract_complete_messages
#
######################################################

def extract_complete_messages(ssap_message):

    """This function extracts all the complete messages from
    ssap_message and returns them, followed by the remaining part"""

    complete = []
    while complete_message_present(ssap_message):

        # extract the first complete message
        c, ssap_message = extract_first_message(ssap_message)
        complete.append(c)

    return [complete, ssap_message]
