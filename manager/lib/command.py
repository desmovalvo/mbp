#!/usr/bin/python

# requirements
import json

COMMANDS = {
    "RegisterPublicSIB": ["owner", "ip", "port"],
    "NewRemoteSIB" : ["owner", "sib_id"],
    "NewVirtualMultiSIB": ["sib_list"],
    "NewVirtualiser": ["id", "ip", "port"],
    "DiscoveryAll" : [],
    "DiscoveryWhere" : ["sib_profile"],
    "DeleteRemoteSIB" : ["virtual_sib_id"],
    "DeleteSIB" : ["sib_id"],
    "DeleteVirtualiser" : ["id"],
    "GetSIBStatus": ["sib_id"],
    "SetSIBStatus": ["sib_id", "status"],
    "AddSIBtoVMSIB": ["vmsib_id", "sib_list"],
    "RemoveSIBfromVMSIB": ["vmsib_id", "sib_list"]
    }

# class
class Command:

    # constructor
    def __init__(self, cmd_dict):
        
        # parse json
        self.data = cmd_dict

        # create attributes from data
        for k in self.data.keys():
            setattr(self, k, self.data[k])

        # validate command
        self.valid = self.validate()


    # validate command presence
    def validate_command_presence(self):
        """Check if the received json message contains the 'command' keyword"""
        if hasattr(self, "command"):
            return True
        else:
            self.invalid_cause = "No command supplied."
            return False


    # validate command name
    def validate_command_name(self):
        """Check if the received command is a valid one"""
        if self.command in COMMANDS.keys():
            return True
        else:
            self.invalid_cause = "Invalid command."
            return False
    

    # validate command arguments
    def validate_command_arguments(self):
        """Check if the received command has the required arguments"""
        for arg in COMMANDS[self.command]:
            if not hasattr(self, arg):
                self.invalid_cause = "Wrong arguments."
                return False
        return True


    # validate the whole command
    def validate(self):
        """Check the validity of the whole command"""

        # check if we received a command
        if not self.validate_command_presence():
            return False
        else:
            # check if we had a valid command
            if not self.validate_command_name():
                return False                
            else:
                # check the arguments
                if not self.validate_command_arguments():
                    return False
                else:
                    self.invalid_cause = None
                    return True
