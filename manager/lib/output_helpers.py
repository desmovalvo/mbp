#!/usr/bin/python

# requirements
from termcolor import *

# helpers

# manager_server_print
def manager_server_print(level):

    """This is a little helper that's used to print a heading string in
    debug messages. Called with True returns a positive message while
    with the False parameter returns the heading for an error message"""

    if level:
        return colored("manager_Server> ", "blue", attrs=["bold"])
    else:
        return colored("manager_Server> ", "red", attrs=["bold"])


# requests_print
def requests_print(level):
    
    """This is a little helper that's used to print a heading string in
    debug messages. Called with True returns a positive message while
    with the False parameter returns the heading for an error message"""
    
    if level:
        return colored("requests_handler> ", "blue", attrs=["bold"])
    else:
        return colored("requests_handler> ", "red", attrs=["bold"])

# command_print
def command_print(command):
    
    """This is a little helper that's used to print a colored command name"""
    
    return colored(command, "cyan", attrs=["bold"])

