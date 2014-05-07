#!/usr/bin/python

# requirements
from termcolor import *

# helpers

# virtserver_print
def virtserver_print(level):

    """This is a little helper that's used to print a heading string in
    debug messages. Called with True returns a positive message while
    with the False parameter returns the heading for an error message"""

    if level:
        return colored("Virtualiser> ", "blue", attrs=["bold"])
    else:
        return colored("Virtualiser> ", "red", attrs=["bold"])


# vmsib_print
def vmsib_print(level):
    
    """This is a little helper that's used to print a heading string in
    debug messages. Called with True returns a positive message while
    with the False parameter returns the heading for an error message"""
    
    if level:
        return colored("virtualMultiSIB> ", "blue", attrs=["bold"])
    else:
        return colored("virtualMultiSIB> ", "red", attrs=["bold"])


# reqhandler_print
def reqhandler_print(level):

    """This is a little helper that's used to print a heading string in
    debug messages. Called with True returns a positive message while
    with the False parameter returns the heading for an error message"""

    if level:
        return colored("request_handlers> ", "blue", attrs=["bold"])
    else:
        return colored("request_handlers> ", "red", attrs=["bold"])


# treplies_print
def treplies_print(level):

    """This is a little helper that's used to print a heading string in
    debug messages. Called with True returns a positive message while
    with the False parameter returns the heading for an error message"""

    if level:
        return colored("treplies> ", "blue", attrs=["bold"])
    else:
        return colored("treplies> ", "red", attrs=["bold"])
