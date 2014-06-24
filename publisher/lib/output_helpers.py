#!/usr/bin/python

# requirements
from termcolor import *

# helpers

# publisher_print
def publisher_print(level):

    """This is a little helper that's used to print a heading string in
    debug messages. Called with True returns a positive message while
    with the False parameter returns the heading for an error message"""

    if level:
        return colored("publisher> ", "blue", attrs=["bold"])
    else:
        return colored("publisher> ", "red", attrs=["bold"])


# command print
def command_print(command):

    """This is a little helper that's used to print the command name"""

    return colored(command, "cyan", attrs=["bold"])
