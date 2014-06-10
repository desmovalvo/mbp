#!/usr/bin/python

# requirements
from Tkinter import *
import Tkinter
from PIL import ImageTk, Image
import tkFont
from tkMessageBox import showinfo
from ttk import *
from SIBLib import *
from termcolor import *
import sys

# font
TITLE_FONT = ("Helvetica", 18, "bold")

general_sparql_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s ?p ?o
WHERE { ?s ?p ?o }"""


class RDFIndicationHandler:

    def __init__(self, app):	
        self.app = app

    def handle(self, added, removed):

        # enable and clear the text area
        self.app.results_text.config(state = NORMAL)
        self.app.results_text.delete(1.0, END)
        
        # notify the added triples
        self.app.results_text.insert(INSERT, "INDICATION - triples inserted:\n")
        ta = ""
        for t in added:
            ta = ta + str(t[0]) + " " + str(t[1]) + " " + str(t[2]) + "\n" 
        self.app.results_text.insert(INSERT, ta + "\n")

        # notify the removed triples
        self.app.results_text.insert(INSERT, "INDICATION - triples deleted:\n")
        td = ""
        for t in removed:
            td = td + str(t[0]) + " " + str(t[1]) + " " + str(t[2]) + "\n" 
        self.app.results_text.insert(INSERT, td + "\n")
            
        # disable the text area
        self.app.results_text.config(state = DISABLED)
