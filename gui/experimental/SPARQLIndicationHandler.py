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


class SPARQLIndicationHandler:

    def __init__(self, app):	
        self.app = app

    def handle(self, added, removed):

        # enable and clear the text area
        self.app.results_text.config(state = NORMAL)
        self.app.results_text.delete(1.0, END)
        
        print added

        # notify the added triples
        self.app.results_text.insert(INSERT, "INDICATION - triples inserted:\n")
        s = ""
        for t in added:
            for el in t:
                s = s + str(el[2]) + " "
            s = s + "\n"

        self.app.results_text.insert(INSERT, s + "\n")

        # notify the removed triples
        self.app.results_text.insert(INSERT, "INDICATION - triples deleted:\n")
        d = ""
        for t in removed:
            for el in t:
                d = d + str(el[2]) + " "
            d = d + "\n"

        self.app.results_text.insert(INSERT, d + "\n")
            
        # disable the text area
        self.app.results_text.config(state = DISABLED)
