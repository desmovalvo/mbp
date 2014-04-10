#!/usr/bin/python

# requirements
from lib import SIBLib
from smart_m3.m3_kp import *

# namespaces
ns = "http://smartm3.org/ontology.owl#"
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
owl = "http://www.w3.org/2002/07/owl#"
xsd = "http://www.w3.org/2001/XMLSchema#"
rdfs = "http://www.w3.org/2000/01/rdf-schema#"

# sib A
a = SIBLib.SibLib("127.0.0.1", 10020)
a.join_sib()

print 'Inserimento triple nella SIB A'
a.insert(Triple(URI(ns + "Alice"), URI(rdf + "type"), URI(ns + "Person")))
a.insert(Triple(URI(ns + "Roger"), URI(rdf + "type"), URI(ns + "Person")))
a.insert(Triple(URI(ns + "Bob"), URI(rdf + "type"), URI(ns + "Person")))
a.insert(Triple(URI(ns + "Yaris"), URI(rdf + "type"), URI(ns + "Car")))
a.insert(Triple(URI(ns + "Giulietta"), URI(rdf + "type"), URI(ns + "Car")))
a.insert(Triple(URI(ns + "M3"), URI(rdf + "type"), URI(ns + "Car")))
a.insert(Triple(URI(ns + "Toyota"), URI(rdf + "type"), URI(ns + "Brand")))
a.insert(Triple(URI(ns + "AlfaRomeo"), URI(rdf + "type"), URI(ns + "Brand")))
a.insert(Triple(URI(ns + "BMW"), URI(rdf + "type"), URI(ns + "Brand")))
a.insert(Triple(URI(ns + "Yaris"), URI(ns + "hasBrand"), URI(ns + "Toyota")))
a.insert(Triple(URI(ns + "Giulietta"), URI(ns + "hasBrand"), URI(ns + "AlfaRomeo")))
a.insert(Triple(URI(ns + "M3"), URI(ns + "hasBrand"), URI(ns + "BMW")))
a.insert(Triple(URI(ns + "Alice"), URI(ns + "owns"), URI(ns + "Yaris")))
a.insert(Triple(URI(ns + "Roger"), URI(ns + "owns"), URI(ns + "Giulietta")))
a.insert(Triple(URI(ns + "Bob"), URI(ns + "owns"), URI(ns + "M3")))

# sib B
b = SIBLib.SibLib("127.0.0.1", 10030)
b.join_sib()

print 'Inserimento triple nella SIB B'
b.insert(Triple(URI(ns + "Alice"), URI(rdf + "type"), URI(ns + "Person")))
b.insert(Triple(URI(ns + "Roger"), URI(rdf + "type"), URI(ns + "Person")))
b.insert(Triple(URI(ns + "Bob"), URI(rdf + "type"), URI(ns + "Person")))
b.insert(Triple(URI(ns + "Tom"), URI(rdf + "type"), URI(ns + "Person")))
b.insert(Triple(URI(ns + "Luise"), URI(rdf + "type"), URI(ns + "Person")))
b.insert(Triple(URI(ns + "Lincoln"), URI(rdf + "type"), URI(ns + "Person")))
b.insert(Triple(URI(ns + "Alice"), URI(ns + "is"), Literal(25)))
b.insert(Triple(URI(ns + "Roger"), URI(ns + "is"), Literal(26)))
b.insert(Triple(URI(ns + "Bob"), URI(ns + "is"), Literal(27)))
b.insert(Triple(URI(ns + "Tom"), URI(ns + "is"), Literal(28)))
b.insert(Triple(URI(ns + "Luise"), URI(ns + "is"), Literal(29)))
b.insert(Triple(URI(ns + "Lincoln"), URI(ns + "is"), Literal(30)))
