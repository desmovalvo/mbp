#!/usr/bin/python

# requirements
from lib import SIBLib
from smart_m3.m3_kp import *
import sys

# namespaces
ns = "http://smartM3Lab/Ontology.owl#"
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
owl = "http://www.w3.org/2002/07/owl#"
xsd = "http://www.w3.org/2001/XMLSchema#"
rdfs = "http://www.w3.org/2000/01/rdf-schema#"

# sib A
a = SIBLib.SibLib(sys.argv[1], int(sys.argv[2]))
a.join_sib()

if int(sys.argv[3]) == 0:

    print 'Inserimento triple del test set 0'
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

elif int(sys.argv[3]) == 1:

    print 'Inserimento triple del test set 1'
    a.insert(Triple(URI(ns + "Alice"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Roger"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Bob"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Tom"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Luise"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Lincoln"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Alice"), URI(ns + "is"), Literal(25)))
    a.insert(Triple(URI(ns + "Roger"), URI(ns + "is"), Literal(26)))
    a.insert(Triple(URI(ns + "Bob"), URI(ns + "is"), Literal(27)))
    a.insert(Triple(URI(ns + "Tom"), URI(ns + "is"), Literal(28)))
    a.insert(Triple(URI(ns + "Luise"), URI(ns + "is"), Literal(29)))
    a.insert(Triple(URI(ns + "Lincoln"), URI(ns + "is"), Literal(30)))

elif int(sys.argv[3]) == 2:

    print 'Inserimento triple del test set 2'
    a.insert(Triple(URI(ns + "Frank"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Mark"), URI(rdf + "type"), URI(ns + "Person")))
    a.insert(Triple(URI(ns + "Frank"), URI(ns + "is"), Literal(15)))
    a.insert(Triple(URI(ns + "Mark"), URI(ns + "is"), Literal(36)))
    a.insert(Triple(URI(ns + "Frank"), URI(ns + "hasBrother"), URI(ns + "Mark")))
    a.insert(Triple(URI(ns + "Frank"), URI(ns + "hasBrother"), URI(ns + "Tom")))
