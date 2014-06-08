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

general_sparql_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s ?p ?o
WHERE { ?s ?p ?o }"""

class IndicationHandler:

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


# main class
class Application(Frame):

    ########################################################
    ##
    ## JOIN or LEAVE
    ##
    ########################################################
    
    # Join/Leave method
    def joinleave(self):

        if self.joined:

            print "Leave request:",

            # disable all the buttons
            self.insert_button.config(state = DISABLED)
            self.remove_button.config(state = DISABLED)
            self.rdf_query_button.config(state = DISABLED)
            self.rdf_subscription_button.config(state = DISABLED)
            self.rdf_unsubscription_button.config(state = DISABLED)
            self.sparql_query_button.config(state = DISABLED)
            self.sparql_subscription_button.config(state = DISABLED)
            self.sparql_unsubscription_button.config(state = DISABLED)
            self.joinleave_button["text"] = "Join"
            self.joined = False

            try: 
                self.kp.leave_sib()
                
                # notify the join success
                self.notification_label["text"] = 'Leaved succesfully!'
                print "OK!"

            except:
                
                # notify the failure
                self.notification_label["text"] = 'Error while leaving the SIB'
                print colored("failed!", "red", attrs=["bold"])

                # re-enable the connection fields
                self.sib_address_entry.config(state = NORMAL)
                self.sib_port_entry.config(state = NORMAL)

            # forget about the joined sib
            self.kp = None

            # re-enable connection fields
            self.sib_address_entry.config(state = NORMAL)
            self.sib_port_entry.config(state = NORMAL)

            # disable rdf/sparql entries
            self.sparql_text.config(state = DISABLED)
            self.subject_entry.config(state = DISABLED)
            self.predicate_entry.config(state = DISABLED)
            self.object_entry.config(state = DISABLED)

        else:

            print "Join request:",
            
            # get sib ip and port
            sib_ip = self.sib_address_entry.get()
            sib_port = int(self.sib_port_entry.get())
            
            # create a SIBLib instance
            try:
                self.kp = SibLib(sib_ip, sib_port)
                self.joined = True
                self.joinleave_button["text"] = "Leave"

                # enable all the buttons
                self.insert_button.config(state = NORMAL)
                self.remove_button.config(state = NORMAL)
                self.rdf_query_button.config(state = NORMAL)
                self.rdf_subscription_button.config(state = NORMAL)
                self.rdf_unsubscription_button.config(state = NORMAL)
                self.sparql_query_button.config(state = NORMAL)
                self.sparql_subscription_button.config(state = NORMAL)
                self.sparql_unsubscription_button.config(state = NORMAL)

                # disable connection fields
                self.sib_address_entry.config(state = DISABLED)
                self.sib_port_entry.config(state = DISABLED)

                # enable rdf/sparql entries
                self.sparql_text.config(state = NORMAL)
                self.subject_entry.config(state = NORMAL)
                self.predicate_entry.config(state = NORMAL)
                self.object_entry.config(state = NORMAL)                
                self.sparql_text.delete(1.0, END)
                self.sparql_text.insert(INSERT, general_sparql_query)

                # notify the join success
                self.notification_label["text"] = 'Joined succesfully!'
                print "OK!"

            except:
                self.notification_label["text"] = 'Error while joining the SIB'
                print colored("failed!", "red", attrs=["bold"])


    ########################################################
    ##
    ## RDF QUERY
    ##
    ########################################################

    def rdf_query(self):
        
        # get the subject
        s = self.subject_entry.get()
        subj = None if s == "*" else URI(s)

        # get the predicate
        p = self.predicate_entry.get()
        pred = None if p == "*" else URI(p)

        # get the object
        o = self.object_entry.get()
        obj = None if o == "*" else URI(o)

        # build the Triple object
        t = Triple(subj, pred, obj)
        print "RDF Query to " + str(t) + ":",

        # query
        try:
            res = self.kp.execute_rdf_query(t)
            s = ""
            for t in res:
                s = s + str(t[0]) + " " + str(t[1]) + " " + str(t[2]) + "\n"
            
            # update the result field
            self.results_text.config(state = NORMAL)
            self.results_text.delete(1.0, END)
            self.results_text.insert(INSERT, s)
            self.results_text.config(state = DISABLED)

            # notify the success
            self.notification_label["text"] = 'RDF query succesful!'
            print "OK!"

        except:

            # notify the failure
            self.notification_label["text"] = 'Error while querying the SIB'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()


    ########################################################
    ##
    ## INSERT
    ##
    ########################################################

    def insert(self):
        
        # get the subject
        subj = URI(self.subject_entry.get())

        # get the predicate
        pred = URI(self.predicate_entry.get())

        # get the object
        obj = URI(self.object_entry.get())

        # build the triple
        t = Triple(subj, pred, obj)

        # notification
        print "Insert request for triple " + str(t) + ":",

        try:
            # insert the triple
            self.kp.insert(t)

            # notification
            self.notification_label["text"] = 'RDF insert succesful'
            print "OK!"

        except:

            # failure notification
            self.notification_label["text"] = 'Error inserting a triple into the SIB'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()


    ########################################################
    ##
    ## REMOVE
    ##
    ########################################################

    def remove(self):

        # get the subject
        s = self.subject_entry.get()
        subj = None if s == "*" else URI(s)

        # get the predicate
        p = self.predicate_entry.get()
        pred = None if p == "*" else URI(p)

        # get the object
        o = self.object_entry.get()
        obj = None if o == "*" else URI(o)

        # build the triple
        t = Triple(subj, pred, obj)

        # notification
        print "Remove request for triple " + str(t) + ":",

        try:
            # remove the triple
            self.kp.remove()

            # notification
            self.notification_label["text"] = 'RDF remove succesful'
            print "OK!"

        except:
            
            # failure notification
            self.notification_label["text"] = 'Error removing a triple from the SIB'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()                  


    ########################################################
    ##
    ## RDF SUBSCRIPTION
    ##
    ########################################################

    def rdf_subscription(self):

        # get the subject
        s = self.subject_entry.get()
        subj = None if s == "*" else URI(s)

        # get the predicate
        p = self.predicate_entry.get()
        pred = None if p == "*" else URI(p)

        # get the object
        o = self.object_entry.get()
        obj = None if o == "*" else URI(o)

        # build the triple
        t = Triple(subj, pred, obj)

        # notification
        print "RDF Subscription to " + str(t) + ":",
        
        # subscribe
        try:
            s = self.kp.create_rdf_subscription(t, IndicationHandler(self))

            # add the subscription to the combobox
            self.rdf_active_subs_combobox.config(state = NORMAL)
            m = self.rdf_active_subs_combobox['menu']
            m.add_command(label = str(s.sub_id), command=Tkinter._setit(self.rdf_active_subs_combobox_var, str(s.sub_id)))
            self.rdf_subscriptions[s.sub_id] = s
            self.rdf_active_subs_label["text"] = "RDF Active subs (" + str(len(self.rdf_subscriptions)) + ")"

            # notification
            print "OK!"
            self.notification_label["text"] = 'Subscribe request successful!'

        except:
            
            # notify the failure
            self.notification_label["text"] = 'Error during Subscription'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()
            

    ########################################################
    ##
    ## RDF UNSUBSCRIBE
    ##
    ########################################################
    
    def rdf_unsubscription(self):
        
        # get the subscription id      
        sub_id = self.rdf_active_subs_combobox_var.get()

        # notification
        print "Unsubscribe request for " + str(sub_id) + ":",

        try:
            
            # unsubscribe
            self.kp.unsubscribe(self.rdf_subscriptions[sub_id])

            # remove the subscription from the dictionary
            del self.rdf_subscriptions[sub_id]

            # remove the subscription from the combobox
            self.rdf_active_subs_combobox.config(state = NORMAL)
            m = self.rdf_active_subs_combobox['menu']
            m.delete(0, 'end')
            for sub in self.rdf_subscriptions.keys():
                m.add_command(label = str(sub), command=Tkinter._setit(self.rdf_active_subs_combobox_var, str(sub)))            
            self.rdf_active_subs_combobox_var.set('')
            self.rdf_active_subs_label["text"] = "RDF Active subs (" + str(len(self.rdf_subscriptions)) + ")"

            # notification
            print "OK"
            self.notification_label["text"] = 'Unsubscribe request successful!'

        except:

            # notification of the failure
            self.notification_label["text"] = 'Unsubscribe request failed!'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()


    ########################################################
    ##
    ## SPARQL QUERY
    ##
    ########################################################

    def sparql_query(self):
        
        # get the sparql query
        q = self.sparql_text.get(1.0, END)
        cmd = q.split()[0]
        
        # execute the query
        try:
            res = self.kp.execute_sparql_query(q)

            if cmd == "SELECT":
                s = ""
                for t in res:
                    s = s + str(t[0][2]) + " " + str(t[1][2]) + " " + str(t[2][2]) + "\n"
            
                # update the result field
                self.results_text.config(state = NORMAL)
                self.results_text.delete(1.0, END)
                self.results_text.insert(INSERT, s)
                self.results_text.config(state = DISABLED)

            # update the notification area
            self.notification_label["text"] = 'SPARQL ' + cmd.lower() + ' succesful'

        except:
            self.notification_label["text"] = 'Error with SPARQL ' + cmd.lower()
            print sys.exc_info()
            print colored("Error> ", "red", attrs=["bold"]) + " SPARQL " + cmd.lower() + " failed"
        

    def createWidgets(self):

        # Connection frame
        self.connection_frame = Frame(self)
        self.connection_frame.pack() #padx = 10, pady = 10)

        # Sib address Label
        self.sib_address = Label(self.connection_frame, text="Sib address:")
        self.sib_address.pack(side = LEFT)

        # Sib address Entry
        self.sib_address_entry = Entry(self.connection_frame)
        self.sib_address_entry.pack(side = LEFT)
        self.sib_address_entry.insert(0, "127.0.0.1")

        # Sib port label
        self.sib_port = Label(self.connection_frame, text="Sib port:")
        self.sib_port.pack(side = LEFT)

        # Sib port Entry
        self.sib_port_entry = Entry(self.connection_frame)
        self.sib_port_entry.pack(side = LEFT)
        self.sib_port_entry.insert(0, "10010")

        # Join/Leave Button
        self.joinleave_button = Button(self.connection_frame)
        self.joinleave_button["text"] = "Join"
        self.joinleave_button["command"] =  self.joinleave
        self.joinleave_button.config( state = NORMAL )
        self.joinleave_button.pack( side = LEFT)        

        # Results frame
        self.results_frame = Frame(self)
        self.results_frame.pack(padx = 10, pady = 10)
        
        # Result Text
        self.results_text = Text(self.results_frame)
        self.results_text.pack()
        self.results_text.config(state = DISABLED)
        self.results_text.config(height = 8)

        # RDF Frame
        self.rdf_frame = Frame(self)
        self.rdf_frame.pack(padx = 10, pady = 10)
        
        # RDF Label
        self.rdf_label = Label(self.rdf_frame, text="RDF Interaction")
        self.rdf_label.pack(side = TOP, padx = 3, pady = 3)
        
        # Triple frame
        self.triple_frame = Frame(self.rdf_frame)
        self.triple_frame.pack(padx = 10, pady = 10)

        # Subject Label
        self.subject_label = Label(self.triple_frame, text="Subject")
        self.subject_label.pack(side = LEFT)

        # Subject Entry
        self.subject_entry = Entry(self.triple_frame)
        self.subject_entry.pack(side = LEFT)
        self.subject_entry.insert(0, "http://ns#")
        self.subject_entry.config(state = DISABLED)

        # Predicate Label
        self.predicate_label = Label(self.triple_frame, text="Predicate")
        self.predicate_label.pack(side = LEFT)

        # Predicate Entry
        self.predicate_entry = Entry(self.triple_frame)
        self.predicate_entry.pack(side = LEFT)
        self.predicate_entry.insert(0, "http://ns#")
        self.predicate_entry.config(state = DISABLED)

        # Object Label
        self.object_label = Label(self.triple_frame, text="Object")
        self.object_label.pack(side = LEFT)

        # Object Entry
        self.object_entry = Entry(self.triple_frame)
        self.object_entry.pack(side = LEFT)
        self.object_entry.insert(0, "http://ns#")
        self.object_entry.config(state = DISABLED)

        # Actions frame
        self.rdf_actions_frame = Frame(self.rdf_frame)
        self.rdf_actions_frame.pack() #padx = 10, pady = 10)

        # Insert button
        self.insert_button = Button(self.rdf_actions_frame)
        self.insert_button["text"] = "Insert"
        self.insert_button["command"] =  self.insert
        self.insert_button.config( state = DISABLED )
        self.insert_button.pack( side = LEFT)        
        
        # Remove button
        self.remove_button = Button(self.rdf_actions_frame)
        self.remove_button["text"] = "Remove"
        self.remove_button["command"] =  self.remove
        self.remove_button.config( state = DISABLED )
        self.remove_button.pack( side = LEFT)

        # RDF query button
        self.rdf_query_button = Button(self.rdf_actions_frame)
        self.rdf_query_button["text"] = "RDF Query"
        self.rdf_query_button["command"] =  self.rdf_query
        self.rdf_query_button.config( state = DISABLED )
        self.rdf_query_button.pack( side = LEFT)

        # RDF query button
        self.rdf_subscription_button = Button(self.rdf_actions_frame)
        self.rdf_subscription_button["text"] = "RDF Subscription"
        self.rdf_subscription_button["command"] =  self.rdf_subscription
        self.rdf_subscription_button.config( state = DISABLED )
        self.rdf_subscription_button.pack( side = LEFT)

        # RDF active_subs
        self.rdf_active_subs_frame = Frame(self.rdf_frame)
        self.rdf_active_subs_frame.pack(padx = 3, pady = 3)

        # Rdf_Active_Subs_Label Label
        self.rdf_active_subs_label = Label(self.rdf_active_subs_frame, text="RDF Active subs (0)")
        self.rdf_active_subs_label.pack(side = LEFT)

        # RDF active subscriptions combobox
        self.rdf_active_subs_combobox_var = StringVar(self.rdf_active_subs_frame)
        self.rdf_active_subs_combobox_items = ()
        self.rdf_active_subs_combobox = OptionMenu(self.rdf_active_subs_frame, self.rdf_active_subs_combobox_var, self.rdf_active_subs_combobox_items)
        self.rdf_active_subs_combobox.config( state = DISABLED, width = 20 )
        self.rdf_active_subs_combobox.pack(side = LEFT)
        # self.rdf_active_subs_combobox_var.set('Select a subscription')

        # Rdf_Unsubscription button
        self.rdf_unsubscription_button = Button(self.rdf_active_subs_frame)
        self.rdf_unsubscription_button["text"] = "RDF Unsubscription"
        self.rdf_unsubscription_button["command"] =  self.rdf_unsubscription
        self.rdf_unsubscription_button.config( state = DISABLED )
        self.rdf_unsubscription_button.pack( side = LEFT)

        # SPARQL interaction
        self.sparql_frame = Frame(self)
        self.sparql_frame.pack(padx = 3, pady = 3)
        
        # SPARQL Label
        self.sparql_label = Label(self.sparql_frame, text="SPARQL Interaction")
        self.sparql_label.pack(side = TOP, padx = 10, pady = 10)

        # Sparql Text
        self.sparql_text = Text(self.sparql_frame)
        self.sparql_text.pack(side = TOP, padx = 10, pady = 10)
        self.sparql_text.config(height = 8, state = DISABLED)
        self.sparql_text.insert(INSERT, general_sparql_query)

        # Sparql Actions Entry
        self.sparql_actions_frame = Frame(self.sparql_frame)
        self.sparql_actions_frame.pack() #padx = 10, pady = 10)

        # Sparql_Query button
        self.sparql_query_button = Button(self.sparql_actions_frame)
        self.sparql_query_button["text"] = "SPARQL Query"
        self.sparql_query_button["command"] =  self.sparql_query
        self.sparql_query_button.config( state = DISABLED )
        self.sparql_query_button.pack( side = LEFT)

        # Sparql_Subscription button
        self.sparql_subscription_button = Button(self.sparql_actions_frame)
        self.sparql_subscription_button["text"] = "SPARQL Subscription"
        self.sparql_subscription_button["command"] =  None
        self.sparql_subscription_button.config( state = DISABLED )
        self.sparql_subscription_button.pack( side = LEFT)

        # Sparql_Unsubscription button
        self.sparql_unsubscription_button = Button(self.sparql_actions_frame)
        self.sparql_unsubscription_button["text"] = "SPARQL Unsubscription"
        self.sparql_unsubscription_button["command"] =  None
        self.sparql_unsubscription_button.config( state = DISABLED )
        self.sparql_unsubscription_button.pack( side = LEFT)        

        # Notification frame
        self.notification_frame = Frame(self)
        self.notification_frame.pack(padx = 10, pady = 10)

        # Notification Label
        self.notification_label = Label(self.sparql_frame, text="Waiting for commands...")
        self.notification_label.pack(side = TOP, padx = 10, pady = 10)

    # Constructor
    def __init__(self, master=None):
        self.s = Style()
        self.s.theme_use('clam')
        Frame.__init__(self, master)
        self.pack()
        self.createWidgets()
        self.joined = False
        self.kp = None
        
        # active subscriptions
        self.rdf_subscriptions = {}
        self.sparql_subscriptions = {}
        
    # Destroyer!
    def destroy(self):
        print "Bye!"

# main loop
root = Tk()
app = Application(master=root)
app.mainloop()

