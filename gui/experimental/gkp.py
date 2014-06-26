#!/usr/bin/python

# requirements
import json
from Tkinter import *
import Tkinter
from PIL import ImageTk, Image
import tkFont
from tkMessageBox import showinfo
from ttk import *
from SIBLib import *
from termcolor import *
import sys
import traceback
from RDFIndicationHandler import *
from SPARQLIndicationHandler import *

# font
TITLE_FONT = ("Helvetica", 18, "bold")
SUBTITLE_FONT = ("Helvetica", 14 )

PREFIXES = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <""" + ns + ">"


general_sparql_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ns: <http://smartM3Lab/Ontology.owl#>
SELECT ?s ?p ?o
WHERE { ?s ?p ?o }"""


# main class
class Application(Tkinter.Tk):

    # Constructor
    def __init__(self, *args, **kwargs): 

        # calling the old constructor
        Tkinter.Tk.__init__(self, *args, **kwargs)

        # creating and placing a main frame
        self.container = Tkinter.Frame(self)
        self.container.pack(side="top", fill="both", expand=True)
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)

        # frames
        self.frames = {}
        for F in (StartPage, SibInteraction, SibSearch, ModifyMultiSIB):
            frame = F(self.container, self)
            self.frames[F] = frame
            frame.grid(row=0, column=0, sticky="nsew")

    # This function puts on top the frame of the given class
    def show_frame(self, c, sib_addr = None, sib_port = None, msib_id = None, profile = None):
        frame = self.frames[c]
        frame.tkraise()

        if str(c) == "__main__.SibInteraction":
            print "Sib interaction"

            # clear the results field
            frame.results_text.config(state = NORMAL)
            frame.results_text.delete(1.0, END)
            frame.results_text.config(state = DISABLED)

            # if already joined, leave
            if frame.joined:
                frame.joinleave()

            # reset the notification area
            frame.notification_label["text"] = "Waiting for commands..."

            # set the connection fields
            frame.set_connection_fields(sib_addr, sib_port)
            

        if str(c) == "__main__.ModifyMultiSIB":
            frame.show_multi_sib(msib_id)

        if str(c) == "__main__.SibSearch":
            frame.profile = profile
            frame.refresh()
            
    # Destroyer!
    def destroy(self):
        print "Bye!"
        sys.exit()


# This is the main interface
class StartPage(Tkinter.Frame):
    def __init__(self, parent, controller):
        
        self.s = Style()
        self.s.theme_use('clam')
        
        Tkinter.Frame.__init__(self, parent) 

        self.s = Style()
        self.s.theme_use('clam')

        label = Tkinter.Label(self, text="Welcome to the MultiSIB client", font=TITLE_FONT)
        label.pack(side="top", fill="x", pady=10)

        # buttons frame
        buttons_frame = Frame(self)
        buttons_frame.pack()
        
        # entry box
        e = Entry(buttons_frame)
        e.pack(side = LEFT)
        e.delete(0, END)
        
        # discoveryWhere button
        discovery_where_button = Button(buttons_frame, text="Discovery Where")
        discovery_where_button["command"] = lambda: controller.show_frame(SibSearch, None, None, None, e.get())
        discovery_where_button.pack(side = LEFT)

        # sibsearch button
        sibsearch_button = Button(buttons_frame, text="Discovery All")
        sibsearch_button["command"] = lambda: controller.show_frame(SibSearch)
        sibsearch_button.pack(side = LEFT)

        # sibinteraction button
        sibinteraction_button = Button(buttons_frame, text="Connect to a known SIB")
        sibinteraction_button["command"] = lambda: controller.show_frame(SibInteraction)
        sibinteraction_button.pack(side = LEFT)

        # quit button
        quit_button = Button(buttons_frame, text="Quit")
        quit_button["command"] = sys.exit
        quit_button.pack(side = LEFT)


########################################################
##
## Modify Multi SIB Class
##
########################################################

class ModifyMultiSIB(Tkinter.Frame):

    ########################################################
    ##
    ## show multi sib
    ##
    ########################################################
    def show_multi_sib(self, msib_id):
        self.multi_sib_id = str(msib_id)
        # components of the multi sib
        self.msib = []
        # other sibs
        self.other_sibs = []
        
        self.multisib_label["text"] = "Multi SIB: " + self.multi_sib_id 

        # build the command to the manager to get the information about the multi sib and the other sibs
        cmd = {"command":"MultiSIBInfo", "multi_sib_id":str(self.multi_sib_id)}
        jcmd = json.dumps(cmd)
                
        # try to connect to the manager and to send the message
        try:
            print "contatto il manager"
            print self.manager_ip
            print self.manager_port

            # connection to the manager
            manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            manager_socket.connect((self.manager_ip, self.manager_port))        
            manager_socket.send(jcmd)

            # wait for a reply
            while 1:
                try:
                    confirm_msg = manager_socket.recv(4096)
                    break
                except:
                    print colored("request_handler> ", "red", attrs=["bold"]) + 'Request to the manager failed'
                    manager_socket.close()
                    self.notification_label["text"] = 'Request failed!'
                                        

            print 'ShowMultiSib: ' + str(confirm_msg)

            # closing socket
            manager_socket.close()            

            # check the confirm content
            c = json.loads(confirm_msg)
            print "c = " + str(c)
            if c["return"] == "fail":
                self.notification_label["text"] = 'Request failed!'
                pass
            else:
                self.multisib_listbox.delete(0, END)
                self.sibs_listbox.delete(0, END)
                info = c["multisib_info"]
                print info["components"]
                print info["others"]

                for sib in info["components"]:
                    self.msib.append(str(sib))
                    owner = info["components"][sib]
                    self.multisib_listbox.insert(END, str(sib) + " Owner: " + str(owner) )
                    
                for sib in info["others"]:
                    if sib not in self.msib:
                        self.other_sibs.append(str(sib))
                        owner = info["others"][sib]
                        self.sibs_listbox.insert(END, str(sib) + " Owner: " + str(owner))


        except:
            pass


                
    ########################################################
    ##
    ## REMOVE
    ##
    ########################################################
    def remove(self):

        selected = self.multisib_listbox.curselection()
        if (len(selected) == 0):
            self.notification_label["text"] = "Select one or more sibs to remove from the multi sib!"
            return

        ss = []
        for s in selected:
            ss.append(self.msib[int(s[0])])
        print ss
        # build RemoveSIBfromVMSIB message to send to the manager
        msg = json.dumps({"command":"RemoveSIBfromVMSIB", "sib_list":ss, "vmsib_id" : str(self.multi_sib_id) })


        # send a message to the vmsib
        try:
            print "contatto il manager"
            # connection to the vmsib
            manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            manager_socket.connect((self.manager_ip, self.manager_port))        
            manager_socket.send(msg)

            # wait for a reply
            while 1:
                try:
                    confirm_msg = manager_socket.recv(4096)
                    break
                except:
                    print colored("request_handler> ", "red", attrs=["bold"]) + 'Request to the manager failed'
                    manager_socket.close()
                    self.notification_label["text"] = 'Request failed!'
                    
                    

            print 'RemoveSIB: ' + str(confirm_msg)

            # closing socket
            manager_socket.close()            

            # check the confirm content
            c = json.loads(confirm_msg)
            if c["return"] == "fail":
                self.notification_label["text"] = 'Request failed!'
                pass
            else:
                self.notification_label["text"] = 'Sib removed!'
                self.controller.show_frame(SibSearch)                
        except:
            pass


    ########################################################
    ##
    ## ADD
    ##
    ########################################################
    def add(self):
        selected = self.sibs_listbox.curselection()
        if (len(selected) == 0):
            self.notification_label["text"] = "Select one or more sibs to add to the multi sib!"
            return

        ss = []
        for s in selected:
            ss.append(self.other_sibs[int(s[0])])
        print ss

        # build AddSIBtoVMSIB message to send to the manager
        msg = json.dumps({"command":"AddSIBtoVMSIB", "sib_list": ss, "vmsib_id" : str(self.multi_sib_id) })


        # send a message to the vmsib
        try:
            print "contatto il manager"
            # connection to the vmsib
            manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            manager_socket.connect((self.manager_ip, self.manager_port))        
            manager_socket.send(msg)

            # wait for a reply
            while 1:
                try:
                    confirm_msg = manager_socket.recv(4096)
                    break
                except:
                    print colored("request_handler> ", "red", attrs=["bold"]) + 'Request to the vmsib failed'
                    manager_socket.close()
                    self.notification_label["text"] = 'Request failed!'
                    
            print 'AddSIB: ' + str(confirm_msg)
            
            # closing socket
            manager_socket.close()            

            # check the confirm content
            c = json.loads(confirm_msg)
            if c["return"] == "fail":
                self.notification_label["text"] = 'Request failed!'
                pass
            else:
                self.notification_label["text"] = 'Sib added!'
                self.controller.show_frame(SibSearch)                
        except:
            pass


    ########################################################
    ##
    ## BACK
    ##
    ########################################################

    def back(self):
        
        # changing frame
        self.controller.show_frame(SibSearch)
        


    ########################################################
    ##
    ## INIT
    ##
    ########################################################

    def __init__(self, parent, controller):

        self.s = Style()
        self.s.theme_use('clam')

        # Frame
        Tkinter.Frame.__init__(self, parent) 

        # attributes
        self.kp = None
        self.controller = controller

        # Open the configuration file to read ip and port of the manager server
        conf_file = open("gkp.conf", "r")
        conf = json.load(conf_file)

        # Manager parameters
        self.manager_ip = conf["manager"]["ip"]
        self.manager_port = conf["manager"]["port"]

        # Closing the configuration file
        conf_file.close()

        # Font
        section_font = tkFont.Font(family="Helvetica", size=14, weight="bold")

        # main frame
        self.main_frame = Frame(self)
        self.main_frame.pack()

        # multisib Listbox frame
        self.multisib_listbox_frame = Frame(self.main_frame)
        self.multisib_listbox_frame.grid(row = 0, column = 0, sticky = S + N)
        
        # Label multi sib
        self.multisib_label = Label(self.multisib_listbox_frame, text="Multi SIB ", font=SUBTITLE_FONT)
        self.multisib_label.grid(row = 0, column = 0, pady = (30, 5))

        # multisib Scrollbar
        self.multisib_scrollbar = Scrollbar(self.multisib_listbox_frame)
        self.multisib_scrollbar.grid(row = 1, column = 1, sticky = S + N)

        # multisib ListBox
        self.multisib_listbox = Listbox(self.multisib_listbox_frame, yscrollcommand=self.multisib_scrollbar.set)
        self.multisib_listbox.config(width = 60, selectmode = MULTIPLE, height = 30)
        self.multisib_listbox.grid(row = 1, column = 0)

        # buttons frame
        self.buttons_frame = Frame(self.main_frame)
        self.buttons_frame.grid(row = 0, column = 3, sticky = S + N)

        # add button
        self.add_button = Button(self.buttons_frame)
        self.add_button["text"] = "Add"
        self.add_button["command"] =  self.add
        self.add_button.grid(row = 1, column = 0, pady = (140,10), padx = 20)
        
        # Remove button
        self.remove_button = Button(self.buttons_frame)
        self.remove_button["text"] = "Remove"
        self.remove_button["command"] =  self.remove
        self.remove_button.grid(row = 2, column = 0, pady = 0, padx = 20)

        # sibs Listbox frame
        self.sibs_listbox_frame = Frame(self.main_frame)
        self.sibs_listbox_frame.grid(row = 0, column = 5, sticky = S + N)

        # Label sibs
        self.sibs_label = Label(self.sibs_listbox_frame, text="Other SIBs ", font=SUBTITLE_FONT)
        self.sibs_label.grid(row = 0, column = 0, pady = (30, 5))

        # sibs Scrollbar
        self.sibs_scrollbar = Scrollbar(self.sibs_listbox_frame)
        self.sibs_scrollbar.grid(row = 1, column = 6, sticky = S + N)

        # sibs ListBox
        self.sibs_listbox = Listbox(self.sibs_listbox_frame, yscrollcommand=self.sibs_scrollbar.set)
        self.sibs_listbox.config(width = 60, selectmode = MULTIPLE, height = 30)
        self.sibs_listbox.grid(row = 1, column = 0)

        # End buttons frame
        self.end_buttons_frame = Frame(self.main_frame)
        self.end_buttons_frame.grid(row = 2, column = 0, columnspan = 8, sticky = E)
        
        # Quit button
        self.quit_button = Button(self.end_buttons_frame, text = "Quit")
        self.quit_button.pack(side = RIGHT)
        self.quit_button["command"] = sys.exit

        # Back button
        self.back_button = Button(self.end_buttons_frame, text = "Back")
        self.back_button.pack(side = RIGHT)
        self.back_button["command"] = self.back
        

        # Notification frame
        self.notification_frame = LabelFrame(self.main_frame)
        self.notification_frame.grid(row = 1, column = 0, columnspan = 8, sticky = E+W)

        # Notification Label
        self.notification_label = Label(self.notification_frame, text="Waiting for commands...")
        self.notification_label.pack(side = BOTTOM, padx = 10, pady = 2)



########################################################
##
## SibInteraction Class
##
########################################################

class SibInteraction(Tkinter.Frame):

    def set_connection_fields(self, sib_ip = None, sib_port = None):

        if sib_port and sib_ip:
            self.sib_address_entry.delete(0, END)
            self.sib_port_entry.delete(0, END)
            self.sib_address_entry.insert(0, sib_ip)
            self.sib_port_entry.insert(0, sib_port)


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
            self.update_button.config(state = DISABLED)
            self.rdf_query_button.config(state = DISABLED)
            self.rdf_query_all_button.config(state = DISABLED)
            self.rdf_subscription_button.config(state = DISABLED)
            self.rdf_unsubscription_button.config(state = DISABLED)
            self.sparql_query_button.config(state = DISABLED)
            self.sparql_query_all_button.config(state = DISABLED)
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
            self.subject_insert_entry.config(state = DISABLED)
            self.predicate_insert_entry.config(state = DISABLED)
            self.object_insert_entry.config(state = DISABLED)
            self.subject_remove_entry.config(state = DISABLED)
            self.predicate_remove_entry.config(state = DISABLED)
            self.object_remove_entry.config(state = DISABLED)

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
                self.update_button.config(state = NORMAL)
                self.rdf_query_button.config(state = NORMAL)
                self.rdf_query_all_button.config(state = NORMAL)
                self.rdf_subscription_button.config(state = NORMAL)
                self.rdf_unsubscription_button.config(state = NORMAL)
                self.sparql_query_button.config(state = NORMAL)
                self.sparql_query_all_button.config(state = NORMAL)
                self.sparql_subscription_button.config(state = NORMAL)
                self.sparql_unsubscription_button.config(state = NORMAL)

                # disable connection fields
                self.sib_address_entry.config(state = DISABLED)
                self.sib_port_entry.config(state = DISABLED)

                # enable rdf/sparql entries
                self.sparql_text.config(state = NORMAL)
                self.subject_insert_entry.config(state = NORMAL)
                self.predicate_insert_entry.config(state = NORMAL)
                self.object_insert_entry.config(state = NORMAL)                
                self.subject_remove_entry.config(state = NORMAL)
                self.predicate_remove_entry.config(state = NORMAL)
                self.object_remove_entry.config(state = NORMAL)                
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
        s = self.subject_insert_entry.get()
        subj = None if s == "*" else URI(s)

        # get the predicate
        p = self.predicate_insert_entry.get()
        pred = None if p == "*" else URI(p)

        # get the object
        o = self.object_insert_entry.get()
        obj = None if o == "*" else URI(o)

        # build the Triple object
        t = Triple(subj, pred, obj)
        print "RDF Query to " + str(t) + ":",

        # query
        try:
            res = self.kp.execute_rdf_query(t)
            s = ""
            s = str(len(res)) + " Triples\n"
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
    ## RDF QUERY ALL
    ##
    ########################################################

    def rdf_query_all(self):
        
        # build the Triple object
        t = Triple(None, None, None)
        print "RDF Query to " + str(t) + ":",

        # query
        try:
            res = self.kp.execute_rdf_query(t)
            s = ""
            s = str(len(res)) + " Triples\n"
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
        subj = URI(self.subject_insert_entry.get())

        # get the predicate
        pred = URI(self.predicate_insert_entry.get())

        # get the object
        obj = URI(self.object_insert_entry.get())

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
        s = self.subject_remove_entry.get()
        subj = None if s == "*" else URI(s)

        # get the predicate
        p = self.predicate_remove_entry.get()
        pred = None if p == "*" else URI(p)

        # get the object
        o = self.object_remove_entry.get()
        obj = None if o == "*" else URI(o)

        # build the triple
        t = Triple(subj, pred, obj)

        # notification
        print "Remove request for triple " + str(t) + ":",

        try:
            # remove the triple
            self.kp.remove(t)

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
    ## UPDATE
    ##
    ########################################################

    def update(self):

        # get the subject to insert
        new_subj = URI(self.subject_insert_entry.get())

        # get the predicate to insert
        new_pred = URI(self.predicate_insert_entry.get())

        # get the object to insert
        new_obj = URI(self.object_insert_entry.get())

        # build the triple
        new_t = Triple(new_subj, new_pred, new_obj)


        # get the subject
        s = self.subject_remove_entry.get()
        old_subj = None if s == "*" else URI(s)

        # get the predicate
        p = self.predicate_remove_entry.get()
        old_pred = None if p == "*" else URI(p)

        # get the object
        o = self.object_remove_entry.get()
        old_obj = None if o == "*" else URI(o)

        # build the triple
        old_t = Triple(old_subj, old_pred, old_obj)

        # notification
        print "Update request for triple " + str(old_t) + " to " + str(new_t) + ":",

        try:
            # remove the triple
            self.kp.update(new_t, old_t)

            # notification
            self.notification_label["text"] = 'RDF update succesful'
            print "OK!"

        except:
            
            # failure notification
            self.notification_label["text"] = 'Error update a triple into the SIB'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()                  


    ########################################################
    ##
    ## RDF SUBSCRIPTION
    ##
    ########################################################

    def rdf_subscription(self):

        # get the subject
        s = self.subject_insert_entry.get()
        subj = None if s == "*" else URI(s)

        # get the predicate
        p = self.predicate_insert_entry.get()
        pred = None if p == "*" else URI(p)

        # get the object
        o = self.object_insert_entry.get()
        obj = None if o == "*" else URI(o)

        # build the triple
        t = Triple(subj, pred, obj)

        # notification
        print "RDF Subscription to " + str(t) + ":",
        
        # subscribe
        try:
            s = self.kp.create_rdf_subscription(t, RDFIndicationHandler(self))

            # add the subscription to the combobox
            self.rdf_active_subs_combobox.config(state = NORMAL)
            m = self.rdf_active_subs_combobox['menu']
            m.add_command(label = str(s.sub_id), command=Tkinter._setit(self.rdf_active_subs_combobox_var, str(s.sub_id)))
            self.rdf_subscriptions[s.sub_id] = s
            self.rdf_active_subs_label["text"] = "RDF Active subs (" + str(len(self.rdf_subscriptions)) + ")"

            # initial results
            ir = self.kp.rdf_initial_results()

            # enable and clear the text area
            self.results_text.config(state = NORMAL)
            self.results_text.delete(1.0, END)
        
            # notify the initial results
            self.results_text.insert(INSERT, "Initial results:\n")
            i = ""
            for t in ir:
                i = i + str(t[0]) + " " + str(t[1]) + " " + str(t[2]) + "\n" 
            self.results_text.insert(INSERT, i + "\n")

            # disable the text area
            self.results_text.config(state = DISABLED)            

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

            # enable and clear the text area
            self.results_text.config(state = NORMAL)
            self.results_text.delete(1.0, END)

            # disable the text area
            self.results_text.config(state = DISABLED)            

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

        # notification
        print "SPARQL query: ",
        
        # execute the query
        try:
            res = self.kp.execute_sparql_query(q)
            
            if "SELECT" in q: # TODO - this is not always right...
#                s = str(res)
                s = ""
                s = str(len(res)) + " Triples\n"

                for t in res:
                    for el in t:
                        s = s + str(el[2]) + " "
                    s = s + "\n"
                
                # update the result field
                self.results_text.config(state = NORMAL)
                self.results_text.delete(1.0, END)
                self.results_text.insert(INSERT, s)
                self.results_text.config(state = DISABLED)

            # notification
            self.notification_label["text"] = 'SPARQL query succesful'
            print "OK!"

        except:

            # notify the failure
            self.notification_label["text"] = 'Error with SPARQL query'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()
            print traceback.print_exc()
        

    ########################################################
    ##
    ## SPARQL QUERY ALL
    ##
    ########################################################

    def sparql_query_all(self):
        
        # get the sparql query
        q = """SELECT ?s ?p ?o WHERE { ?s ?p ?o }"""
        print "SPARQL query: ",
        
        # execute the query
        try:
            res = self.kp.execute_sparql_query(q)

            s = ""
            s = str(len(res)) + " Triples\n"

            for t in res:
                s = s + str(t[0][2]) + " " + str(t[1][2]) + " " + str(t[2][2]) + "\n"

                            
            # update the result field
            self.results_text.config(state = NORMAL)
            self.results_text.delete(1.0, END)
            self.results_text.insert(INSERT, s)
            self.results_text.config(state = DISABLED)

            # notification
            self.notification_label["text"] = 'SPARQL query succesful'
            print "OK!"

        except:
            
            # notify the failure
            self.notification_label["text"] = 'Error with SPARQL query'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()


    ########################################################
    ##
    ## SPARQL SUBSCRIPTION
    ##
    ########################################################

    def sparql_subscription(self):

        # notification
        print "SPARQL Subscription:",
        
        # get the subscription text
        s = self.sparql_text.get(1.0, END)
        
        # subscribe
        try:
            s = self.kp.create_sparql_subscription(s, SPARQLIndicationHandler(self))

            # add the subscription to the combobox
            self.sparql_active_subs_combobox.config(state = NORMAL)
            m = self.sparql_active_subs_combobox['menu']
            m.add_command(label = str(s.sub_id), command=Tkinter._setit(self.sparql_active_subs_combobox_var, str(s.sub_id)))
            self.sparql_subscriptions[s.sub_id] = s
            self.sparql_active_subs_label["text"] = "SPARQL Active subs (" + str(len(self.sparql_subscriptions)) + ")"

            # initial results
            ir = self.kp.sparql_initial_results()

            # enable and clear the text area
            self.results_text.config(state = NORMAL)
            self.results_text.delete(1.0, END)
        
            # notify the initial results
            self.results_text.insert(INSERT, "Initial results:\n")
            s = ""
            s = str(len(ir)) + " Triples\n"
            
            for t in ir:
                for el in t:
                    s = s + str(el[2]) + " "
                s = s + "\n"

            self.results_text.insert(INSERT, s + "\n")

            # disable the text area
            self.results_text.config(state = DISABLED)            

            # notification
            print "OK!"
            self.notification_label["text"] = 'Subscribe request successful!'

        except ZeroDivisionError:
            
            # notify the failure
            self.notification_label["text"] = 'Error during Subscription'
            print colored("failed!", "red", attrs=["bold"])
            print sys.exc_info()
            

    ########################################################
    ##
    ## SPARQL UNSUBSCRIBE
    ##
    ########################################################
    
    def sparql_unsubscription(self):
        
        # get the subscription id      
        sub_id = self.sparql_active_subs_combobox_var.get()

        # notification
        print "Unsubscribe request for " + str(sub_id) + ":",

        try:
            
            # unsubscribe
            self.kp.unsubscribe(self.sparql_subscriptions[sub_id])

            # remove the subscription from the dictionary
            del self.sparql_subscriptions[sub_id]

            # remove the subscription from the combobox
            self.sparql_active_subs_combobox.config(state = NORMAL)
            m = self.sparql_active_subs_combobox['menu']
            m.delete(0, 'end')
            for sub in self.sparql_subscriptions.keys():
                m.add_command(label = str(sub), command=Tkinter._setit(self.sparql_active_subs_combobox_var, str(sub)))            
            self.sparql_active_subs_combobox_var.set('')
            self.sparql_active_subs_label["text"] = "SPARQL Active subs (" + str(len(self.sparql_subscriptions)) + ")"

            # enable and clear the text area
            self.results_text.config(state = NORMAL)
            self.results_text.delete(1.0, END)

            # disable the text area
            self.results_text.config(state = DISABLED)            

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
    ## BACK
    ##
    ########################################################

    def back(self):
        
        # closing rdf subscriptions
        for sub in self.rdf_subscriptions:
            try:
                print "Unsubscribe request:",
                self.kp.unsubscribe(self.rdf_subscriptions[sub])
                print "OK!"
            except:
                print colored("failed!", "red", attrs=["bold"])

        # closing sparql subscriptions
        for sub in self.sparql_subscriptions:
            try:
                print "Unsubscribe request:",
                self.kp.unsubscribe(self.sparql_subscriptions[sub])
                print "OK!"
            except:
                print colored("failed!", "red", attrs=["bold"])

        # leave
        if self.joined:
            try:
                print "Leave request:",
                self.kp.leave_sib()                
                print "OK!"
            except:
                print colored("failed!", "red", attrs=["bold"])

        # changing frame
        self.controller.show_frame(StartPage)


    ########################################################
    ##
    ## INIT
    ##
    ########################################################

    def __init__(self, parent, controller):

        self.s = Style()
        self.s.theme_use('clam')

        # Frame
        Tkinter.Frame.__init__(self, parent) 

        # attributes
        self.joined = False
        self.kp = None
        self.rdf_subscriptions = {}
        self.sparql_subscriptions = {}
        self.controller = controller

        # Font
        section_font = tkFont.Font(family="Helvetica", size=14, weight="bold")

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
        
        # Results Scrollbar
        self.results_scrollbar = Scrollbar(self.results_frame)
        self.results_scrollbar.pack(side = RIGHT, fill = Y)

        # Result Text
        self.results_text = Text(self.results_frame, yscrollcommand=self.results_scrollbar.set)
        self.results_text.pack()
        self.results_text.config(state = DISABLED)
        self.results_text.config(height = 19, width=200)
        self.results_scrollbar.config( command = self.results_text.yview )

        # RDFSPARQL frame
        self.rdfsparql_frame = Frame(self)
        self.rdfsparql_frame.pack(padx = 10, pady = 10)
        
        # RDF Label
        self.rdf_label = Label(self.rdfsparql_frame, text="RDF Interaction", font = section_font)
        self.rdf_label.grid( row = 0, sticky = NW, columnspan = 3, padx = 20, pady = 3)
        
        # Subject Label
        self.subject_label = Label(self.rdfsparql_frame, text="Insert subject")
        self.subject_label.grid(row = 1, column = 0, sticky = SW, padx = 20, pady = 3)

        # Subject Entry
        self.subject_insert_entry = Entry(self.rdfsparql_frame)
        self.subject_insert_entry.grid(row = 2, column = 0, sticky = W+N+E, padx = 20, pady = 3)
        self.subject_insert_entry.insert(0, "http://ns#")
        self.subject_insert_entry.config(state = DISABLED)

        # Predicate Label
        self.predicate_label = Label(self.rdfsparql_frame, text="Insert predicate")
        self.predicate_label.grid(row = 1, column = 1, sticky = SW, padx = 20, pady = 3)

        # Predicate Entry
        self.predicate_insert_entry = Entry(self.rdfsparql_frame)
        self.predicate_insert_entry.grid(row = 2, column = 1, sticky = W+E+N, padx = 20, pady = 3)
        self.predicate_insert_entry.insert(0, "http://ns#")
        self.predicate_insert_entry.config(state = DISABLED)

        # Object Label
        self.object_label = Label(self.rdfsparql_frame, text="Insert object")
        self.object_label.grid(row = 1, column = 2, sticky = SW, padx = 20, pady = 3)

        # Object Entry
        self.object_insert_entry = Entry(self.rdfsparql_frame)
        self.object_insert_entry.grid(row = 2, column = 2, sticky = W+E+N, padx = 20, pady = 3)
        self.object_insert_entry.insert(0, "http://ns#")
        self.object_insert_entry.config(state = DISABLED)

        ### Update fields

        # Subject Label
        self.subject_remove_label = Label(self.rdfsparql_frame, text="Remove subject")
        self.subject_remove_label.grid(row = 3, column = 0, sticky = SW, padx = 20, pady = 3)

        # Subject Entry
        self.subject_remove_entry = Entry(self.rdfsparql_frame)
        self.subject_remove_entry.grid(row = 4, column = 0, sticky = W+N+E, padx = 20, pady = 3)
        self.subject_remove_entry.insert(0, "http://ns#")
        self.subject_remove_entry.config(state = DISABLED)

        # Predicate Label
        self.predicate_remove_label = Label(self.rdfsparql_frame, text="Remove predicate")
        self.predicate_remove_label.grid(row = 3, column = 1, sticky = SW, padx = 20, pady = 3)

        # Predicate Entry
        self.predicate_remove_entry = Entry(self.rdfsparql_frame)
        self.predicate_remove_entry.grid(row = 4, column = 1, sticky = W+E+N, padx = 20, pady = 3)
        self.predicate_remove_entry.insert(0, "http://ns#")
        self.predicate_remove_entry.config(state = DISABLED)

        # Object Label
        self.object_remove_label = Label(self.rdfsparql_frame, text="Remove object")
        self.object_remove_label.grid(row = 3, column = 2, sticky = SW, padx = 20, pady = 3)

        # Object Entry
        self.object_remove_entry = Entry(self.rdfsparql_frame)
        self.object_remove_entry.grid(row = 4, column = 2, sticky = W+E+N, padx = 20, pady = 3)
        self.object_remove_entry.insert(0, "http://ns#")
        self.object_remove_entry.config(state = DISABLED)

        # Buttons' Frame
        self.rdfbuttons_frame = Frame(self.rdfsparql_frame)
        self.rdfbuttons_frame.grid(row = 5, column = 0, columnspan = 3, padx = 20, pady = 3)

        # Insert button
        self.insert_button = Button(self.rdfbuttons_frame)
        self.insert_button["text"] = "Insert"
        self.insert_button["command"] =  self.insert
        self.insert_button.config( state = DISABLED )
        self.insert_button.grid(row = 0, column = 0)
        
        # Remove button
        self.remove_button = Button(self.rdfbuttons_frame)
        self.remove_button["text"] = "Remove"
        self.remove_button["command"] =  self.remove
        self.remove_button.config( state = DISABLED )
        self.remove_button.grid(row = 0, column = 1)

        # Remove button
        self.update_button = Button(self.rdfbuttons_frame)
        self.update_button["text"] = "Update"
        self.update_button["command"] =  self.update
        self.update_button.config( state = DISABLED )
        self.update_button.grid(row = 0, column = 2)

        # RDF query button
        self.rdf_query_button = Button(self.rdfbuttons_frame)
        self.rdf_query_button["text"] = "RDF Query"
        self.rdf_query_button["command"] =  self.rdf_query
        self.rdf_query_button.config( state = DISABLED )
        self.rdf_query_button.grid(row = 0, column = 3)

        # RDF query all button
        self.rdf_query_all_button = Button(self.rdfbuttons_frame)
        self.rdf_query_all_button["text"] = "RDF Query *"
        self.rdf_query_all_button["command"] = self.rdf_query_all
        self.rdf_query_all_button.config( state = DISABLED )
        self.rdf_query_all_button.grid(row = 0, column = 4)

        # RDF subscription button
        self.rdf_subscription_button = Button(self.rdfbuttons_frame)
        self.rdf_subscription_button["text"] = "RDF Subscription"
        self.rdf_subscription_button["command"] =  self.rdf_subscription
        self.rdf_subscription_button.config( state = DISABLED )
        self.rdf_subscription_button.grid(row = 0, column = 5)

        # Rdf_Active_Subs_Label Label
        self.rdf_active_subs_label = Label(self.rdfsparql_frame, text="RDF Active subs (0)")
        self.rdf_active_subs_label.grid(row = 6, column = 0)

        # RDF active subscriptions combobox
        self.rdf_active_subs_combobox_var = StringVar(self.rdfsparql_frame)
        self.rdf_active_subs_combobox_items = ()
        self.rdf_active_subs_combobox = OptionMenu(self.rdfsparql_frame, self.rdf_active_subs_combobox_var, self.rdf_active_subs_combobox_items)
        self.rdf_active_subs_combobox.config( state = DISABLED, width = 20 )
        self.rdf_active_subs_combobox.grid(row = 6, column = 1)

        # Rdf_Unsubscription button
        self.rdf_unsubscription_button = Button(self.rdfsparql_frame)
        self.rdf_unsubscription_button["text"] = "RDF Unsubscription"
        self.rdf_unsubscription_button["command"] =  self.rdf_unsubscription
        self.rdf_unsubscription_button.config( state = DISABLED )
        self.rdf_unsubscription_button.grid(row = 6, column = 2)
        
        # Separator
        self.sep = Frame(self.rdfsparql_frame, height = 250, relief=SUNKEN)
        self.sep.grid(row = 0, column = 3, rowspan = 7)

        # SPARQL Label
        self.sparql_label = Label(self.rdfsparql_frame, text="SPARQL Interaction", font = section_font)
        self.sparql_label.grid(row = 0, column = 4, sticky = NW, padx = 20, pady = 3)

        # Sparql Text
        self.sparql_text = Text(self.rdfsparql_frame)
        self.sparql_text.grid(row = 1, column = 4, rowspan = 4, padx = (20,0), pady = 3, columnspan = 3)
        self.sparql_text.config(height = 8, state = DISABLED)
        self.sparql_text.insert(INSERT, general_sparql_query)

        # Sparql scrollbar
        self.sparql_scrollbar = Scrollbar(self.rdfsparql_frame)
        self.sparql_scrollbar.grid(row = 1, column = 7, rowspan = 4, sticky = N + S, pady = 3, padx = (0,20))
        self.sparql_scrollbar.config( command = self.sparql_text.yview )
        self.sparql_text.config(yscrollcommand=self.sparql_scrollbar.set)

        # Sparql buttons' frame
        self.sparqlbuttons_frame = Frame(self.rdfsparql_frame)
        self.sparqlbuttons_frame.grid(row = 5, column = 4, padx = 20, pady = 3, columnspan = 4)

        # Sparql_Query button
        self.sparql_query_button = Button(self.sparqlbuttons_frame)
        self.sparql_query_button["text"] = "SPARQL Query"
        self.sparql_query_button["command"] =  self.sparql_query
        self.sparql_query_button.config( state = DISABLED )
        self.sparql_query_button.grid(row = 0, column = 0)

        # Sparql_Query button
        self.sparql_query_all_button = Button(self.sparqlbuttons_frame)
        self.sparql_query_all_button["text"] = "SPARQL Query *"
        self.sparql_query_all_button["command"] =  self.sparql_query_all
        self.sparql_query_all_button.config( state = DISABLED )
        self.sparql_query_all_button.grid(row = 0, column = 1)

        # Sparql_Subscription button
        self.sparql_subscription_button = Button(self.sparqlbuttons_frame)
        self.sparql_subscription_button["text"] = "SPARQL Subscription"
        self.sparql_subscription_button["command"] =  self.sparql_subscription
        self.sparql_subscription_button.config( state = DISABLED )
        self.sparql_subscription_button.grid(row = 0, column = 2)

        # SPARQL Active_Subs_Label Label
        self.sparql_active_subs_label = Label(self.rdfsparql_frame, text="SPARQL Active subs (0)")
        self.sparql_active_subs_label.grid(row = 6, column = 4, sticky = W, padx = 20, pady = 3)

        # SPARQL active subscriptions combobox
        self.sparql_active_subs_combobox_var = StringVar(self.rdfsparql_frame)
        self.sparql_active_subs_combobox_items = ()
        self.sparql_active_subs_combobox = OptionMenu(self.rdfsparql_frame, self.sparql_active_subs_combobox_var, self.sparql_active_subs_combobox_items)
        self.sparql_active_subs_combobox.config( state = DISABLED, width = 20 )
        self.sparql_active_subs_combobox.grid(row = 6, column = 5)

        # Sparql_Unsubscription button
        self.sparql_unsubscription_button = Button(self.rdfsparql_frame)
        self.sparql_unsubscription_button["text"] = "SPARQL Unsubscription"
        self.sparql_unsubscription_button["command"] =  self.sparql_unsubscription
        self.sparql_unsubscription_button.config( state = DISABLED )
        self.sparql_unsubscription_button.grid(row = 6, column = 6)

        # Notification frame
        self.notification_frame = LabelFrame(self.rdfsparql_frame)
        self.notification_frame.grid(row = 7, column = 0, columnspan = 8, sticky = E+W)

        # Notification Label
        self.notification_label = Label(self.notification_frame, text="Waiting for commands...")
        self.notification_label.pack(side = BOTTOM, padx = 10, pady = 2)

        # End buttons frame
        self.end_buttons_frame = Frame(self.rdfsparql_frame)
        self.end_buttons_frame.grid(row = 8, column = 0, columnspan = 8, sticky = E)
        
        # Quit button
        self.quit_button = Button(self.end_buttons_frame, text = "Quit")
        self.quit_button.pack(side = RIGHT)
        self.quit_button["command"] = sys.exit

        # Back button
        self.back_button = Button(self.end_buttons_frame, text = "Back") #, command=lambda: controller.show_frame(StartPage))
        self.back_button.pack(side = RIGHT)
        self.back_button["command"] = self.back
        

########################################################
##
## SibSearch Class
##
########################################################

class SibSearch(Tkinter.Frame):

    ###################################################
    #
    # select all
    #
    ###################################################
    
    def select_all(self):

        """This method is used to select/deselect all the items
        into the listbox"""

        if self.sflag:

            # toggle flag
            self.sflag = False

            # select all the items
            items = self.sib_listbox.get(0, END)
            for i in range(len(items)):
                self.sib_listbox.selection_set(i)

            # update the text in the button
            self.selectall_button["text"] = "Deselect all"

        else:
            
            # toggle flag
            self.sflag = True

            # deselect all the items
            self.sib_listbox.selection_clear(0, END)

            # update the text in the button
            self.selectall_button["text"] = "Select all"

    ###################################################
    #
    # refresh
    #
    ###################################################
    
    def refresh(self):

        """This method is used to fetch the list of registered SIBs"""

        print self.profile
            
        # manager connection
        manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try :
            print "Connecting to the manager:",
            manager_socket.connect((self.manager_ip, self.manager_port))
            print "OK!"

            # # discoveryAll request
            # print "Sending DiscoveryAll request to the manager:",
            # msg = {"command":"DiscoveryAll"}

            
            if self.profile == None or self.profile == "":
                print "discovery all........"
                # discoveryAll request
                print "Sending DiscoveryAll request to the manager:",
                msg = {"command":"DiscoveryAll"}
            
            else:
                print "discovery where........"
                # discoveryWhere request
                print "-------------" + str(self.profile)
                print type(self.profile)
                print "Sending DiscoveryWhere request to the manager:",
                msg = {"command":"DiscoveryWhere", "sib_profile":"hasOwner:" + str(self.profile)}

            request = json.dumps(msg)
            manager_socket.send(request)
        
            # discovery reply
            while 1:
                msg = manager_socket.recv(4096)
                if msg:
                    manager_socket.close()
                    break
    
            # was it a success?
            parsed_msg = json.loads(msg)
            print parsed_msg
            if parsed_msg["return"] == "fail":
                print colored("failed!", "red", attrs=["bold"])  + "(" + parsed_msg["cause"] +")"
                self.notification_label["text"] = "Discovery failed"
                
            elif parsed_msg["return"] == "ok":
                print "OK!"
                virtual_sib_list = parsed_msg["virtual_sib_list"]
    
                # remove everything from the listbox
                self.sib_listbox.delete(0, END)
          
            # parsing the reply
            self.vsib_list = []
            i = 0
            for vs in virtual_sib_list:
                
                print str(i) + ") " + vs
                
                # built a dict
                sib_dict = {}
                sib_dict["ip"] = virtual_sib_list[vs]["ip"]
                sib_dict["port"] = virtual_sib_list[vs]["port"]
                sib_dict["id"] = vs
                sib_dict["owner"] =  virtual_sib_list[vs]["owner"]
                self.vsib_list.append(sib_dict)
                
                # filling the listbox
                self.sib_listbox.insert(i, str(sib_dict["id"]) + "-------" + str(sib_dict["owner"]))
                
                # increment the counter
                i += 1

            self.connect_button.config(state = NORMAL)
            self.selectall_button.config(state = NORMAL)
            self.notification_label["text"] = "Connected to the manager"
    
        except:
            print colored("failed!", "red", attrs=['bold'])
            self.notification_label["text"] = "Connection to the manager failed"
            self.connect_button.config(state = DISABLED)
            self.selectall_button.config(state = DISABLED)

    ###################################################
    #
    # modify
    #
    ###################################################
    def modify(self):
        """Connects to the selected SIB(s)"""

        # get the list of selected sibs
        selected = self.sib_listbox.curselection()
        
        if len(selected) != 1:
            pass
        
        else:
            ss = self.vsib_list[int(selected[0])]
            if ss["owner"] != "Virtual Multi SIB":
                pass
            else:
                # open new frame to show the other sibs
                self.controller.show_frame(ModifyMultiSIB, None, None, str(ss['id']))
            



    ###################################################
    #
    # connect
    #
    ###################################################

    def connect(self):

        """Connects to the selected SIB(s)"""

        # get the list of selected sibs
        selected = self.sib_listbox.curselection()
        
        # zero sibs
        if len(selected) == 0:
            pass
        
        # only one sib
        elif len(selected) == 1:
            
            ss = self.vsib_list[int(selected[0])]
            self.controller.show_frame(SibInteraction, str(ss['ip']), str(ss['port']))
            #print self.vsib_list[int(selected[0])]

        # more than one sib
        else:
            
            # find the selected SIBs
            id_list = []
            for s in selected:
                print self.vsib_list[int(s)]['id']
                id_list.append(self.vsib_list[int(s)]['id'])

            # Send the NewVirtualMultiSIB request
            msg = {'command':'NewVirtualMultiSIB','sib_list': id_list}
            request = json.dumps(msg)
            manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)            
            try :
                print "Sending NewVirtualMultiSIB request to the manager:", 
                manager_socket.connect((self.manager_ip, self.manager_port))
                manager_socket.send(request)
            except :                
                print colored("failed!", "red", attrs=['bold'])

            # wait for a reply            
            while 1:
                msg = manager_socket.recv(4096)
                if msg:
                    manager_socket.close()
                    parsed_msg = json.loads(msg)
                    break
                
            # parse the reply
            if parsed_msg["return"] == "fail":
                print colored("failed!", "red", attrs=["bold"]) + "(" + parsed_msg["cause"] +")"
            
            elif parsed_msg["return"] == "ok":
                print "OK!"
                vmsib_id = parsed_msg["virtual_multi_sib_info"]["virtual_multi_sib_id"]
                ip = parsed_msg["virtual_multi_sib_info"]["virtual_multi_sib_ip"]
                port = parsed_msg["virtual_multi_sib_info"]["virtual_multi_sib_kp_port"]
                self.controller.show_frame(SibInteraction, ip, port)
            

    def __init__(self, parent, controller):

        self.s = Style()
        self.s.theme_use('clam')
        self.profile = None

        # Frame constructor
        Tkinter.Frame.__init__(self, parent) 
        
        self.controller = controller
        self.sflag = True
        
        # Open the configuration file to read ip and port of the manager server
        conf_file = open("gkp.conf", "r")
        conf = json.load(conf_file)

        # Manager parameters
        self.manager_ip = conf["manager"]["ip"]
        self.manager_port = conf["manager"]["port"]

        # Closing the configuration file
        conf_file.close()

        # Sib Label
        self.sib_label = Label(self)
        self.sib_label["text"] = "Select one or more SIB(s)"
        self.sib_label.pack()

        # Listbox frame
        self.sib_listbox_frame = Frame(self)
        self.sib_listbox_frame.pack()

        # Scrollbar
        self.sib_scrollbar = Scrollbar(self.sib_listbox_frame)
        self.sib_scrollbar.grid(row = 0, column = 1, sticky = S + N)

        # ListBox
        self.sib_listbox = Listbox(self.sib_listbox_frame, yscrollcommand=self.sib_scrollbar.set)
        self.sib_listbox.config(width = 140, selectmode = MULTIPLE, height = 30)
        self.sib_listbox.grid(row = 0, column = 0)

        # Buttons frame
        self.buttons_frame = Frame(self)
        self.buttons_frame.pack()

        # Buttons
        self.back_button = Button(self.buttons_frame, text="Back", command=lambda: controller.show_frame(StartPage))
        self.back_button.pack(side = LEFT)

        self.connect_button = Button(self.buttons_frame, text="Connect")
        self.connect_button.pack(side = LEFT)        
        self.connect_button["command"] = self.connect

        self.selectall_button = Button(self.buttons_frame, text="Select All")
        self.selectall_button.pack(side = LEFT)
        self.selectall_button["command"] = self.select_all

        self.modify_button = Button(self.buttons_frame, text="Modify")
        self.modify_button.pack(side = LEFT)        
        self.modify_button["command"] = self.modify

        self.refresh_button = Button(self.buttons_frame, text="Refresh")
        self.refresh_button.pack(side = LEFT)
        self.refresh_button["command"] = self.refresh

        self.quit_button = Button(self.buttons_frame, text="Quit")
        self.quit_button.pack(side = LEFT)
        self.quit_button["command"] = sys.exit

        self.notification_label = Label(self)
        self.notification_label.pack(side = BOTTOM)

        print 'hello'
        
        # call the refresh method to fill the listbox
        self.refresh()




# main
if __name__ == "__main__":
    app = Application()
    app.show_frame(StartPage)
    app.mainloop()
    
