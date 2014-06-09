#!/usr/bin/python

# requirements
from Tkinter import *
import tkMessageBox
import Tkinter
from SIBLib import *


top = Tk()

Lb1 = Listbox(top)
Lb1.config(selectmode = MULTIPLE)
Lb1.insert(1, "Python")
Lb1.insert(2, "Perl")
Lb1.insert(3, "C")
Lb1.insert(4, "PHP")
Lb1.insert(5, "JSP")
Lb1.insert(6, "Ruby")

Lb1.pack()
top.mainloop()
