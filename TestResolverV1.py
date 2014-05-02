import sys
import os

from xml.dom import minidom

def parse_xml(afile):
    xmldoc = minidom.parse(test_xml)
    xmldoc.getElementsByTagName('s')
    
    

def calc(stat_tbl, test_xml):
    print "Local Statistics Table Name: ", stat_tbl
    print "Test XML directory/file path: ", test_xml

    if os.path.isdir(test_xml) == True:
        print "Need to implement logic for directory..."
    elif os.path.isdir(test_xml) == False:
        print "Reading as file"
        
        
        
