import sys
import os

import xml.etree.ElementTree as ET

def parse_xml(afile):
    xmldoc = ET.parse(file(afile))
    root = xmldoc.getroot()

    wordref = {}
    toporef = {}
    i = 0
    
    print root.tag
    print root.attrib
    for child in root.iter('s'):
 		#print child.attrib
 		for sub in child:
 			i += 1
 			#print sub.tag, sub.attrib
 			if sub.tag == "w":
 				#print sub.attrib['tok']
 				wordref[i] = sub.attrib['tok']
 			elif sub.tag == "toponym":
 				#print sub.attrib['term']
 				wordref[i] = sub.attrib['term']
 				for sub2 in sub:
 					for sub3 in sub2:
 						if "selected" in sub3.attrib:
 							#print sub3.attrib
 							toporef[i] = [wordref[i], sub3.attrib]
    return wordref, toporef

def calc(stat_tbl, test_xml, conn_info):
    print "Local Statistics Table Name: ", stat_tbl
    print "Test XML directory/file path: ", test_xml
    print "DB conneciton info: ", conn_info

    if os.path.isdir(test_xml) == True:
        print "Need to implement logic for directory..."
    elif os.path.isdir(test_xml) == False:
        print "Reading as file"
        wordref, toporef = parse_xml(test_xml)
        for topo in toporef:
        	print toporef[topo][0], toporef[topo][1]['lat'], toporef[topo][1]['long']
        