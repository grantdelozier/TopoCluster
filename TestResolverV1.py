import sys
import os


import psycopg2
import xml.etree.ElementTree as ET

from collections import Counter
from operator import itemgetter

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

def getContext(wordref, i, window):
	j = i
	contextlist = [wordref[j]]
	while j > 1:
		j = j - 1
		if i - window >= j:
			break
		contextlist.append(wordref[j])
	#print len(contextlist)
	j = i
	while j < len(wordref):
		j = j + 1
		if i + window < j:
			break
		contextlist.append(wordref[j])
	return contextlist

def updateInPlace(a,b):
    a.update(b)
    return a

def calc(stat_tbl, test_xml, conn_info):
    print "Local Statistics Table Name: ", stat_tbl
    print "Test XML directory/file path: ", test_xml
    print "DB conneciton info: ", conn_info

    conn = psycopg2.connect(conn_info)
    print "Connection Success"

    cur = conn.cursor()

    window = 20

    if os.path.isdir(test_xml) == True:
        print "Need to implement logic for directory..."
    elif os.path.isdir(test_xml) == False:
        print "Reading as file"
        wordref, toporef = parse_xml(test_xml)
        for j in toporef:
        	print toporef[j][0], toporef[j][1]['lat'], toporef[j][1]['long']
        	contextlist = getContext(wordref, j, window)
        	print contextlist
        	totaldict = Counter()
        	for word in contextlist:
        		SQL = "Select gid, stat FROM %s WHERE word = %s ;" % (stat_tbl, '%s')
        		cur.execute(SQL, (word, ))
        		adict =  dict([(int(k), float(v)) for k, v in cur.fetchall()])
        		totaldict += Counter(adict)
        	#print totaldict
        	sorted_total = sorted(totaldict.items(), key=itemgetter(1), reverse=True)
        	print sorted_total[:5]

        	#print len(contextlist)




        