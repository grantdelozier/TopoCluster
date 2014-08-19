import sys
import os


import psycopg2
import xml.etree.ElementTree as ET
from lxml import etree
import math

from collections import Counter
from operator import itemgetter

import datetime

import collections

import datetime
import io
import math

def parse_xml(afile, out_dir):
	parser = etree.XMLParser(ns_clean=True, recover=True, encoding='latin1')
	xmldoc = ET.parse(file(afile), parser)
	root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	
	'''cwar_test1 = io.open("cwar_test_1.xml", 'w', encoding='utf-8')
	cwar_test1.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test1.write(u'<corpus>'+'\r\n')
	cw1 = 0
	cwar_test2 = io.open("cwar_test_2.xml", 'w', encoding='utf-8')
	cwar_test2.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test2.write(u'<corpus>'+'\r\n')
	cw2 = 0
	cwar_test3 = io.open("cwar_test_3.xml", 'w', encoding='utf-8')
	cwar_test3.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test3.write(u'<corpus>'+'\r\n')
	cw3 = 0
	cwar_test4 = io.open("cwar_test_4.xml", 'w', encoding='utf-8')
	cwar_test4.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test4.write(u'<corpus>'+'\r\n')
	cw4 = 0
	cwar_test5 = io.open("cwar_test_5.xml", 'w', encoding='utf-8')
	cwar_test5.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test5.write(u'<corpus>'+'\r\n')
	cw5 = 0
	cwar_test6 = io.open("cwar_test_6.xml", 'w', encoding='utf-8')
	cwar_test6.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test6.write(u'<corpus>'+'\r\n')
	cw6 = 0
	cwar_test7 = io.open("cwar_test_7.xml", 'w', encoding='utf-8')
	cwar_test7.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test7.write(u'<corpus>'+'\r\n')
	cw7 = 0
	cwar_test8 = io.open("cwar_test_8.xml", 'w', encoding='utf-8')
	cwar_test8.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test8.write(u'<corpus>'+'\r\n')
	cw8 = 0
	cwar_test9 = io.open("cwar_test_9.xml", 'w', encoding='utf-8')
	cwar_test9.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test9.write(u'<corpus>'+'\r\n')
	cw9 = 0
	cwar_test10 = io.open("cwar_test_10.xml", 'w', encoding='utf-8')
	cwar_test10.write(u'<?xml version="1.0" encoding="UTF-8"?>'+'\r\n')
	cwar_test10.write(u'<corpus>'+'\r\n')
	cw10 = 0'''

	#print root.tag
	#print root.attrib
	for document in root.iter('doc'):
		#print child.attrib
		#sid = child.attrib['id']
		sid += 1
		did = document.attrib['id']
		wf = io.open(out_dir+"cwar_dev_"+did+".xml", 'w', encoding='utf-8')
		wf.write(unicode(ET.tostring(document)))
		#print sid
		'''if cw1 == cw2 and cw1 == cw6:
			cwar_test1.write(unicode(ET.tostring(document)))
			cw1 += 1
		elif cw1 > cw2 and cw2 == cw3:
			cwar_test2.write(unicode(ET.tostring(document)))
			cw2 += 1
		elif cw2 > cw3 and cw3 == cw4:
			cwar_test3.write(unicode(ET.tostring(document)))
			cw3 += 1
		elif cw3 > cw4 and cw4 == cw5:
			cwar_test4.write(unicode(ET.tostring(document)))
			cw4 += 1
		elif cw4 > cw5 and cw5 == cw6:
			cwar_test5.write(unicode(ET.tostring(document)))
			cw5 += 1
		elif cw5 > cw6 and cw6 == cw7:
			cwar_test6.write(unicode(ET.tostring(document)))
			cw6 += 1
		elif cw6 > cw7 and cw7 == cw8:
			cwar_test7.write(unicode(ET.tostring(document)))
			cw7 += 1
		elif cw7 > cw8 and cw8 == cw9:
			cwar_test8.write(unicode(ET.tostring(document)))
			cw8 += 1
		elif cw8 > cw9 and cw9 == cw10:
			cwar_test9.write(unicode(ET.tostring(document)))
			cw9 += 1
		elif cw9 > cw10:
			cwar_test10.write(unicode(ET.tostring(document)))
			cw10 += 1'''
		wf.close()

		if sid % 5 == 0:
			print sid

	'''cwar_test1.write(u'</corpus>'+'\r\n')
	cwar_test2.write(u'</corpus>'+'\r\n')
	cwar_test3.write(u'</corpus>'+'\r\n')
	cwar_test4.write(u'</corpus>'+'\r\n')
	cwar_test5.write(u'</corpus>'+'\r\n')
	cwar_test6.write(u'</corpus>'+'\r\n')
	cwar_test7.write(u'</corpus>'+'\r\n')
	cwar_test8.write(u'</corpus>'+'\r\n')
	cwar_test9.write(u'</corpus>'+'\r\n')
	cwar_test10.write(u'</corpus>'+'\r\n')

	cwar_test1.close()
	cwar_test2.close()
	cwar_test3.close()
	cwar_test4.close()
	cwar_test5.close()
	cwar_test6.close()
	cwar_test7.close()
	cwar_test8.close()
	cwar_test9.close()
	cwar_test10.close()'''

def calc(xml_doc):
	stopwords = set(['.',',','(',')','-', '--', u'\u2010', u'\u2011', u'\u2012', u'\u2013','=',";",':',"'",'"','$','the','a','an','that','this',
					'to', 'be', 'have', 'has', 'is', 'are', 'was', 'am', "'s",
					'and', 'or','but',
					'by', 'of', 'from','in','after','on','for', 'to', 'TO',
					'I', 'me', 'he', 'him', 'she', 'her', 'we', 'us', 'you', 'your', 'yours' 'they', 'them', 'their', 'it', 'its'])
	out_dir = "/home/grant/devel/TopCluster/cwar/cwar/xml/"
	if os.path.isdir(xml_doc) == True:
		print "Reading as directory"
		files = os.listdir(xml_doc)
		for xml in files:
			parse_xml(xml_doc+'/'+xml, out_dir)
			#m += 1
			print xml

calc("/home/grant/devel/TopCluster/cwar/cwar/xml/dev")