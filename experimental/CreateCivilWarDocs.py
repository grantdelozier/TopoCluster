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

def parse_xml(afile):
	parser = etree.XMLParser(ns_clean=True, recover=True, encoding='latin1')
	xmldoc = ET.parse(file(afile), parser)
	root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	
	#print root.tag
	#print root.attrib
	for child in root.iter('s'):
		#print child.attrib
		#sid = child.attrib['id']
		sid += 1
		#print sid
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
							toporef[i] = [sid, wordref[i], sub3.attrib]
		if sid % 20000 == 0:
			print sid
	return wordref, toporef

def getContext(wordref, i, window, stopwords):
	j = i
	contextlist = [wordref[j]]
	while j > 1:
		j = j - 1
		if i - window >= j:
			break
		if wordref[j] not in stopwords:
			try:
				contextlist.append(wordref[j])
			except: 
				print "~~~~Broken String~~~~"
				print wordref[j]
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	#print len(contextlist)
	j = i
	while j < len(wordref):
		j = j + 1
		if i + window < j:
			break
		if wordref[j] not in stopwords:
			try:
				contextlist.append(wordref[j])
			except: 
				print "~~~~Broken String~~~~"
				print wordref[j]
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	return contextlist

def calc(xml_doc, out_file, window):
	stopwords = set(['.',',','(',')','-', '--', u'\u2010', u'\u2011', u'\u2012', u'\u2013','=',";",':',"'",'"','$','the','a','an','that','this',
					'to', 'be', 'have', 'has', 'is', 'are', 'was', 'am', "'s",
					'and', 'or','but',
					'by', 'of', 'from','in','after','on','for', 'to', 'TO',
					'I', 'me', 'he', 'him', 'she', 'her', 'we', 'us', 'you', 'your', 'yours' 'they', 'them', 'their', 'it', 'its'])
	tf = io.open(out_file, 'w', encoding='utf-8')
	tf.close()
	if os.path.isdir(xml_doc) == True:
		print "Reading as directory"
		files = os.listdir(xml_doc)
		for xml in files:
			wordref, toporef = parse_xml(xml_doc+'/'+xml)
			#m += 1
			print xml
			CreateDocsFile(out_file, wordref, toporef, window, stopwords)



def CreateDocsFile(out_file, wordref, toporef, window, stopwords):
	ouf = io.open(out_file, 'a', encoding='utf-8')
	m = 0
	for i in toporef:
		#print wordref[i]
		m += 1
		contextList = getContext(wordref, i, window, stopwords)
		contextDict = {}
		for w in contextList:
			nw = w
			if ' ' in w.strip():
				nw = w.strip().replace(' ', '|')
				nws = w.strip().split(' ')
				for n in nws:
					contextDict[n] = contextDict.get(n, 0) + 1
			if ':' in w.strip():
				nw = w.strip().replace(':','')
			if len(nw) > 0:
				contextDict[nw] = contextDict.get(nw, 0) + 1
		for thing in contextDict:
			s1 = [str.join('', [' ', k, ':', unicode(v)]) for k,v in contextDict.items()]
			s2 = str.join('', s1)
			lat_long = unicode(toporef[i][2]['lat']) + ',' + unicode(toporef[i][2]['long'])
		ouf.write(unicode(m) + '\t' + unicode(toporef[i][0])+'-'+unicode(toporef[i][1]) + '\t' + lat_long + '\t' + '?' + '\t' + '?' + '\t' + '?' + '\t' + '?' + '\t' + '?' + '\t' + s2.strip() + '\r\n')
		if m % 5000 == 0:
			print m
	ouf.close()

out_file = "Cwar_dev_docs.txt"
xml_doc = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev"
window = 100
calc(xml_doc, out_file, window)
print "Done"
