import sys
import os


import psycopg2
import xml.etree.ElementTree as ET
from lxml import etree
import math

from collections import Counter
from operator import itemgetter

import datetime

import nltk

import collections

import datetime
import io
import math

#Use with Cwar
def parse_xml(afile):
	xmldoc = ET.parse(file(afile))
	root = xmldoc.getroot()
	#parser = etree.XMLParser(ns_clean=True, recover=False, encoding='latin1')
	#xmldoc = ET.parse(file(afile), parser)
	#root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	
	#print root.tag
	#print root.attrib
	#print root.iter('doc')
	print afile
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

#Use with trconll
def parse_xml2(afile):
	#print afile
	xmldoc = ET.parse(file(afile))
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
	return wordref, toporef

#Use with LGL
def parse_xml3(afile):
	#print afile
	xmldoc = ET.parse(file(afile))
	root = xmldoc.getroot()


	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	
	sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

	#print root.tag
	#print root.attrib
	start_end = {}
	for child in root.iter('articles'):
		#print child.attrib
		#sid = child.attrib['id']
		#sid += 1
		#print sid
		for article in child:
			did = article.attrib['docid']
			#print did
			for item in article:
				if item.tag == 'title':
					art_title = item.text
					#print art_title 
				if item.tag == 'domain':
					art_domain = item.text
					#print art_domain
				if item.tag == 'text':
					art_text = item.text
				toporefs = []
				if item.tag == 'toponyms':
					for topo in item:
						for t in topo.iter('toponym'):
							#i+= 1
							for item2 in t:
								if item2.tag == 'start':
									start = item2.text
								if item2.tag == 'end':
									end = item2.text
								if item2.tag == 'phrase':
									phrase = item2.text
								if item2.tag == 'gaztag':
									for item3 in item2:
										if item3.tag == 'lat':
											lat = item3.text
										if item3.tag == 'lon':
											lon = item3.text
									toporefs.append([did, art_title, art_domain, start, end, phrase, lat, lon])
									#print toporefs[-1]
					#sys.exit()
			wordref, i, toporef = getContext2(art_text, wordref, i, toporefs, toporef, sent_detector)


	return toporef, wordref

#Use with LGL classic xml
def parse_xml4():
	pass

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

def getContext2(art_text, wordref, i, toporefs, toporef, sent_detector):
	span = 0
	#print art_text
	sents = sent_detector.tokenize(art_text)
	se_pairs = [[t[0], t[1], t[2], int(t[3]), int(t[4]), t[5], t[6], t[7]] for t in toporefs]
	#print sents
	for s in sents:
		tokens = nltk.word_tokenize(s)
		#print tokens
		#print s
		for tok in tokens:
			i+= 1
			start_span = span + art_text[span:].index(tok)
			span += len(tok)
			#end_span = span
			#print tok
			#print tok
			#print start_span
			#print start_span+len(tok)
			#print art_text[start_span:(start_span+len(tok))].strip()
			if Between(start_span, se_pairs) == "-99":
				wordref[i] = [start_span, start_span+len(tok), tok]
				#print tok
				#print i
			else:
				if (i == 1) or (i >= 2 and wordref[i-1][2] != Between(start_span, se_pairs)[5]):
					t3 = Between(start_span, se_pairs)
					#print t3[5]
					#print i
					wordref[i] = [t3[3], t3[4], t3[5]]
					toporef[i] = [t3[5], {'did':t3[0], 'start':t3[3], 'lat':t3[6], 'lon':t3[7]}]
				else:
					i = i - 1
			#start_end[did+'_'+start_span)] = [i, tok]
		#sys.exit()
	return wordref, i, toporef

def CreateDocsFile(out_file, wordref, toporef, window, stopwords, m):
	ouf = io.open(out_file, 'a', encoding='utf-8')
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
	return m

def calc(xml_doc, out_file, window, file_type):
	stopwords = set(['.',',','(',')','-', '--', u'\u2010', u'\u2011', u'\u2012', u'\u2013','=',";",':',"'",'"','$','the','a','an','that','this',
					'to', 'be', 'have', 'has', 'is', 'are', 'was', 'am', "'s",
					'and', 'or','but',
					'by', 'of', 'from','in','after','on','for', 'to', 'TO',
					'I', 'me', 'he', 'him', 'she', 'her', 'we', 'us', 'you', 'your', 'yours' 'they', 'them', 'their', 'it', 'its'])
	tf = io.open(out_file, 'w', encoding='utf-8')
	tf.close()
	m = 0
	if os.path.isdir(xml_doc) == True:
		print "Reading as directory"
		files = os.listdir(xml_doc)
		for xml in files:
			if file_type.lower() == "cwar":
				wordref, toporef = parse_xml(xml_doc+'/'+xml)
			elif file_type.lower() == "trconll":
				wordref, toporef = parse_xml2(xml_doc+'/'+xml)
			elif file_type.lower() == "lgl":
				wordref, toporef = parse_xml(xml_doc+'/'+xml)
			#m += 1
			#print xml
			m = CreateDocsFile(out_file, wordref, toporef, window, stopwords, m)