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
import io

import collections

def parse_xml(afile):
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

def parse_xml2(afile):
	#print afile
	xmldoc = ET.parse(file(afile))
	root = xmldoc.getroot()


	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	
	sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

	lgl_dev = io.open("lgl_dev_articles.txt", 'w', encoding='utf-8')
	d = 0
	lgl_test = io.open("lgl_test_articles.txt", 'w', encoding='utf-8')
	t = 0

	lgl_dev.write(u'<?xml version="1.0" encoding="utf-8"?>'+'\r\n')
	lgl_dev.write(u"<articles>"+'\r\n')
	lgl_test.write(u'<?xml version="1.0" encoding="utf-8"?>'+'\r\n')
	lgl_test.write(u"<articles>"+'\r\n')

	#print root.tag
	#print root.attrib
	start_end = {}
	for child in root.iter('articles'):
		#print child.toxml()
		#print sid
		for article in child:
			did = article.attrib['docid']
			#print did
			#print ET.tostring(article)
			if float(t) < (float(d) * .333) :
				lgl_test.write(unicode(ET.tostring(article)))
				t += 1
			else:
				lgl_dev.write(unicode(ET.tostring(article)))
				d += 1
			if d % 100 == 0:
				print d
	lgl_dev.write(u"</articles>"+'\r\n')
	lgl_test.write(u"</articles>"+'\r\n')
	lgl_test.close()
	lgl_dev.close()
	print "Done"



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

def Between(start_span, se_pairs):
	for t in se_pairs:
		if start_span >= t[3] and start_span <= t[4]:
			return t
	return "-99"

def getContext(wordref, i, window, stopwords):
	j = i
	#print j
	#print sorted(wordref.keys(), reverse=False)[:5]
	#print wordref[j]
	contextlist = [wordref[j][2]]
	while j > 1:
		j = j - 1
		if i - window >= j:
			break
		if wordref[j][2] not in stopwords:
			try:
				#u1 = unicode(wordref[j], 'utf-8')
				if len(wordref[j][2]) == 1 and block(wordref[j][2]) == "General Punctuation":
					pass
					#print "~~~~Forbidden Character~~~~"
					#print wordref[j]
					#print "~~~~~~~~~~~~~~~~~~~~~"
					#sys.exit()
				else:
					contextlist.append(wordref[j][2])
			except: 
				print "~~~~Broken String~~~~"
				print wordref[j][2]
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	#print len(contextlist)
	j = i
	while j < len(wordref):
		j = j + 1
		if i + window < j:
			break
		if wordref[j][2] not in stopwords:
			try:
				if len(wordref[j][2]) == 1 and block(wordref[j][2]) == "General Punctuation":
					pass
					#print "~~~~Forbidden Character~~~~"
					#print wordref[j]
					#print "~~~~~~~~~~~~~~~~~~~~~"
					#sys.exit()
				else:
					contextlist.append(wordref[j][2])
			except: 
				print "~~~~Broken String~~~~"
				print wordref[j][2]
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	return contextlist

def updateInPlace(a,b):
	a.update(b)
	return a

def calc(test_xml):

	if os.path.isdir(test_xml) == True:
		print "Reading as directory"
		files = os.listdir(test_xml)
		for xml in files:
			print xml
			#print "Left to go: ", len(files) - m
			#print "Total Toponyms ", total_topo
			parse_xml2(test_xml+'/'+xml)


def VectorSum(wordref, toporef):
	for j in toporef:
		print "=======Vector Sum=============="
		#total_topo += 1
		#print total_topo
		#contextlist = getContext(wordref, j, window, stopwords)


test_xml = "/home/grant/Downloads/LGL/articles/full"
calc(test_xml)