import sys
import os
import json

import sys
oldpaths = sys.path
#sys.path = ['/Library/Frameworks/GDAL.framework/Versions/1.11/Python']
sys.path = ['/Library/Frameworks/GDAL.framework/Versions/1.11/Python/2.7/site-packages']
sys.path.extend(oldpaths)

from osgeo import gdal
print gdal.__version__


import psycopg2
import xml.etree.ElementTree as ET
from lxml import etree
import math

from collections import Counter
from operator import itemgetter

import datetime
import nltk
import io

from osgeo import ogr

def html_escape(text):
	html_escape_table = {"&": "&amp;",
	'"': "&quot;",
	"'": "&apos;",
	">": "&gt;",
	"<": "&lt;",
	}
	#"""Produce entities within text."""
	return "".join(html_escape_table.get(c,c) for c in text)

def parse_json(afile):

	i = 0
	sid = 0
	wordref = {}
	toporef = {}

	sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

	with open(afile, 'rb') as r:
		data = json.load(r)
	for article in data:
		vol = article['vol']
		docid = article['docid']
		did = str(vol) + '-' + str(docid)
		text = article['text']
		toporefs = []
		if 'named_entities' in article:
			for entity in article['named_entities']:
				if entity['entity_type'] == 'place':
					lat = 'NA'
					lon = 'NA'
					name = entity['entity_string']
					char_end = entity['char_end']
					char_start = entity['char_start']
					#print entity['geo']
					geojson = str(entity['geo'])
					if len(geojson) > 0:
						print geojson
						geom = ogr.CreateGeometryFromJson(geojson)
						centroid = geom.Centroid()
						print name
						print centroid
						lat = str(centroid.GetY())
						lon = str(centroid.GetX())
					toporefs.append([did, vol, 'wotr', char_start, char_end, name, lat, lon, geojson])
		wordref, i, toporef = getContext2(text, wordref, i, toporefs, toporef, sent_detector, did, vol, 'wotr')
	return toporef, wordref

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
			wordref, i, toporef = getContext2(art_text, wordref, i, toporefs, toporef, sent_detector, did, art_title, art_domain)


	return toporef, wordref

def CreateXML(out_dir, json_name, toporef, wordref):
	docs_written = []
	print json_name
	if '.json' in json_name:
		rindex = json.rfind('/')
		outname = json_name[rindex:-5]
		print outname
	for i in sorted(wordref.keys()):
		did = wordref[i][-4]
		art_title = wordref[i][-3]
		art_domain = wordref[i][-1]
		tok = wordref[i][2]
		if did not in docs_written:
			if len(docs_written) > 0:
				outfile.write(u'</s>' + '\r\n')
				outfile.write(u'</doc>'+'\r\n')
				outfile.close()
			outfile = io.open(os.path.join(out_dir, outname+'-'+did+'.xml'), 'w', encoding='utf-8')
			outfile.write(u'<?xml version="1.0" encoding="utf-8"?>'+'\r\n')
			outfile.write(u'<doc id="'+did +u'" title="' + html_escape(art_title) + u'" domain="' + html_escape(art_domain)  + u'">'+'\r\n')
			outfile.write(u'<s id="s1">' + '\r\n')
			docs_written.append(did)
		if wordref[i][-2] == False:	
			outfile.write(u'<w tok="' + html_escape(tok) + u'"/>' + '\r\n')
		else:
			lat = toporef[i][1]['lat']
			lon = toporef[i][1]['lon']
			outfile.write(u'<toponym term="' + html_escape(tok)  + '">' + '\r\n')
			outfile.write(u'<candidates>'+'\r\n')
			outfile.write(u'<cand lat="' + lat + '" long="' + lon +u'" selected="yes"'  +u'/>' +'\r\n')
			outfile.write(u'</candidates>'+ '\r\n')
			outfile.write(u'</toponym>'+'\r\n')
	outfile.write(u'</s>' + '\r\n')		
	outfile.write(u'</doc>'+'\r\n')
	outfile.close()





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



def getContext2(art_text, wordref, i, toporefs, toporef, sent_detector, did, art_title, domain):
	span = 0
	#print art_text
	sents = sent_detector.tokenize(art_text)
	se_pairs = [[t[0], t[1], t[2], int(t[3]), int(t[4]), t[5], t[6], t[7]] for t in toporefs]
	#print sents
	for s in sents:
		tokens = nltk.word_tokenize(s)
		#print tokens
		#print s
		#print s
		for tok in tokens:
			i+= 1 
			if tok == "``" or tok == "''":
				tok = '"'
			#print tok
			try:
				start_span = span + art_text[span:].index(tok)
			except:
				print "Broken"
				print s
				print tok
				sys.exit()
			span += len(tok)
			#end_span = span
			#print tok
			#print tok
			#print start_span
			#print start_span+len(tok)
			#print art_text[start_span:(start_span+len(tok))].strip()
			if Between(start_span, se_pairs) == "-99":
				wordref[i] = [start_span, start_span+len(tok), tok, did, art_title, False, domain]
				#print tok
				#print i
			else:
				if (i == 1) or (i >= 2 and wordref[i-1][2] != Between(start_span, se_pairs)[5]):
					t3 = Between(start_span, se_pairs)
					#print t3[5]
					#print i
					wordref[i] = [t3[3], t3[4], t3[5], did, art_title, True, domain]
					toporef[i] = [t3[5], {'did':t3[0], 'start':t3[3], 'lat':t3[6], 'lon':t3[7]}, domain]
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

def calc(json_file):

	out_dir = "/Users/grant/devel/corpora/wotr-topo-classicxml/train"
	toporef, wordref = parse_json(json_file)
	#with io.open(outfile, 'w', encoding='utf-8') as w:
	CreateXML(out_dir, json_file, toporef, wordref)

json_file = "/Users/grant/devel/GeoAnnotate/wotr-topo-train.json"
calc(json_file)