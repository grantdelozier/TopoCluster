import sys
import os


import psycopg2
import xml.etree.ElementTree as ET
import math

from collections import Counter
from operator import itemgetter

from lxml import etree
#from lxml.html import soupparser

def parse_xml(afile):
	parser = etree.XMLParser(ns_clean=True, recover=True, encoding='latin1')
	xmldoc = ET.parse(file(afile), parser)
	root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	wid = 0
	
	print root.tag
	print root.attrib
	for child in root.iter('doc'):
		#print child.attrib
		did = child.attrib['id']
		#print sid
		sid = 0
		for sent in child:
			sid += 1
			wid = 0
			for sub in sent:
				if sub.tag == "w" and len(sub.attrib['tok']) > 0:
					i += 1
					wid += 1
					#print sub.attrib['tok']
					wordref[i] = sub.attrib['tok']
				elif sub.tag == "toponym":
					i += 1
					wid += 1
					#print sub.attrib['term']
					#wordref[i] = sub.attrib['term']
					for sub2 in sub:
						for sub3 in sub2:
							if "selected" in sub3.attrib:
								#print sub3.attrib
								toporef[i] = [sid, sub.attrib['term'], sub3.attrib, did, wid]
					if sub.attrib['term'].strip().count(' ') > 0:
						splstr = sub.attrib['term'].strip().split(' ')
						for st in splstr:
							wordref[i] = st
							i += 1
				#print sub.tag, sub.attrib

	return toporef

def parse_xml2(afile):
	xmldoc = ET.parse(file(afile))
	root = xmldoc.getroot()
	#parser = etree.XMLParser(ns_clean=True, recover=True, encoding='latin1')
	#xmldoc = ET.parse(file(afile), parser)
	#root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	
	#print root.tag
	#print root.attrib
	for child in root.iter('doc'):
		did = child.attrib['id']
		sid = 0
		for c in child:
			#print child.attrib
			#sid = child.attrib['id']
			sid += 1
			wid = 0
			#print sid
			for sub in c:
				#print sub.tag, sub.attrib
				if sub.tag == "w" and len(sub.attrib['tok']) > 0:
					i += 1
					#print sub.attrib['tok']
					wordref[i] = sub.attrib['tok']
					wid += 1
				elif sub.tag == "toponym":
					i += 1
					#print sub.attrib['term']
					wordref[i] = sub.attrib['term']
					wid += 1
					for sub2 in sub:
						for sub3 in sub2:
							if "selected" in sub3.attrib:
								#print sub3.attrib
								toporef[i] = [sid, wordref[i], sub3.attrib, did, wid]
	return wordref, toporef

def updateInPlace(a,b):
	a.update(b)
	return a

def calc(test_xml, conn_info, gtbl, out_tbl):
	print "Test XML directory/file path: ", test_xml
	print "DB conneciton info: ", conn_info
	print "Grid table used: ", gtbl
	print "out table name", out_tbl

	conn = psycopg2.connect(conn_info)
	print "Connection Success"

	cur = conn.cursor()

	lat_long_lookup = {}
	SQL2 = "SELECT gid, ST_Y(geog::geometry), ST_X(geog::geometry) from %s ;" % gtbl
	cur.execute(SQL2)
	lat_long_lookup = dict([(g[0], [g[1],g[2]]) for g in cur.fetchall()])
	print len(lat_long_lookup)

	if os.path.isdir(test_xml) == True:
		print "Reading as directory"
		files = os.listdir(test_xml)
		t_total = 0
		cur.execute("CREATE TABLE %s (docid varchar(60), sid varchar(20),  wid integer, placename varchar(40), latit float, longit float, pointgeog Geography(Point, 4326) );" % (out_tbl))
		SQL = "INSERT INTO %s VALUES(%s, %s, %s, %s, %s, %s, ST_GeographyFromText('SRID=4326;Point(%s %s)')) " % (out_tbl, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')
		for xml in files:
			wordef, toporefs = parse_xml2(test_xml + "/" + xml)
			for t in toporefs:
				allatts = toporefs[t][2]
				#print allatts
				t_total += 1
				filename = xml
				sid = toporefs[t][0]
				#src = allatts['src']
				#cid = allatts['id']
				did = toporefs[t][3]
				wid = toporefs[t][4]
				#humanpath = allatts['humanPath']
				placename =  toporefs[t][1]
				latit = float(allatts['lat'])
				longit = float(allatts['long'])
				

				#print "xml filename: ", filename
				#print "did", did
				#print "sid ", sid
				#print "src ", src
				#print "cid ", cid
				#print "wid ", wid
				#print "humanpath ", humanpath
				#print "placename ", placename
				#print "latit ", latit
				#print "longit ", longit
				cur.execute(SQL, (did, sid, wid, placename, latit, longit, longit, latit))
				if t_total % 100 == 0:
					print did
					print t_total



	print "T total", t_total
	conn.commit()
	conn.close()

conn_info = "dbname=topodb user=postgres host='localhost' port='5433' password='grant'"
gtbl = "globalgrid_5_clip_geog"
out_tbl = "lgl_dev_classic"
in_dir = "/home/grant/Downloads/LGL/articles/dev_classicxml"
#in_dir = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev"
calc(in_dir, conn_info, gtbl, out_tbl)
print "Done"