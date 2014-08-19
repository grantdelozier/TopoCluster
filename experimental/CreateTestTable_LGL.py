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
	#print afile
	xmldoc = ET.parse(file(afile))
	root = xmldoc.getroot()


	wordref = {}
	toporef = {}
	i = 0
	sid = 0
	
	#print root.tag
	#print root.attrib
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
				if item.tag == 'toponyms':
					for topo in item:
						for t in topo.iter('toponym'):
							i+= 1
							for item2 in t:
								if item2.tag == 'start':
									start = item2.text
								if item2.tag == 'phrase':
									phrase = item2.text
								if item2.tag == 'gaztag':
									for item3 in item2:
										if item3.tag == 'lat':
											lat = item3.text
										if item3.tag == 'lon':
											lon = item3.text
							toporef[i] = [did, art_title, art_domain, start, phrase, lat, lon]



				#print item.tag
	return toporef

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
		cur.execute("CREATE TABLE %s (arttitle varchar(400), webdomain varchar(50), did varchar(20), wid integer, placename varchar(40), latit float, longit float, pointgeog Geography(Point, 4326) );" % (out_tbl))
		SQL = "INSERT INTO %s VALUES(%s, %s, %s, %s, %s, %s, %s, ST_GeographyFromText('SRID=4326;Point(%s %s)')) " % (out_tbl, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')
		for xml in files:
			toporefs = parse_xml(test_xml + "/" + xml)
			for t in toporefs:
				#allatts = toporefs[t][2]
				#print allatts
				print toporefs[t]
				t_total += 1
				did = toporefs[t][0]
				art_title = toporefs[t][1]
				art_domain = toporefs[t][2]
				start = toporefs[t][3]
				placename = toporefs[t][4]
				latit = float(toporefs[t][5])
				longit = float(toporefs[t][6]) 
				#filename = xml
				#sid = toporefs[t][0]
				#src = allatts['src']
				#cid = allatts['id']
				#wid = t
				#humanpath = allatts['humanPath']
				#placename =  toporefs[t][1]
				#latit = float(allatts['lat'])
				#longit = float(allatts['long'])
				

				#print "xml filename: ", filename
				#print "sid ", sid
				#print "src ", src
				#print "cid ", cid
				#print "wid ", wid
				#print "humanpath ", humanpath
				#print "placename ", placename
				#print "latit ", latit
				#print "longit ", longit
				cur.execute(SQL, (art_title, art_domain, did, start, placename, latit, longit, longit, latit))



	print "T total", t_total
	conn.commit()
	conn.close()

conn_info = "dbname=topodb user=postgres host='localhost' port='5433' password='grant'"
gtbl = "globalgrid_5_clip_geog"
#out_tbl = "cwar_test"
out_tbl = "lgl_test"
in_dir = "/home/grant/Downloads/LGL/articles/"
calc(in_dir, conn_info, gtbl, out_tbl)
print "Done"