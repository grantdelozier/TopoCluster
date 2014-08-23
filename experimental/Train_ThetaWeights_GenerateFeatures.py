import sys
import os

import io
import psycopg2
import xml.etree.ElementTree as ET
from lxml import etree
import math

from collections import Counter
from operator import itemgetter

import datetime

import collections

#For use with Tr-ConLL

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
							toporef[i] = [wordref[i], sub3.attrib]
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
				#u1 = unicode(wordref[j], 'utf-8')
				if len(wordref[j]) == 1 and block(wordref[j]) == "General Punctuation":
					pass
					#print "~~~~Forbidden Character~~~~"
					#print wordref[j]
					#print "~~~~~~~~~~~~~~~~~~~~~"
					#sys.exit()
				else:
					contextlist.append(wordref[j])
			except: 
				#print "~~~~Broken String~~~~"
				#print wordref[j]
				pass
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	#print len(contextlist)
	j = i
	while j < len(wordref):
		j = j + 1
		if i + window < j:
			break
		if wordref[j] not in stopwords:
			try:
				if len(wordref[j]) == 1 and block(wordref[j]) == "General Punctuation":
					pass
					#print "~~~~Forbidden Character~~~~"
					#print wordref[j]
					#print "~~~~~~~~~~~~~~~~~~~~~"
					#sys.exit()
				else:
					contextlist.append(wordref[j])
			except:
				pass 
				#print "~~~~Broken String~~~~"
				#print wordref[j]
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	return contextlist

def getContext_NoteTopos(wordref, i, window, stopwords, toporef):
	j = i
	contextlist = [[wordref[j], "MainTopo", (i-j)]]
	while j > 1:
		j = j - 1
		if i - window >= j:
			break
		if j in toporef:
			if " " in wordref[j]:
				contextlist.append([wordref[j].strip().replace(" ", "|"), "OtherTopo", (i-j)])
			else:
				contextlist.append([wordref[j], "OtherTopo", (i-j)])
		elif wordref[j] not in stopwords:
			try:
				#u1 = unicode(wordref[j], 'utf-8')
				if len(wordref[j]) == 1 and block(wordref[j]) == "General Punctuation":
					pass
					#print "~~~~Forbidden Character~~~~"
					#print wordref[j]
					#print "~~~~~~~~~~~~~~~~~~~~~"
					#sys.exit()
				else:
					contextlist.append([wordref[j], "Word", (i-j)])
			except: 
				#print "~~~~Broken String~~~~"
				#print wordref[j]
				pass
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	#print len(contextlist)
	j = i
	while j < len(wordref):
		j = j + 1
		if i + window < j:
			break
		if j in toporef:
			if " " in wordref[j]:
				contextlist.append([wordref[j].strip().replace(" ", "|"), "OtherTopo", (i-j)])
			else:
				contextlist.append([wordref[j], "OtherTopo", (i-j)])
		elif wordref[j] not in stopwords:
			try:
				if len(wordref[j]) == 1 and block(wordref[j]) == "General Punctuation":
					pass
					#print "~~~~Forbidden Character~~~~"
					#print wordref[j]
					#print "~~~~~~~~~~~~~~~~~~~~~"
					#sys.exit()
				else:
					contextlist.append([wordref[j], "Word", (i-j)])
			except:
				pass 
				#print "~~~~Broken String~~~~"
				#print wordref[j]
			#	print "~~~~~~~~~~~~~~~~~~~~~"
	return contextlist

def updateInPlace(a,b):
	a.update(b)
	return a

def getCorrectTable(word, tab1, tab2, tab3):
	tablelist = ['enwiki20130102_ner_final_atoi', 'enwiki20130102_ner_final_jtos', 'enwiki20130102_ner_final_ttoz', 'enwiki20130102_ner_final_other']
	table = ""
	if len(word) > 0:
		if word[0].lower() in tab1:
			table = 'enwiki20130102_ner_final_atoi'
		elif word[0].lower() in tab2: 
			table = 'enwiki20130102_ner_final_jtos'
		elif word[0].lower() in tab3:
			table = 'enwiki20130102_ner_final_ttoz'
		else:
			table = 'enwiki20130102_ner_final_other'
	return table


def calc(in_domain_stat_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile,
 tst_tbl, results_file):
	print "In Domain Local Statistics Table Name: ", in_domain_stat_tbl
	print "Out of domain Local Statistics Table Name: ", out_domain_stat_tbl
	print "Test XML directory/file path: ", test_xml
	print "DB conneciton info: ", conn_info
	print "Grid table used: ", gtbl
	print "Window size", window
	print "Percentile: ", percentile
	#print "Place name weight: ", place_name_weight

	#print "Country table name: ", country_tbl
	#print "Region table name: ", region_tbl
	#print "State table name: ", state_tbl
	#print "Out of Domain Lambda", out_corp_lamb
	#print "In Domain Lambda", in_corp_lamb

	print "Test table name:", tst_tbl

	conn = psycopg2.connect(conn_info)
	print "Connection Success"

	stopwords = set(['.',',','(',')','-', '--', u'\u2010', u'\u2011', u'\u2012', u'\u2013','=',";",':',"'",'"','$','the','a','an','that','this',
					'to', 'be', 'have', 'has', 'is', 'are', 'was', 'am', "'s",
					'and', 'or','but',
					'by', 'of', 'from','in','after','on','for', 'to', 'TO',
					'I', 'me', 'he', 'him', 'she', 'her', 'we', 'us', 'you', 'your', 'yours' 'they', 'them', 'their', 'it', 'its'])

	#stopwords = set([unicode(w, 'utf-8') for w in sw])

	cur = conn.cursor()

	lat_long_lookup = {}
	SQL2 = "SELECT gid, ST_Y(geog::geometry), ST_X(geog::geometry) from %s ;" % gtbl
	cur.execute(SQL2)
	lat_long_lookup = dict([(g[0], [g[1],g[2]]) for g in cur.fetchall()])
	print len(lat_long_lookup)
	point_total_correct = 0
	poly_total_correct = 0

	m = 0

	Observations = {}

	start_time = datetime.datetime.now()

	if os.path.isdir(test_xml) == True:
		print "Reading as directory"
		files = os.listdir(test_xml)
		point_bigerror = []
		poly_bigerror = []
		point_dist_list = []
		poly_dist_list = []
		total_topo = 0
		closest_gids = set([])

		print "Getting closest gids..."
		for xml in files:
			print xml
			wordref, toporef = parse_xml(test_xml+'/'+xml)
			Observations = get_solutions(wordref, toporef, xml, tst_tbl, window, out_domain_stat_tbl, in_domain_stat_tbl, cur, stopwords, lat_long_lookup, results_file, total_topo, Observations)

			opf = io.open(results_file, 'a', encoding='utf-8')
			for ob in Observations:
				print ob
				s1 = [str.join('' [' ', sol]) for sol in Observations[ob]['solutions']]
				s2 = str.join('', s1)

				s3 = [str.join('', [' ', feat]) for feat in Observations[ob]['features']]
				s4 = str.join('', s3)

				row = s2 + " |"  + s4 + '\r\n'

				opf.write(row)
				
			opf.close()
			
			#point_error_sum, poly_error_sum, total_topo, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct = VectorSum(wordref, toporef, total_topo, point_error_sum, poly_error_sum, cur, lat_long_lookup,  
			#	percentile, window, stopwords, place_name_weight, xml, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, country_tbl, region_tbl, state_tbl,
			#	 geonames_tbl, point_total_correct, poly_total_correct, tst_tbl, cntry_alt, region_alt, state_alt, pplc_alt, in_domain_stat_tbl, in_corp_lamb, out_corp_lamb)
			#error_sum2 = MostOverlap(wordref, toporef, error_sum2, cur, lat_long_lookup, stat_tbl, percentile, window, stopwords, place_name_weight, xml)
		conn.close()


def get_solutions(wordref, toporef, xml, tst_tbl, window, table, in_domain_stat_tbl, cur, stopwords, lat_long_lookup, results_file, total_topo, closest_gids):
	tab1 = [chr(item) for item in range(ord('a'), ord('i')+1)]
	tab2 = [chr(item) for item in range(ord('j'), ord('s')+1)]
	tab3 = [chr(item) for item in range(ord('t'), ord('z')+1)]
	for j in toporef:
		Observations[j] = {}
		topobase = toporef[j][0]
		total_topo += 1
		#print topobase, total_topo
		topotokens = []
		contextlist = getContext_NoteTopos(wordref, j, window, stopwords, toporef)
		
		#This section attempts to enforce regularity in case. Attempt to force title case on all place names, except for acronyms
		if topobase.title() != topobase and (len(toporef[j][0]) != 2 and len(toporef[j][0]) != 3):
			#contextlist.append(topobase.title())
			contextlist.append([topobase.title(), "MainTopo", 0])
			#topotokens.append(toporef[j][0].title())
			topobase = topobase.title()
			#print contextlist
			#print "Inside title case changer"
			#print topobase
		#Change acronyms with periods into regular acronyms
		if "." in topobase and ". " not in topobase.strip():
			combinedtokens = ""
			for token in topobase.split("."):
				combinedtokens = combinedtokens + token
				#topotokens.append(token)
				#contextlist.append(token)
			#topotokens.append(topobase.replace('.', ''))
			topotokens.append(combinedtokens)
			#contextlist.append(combinedtokens)
			contextlist.append([combinedtokens, "MainTopo", 0])
		else: topotokens.append(topobase)
		gazet_topos = topotokens
		if " " in topobase:
			topotokens.append(topobase.replace(" ", '|'))
			#contextlist.append(topobase.replace(" ", '|'))
			contextlist.append([topobase.replace(" ", '|'), "MainTopo", 0])
			#for token in topobase.split(" "):
			#	topotokens.append(token)
			#	contextlist.append(token)
		#print toporef[j]
		
		gold_lat = float(toporef[j][1]['lat'])
		gold_long = float(toporef[j][1]['long'])
		gold_doc = xml
		gold_wid = j

		#Currently finds closest global grid points to lat long in corpus
		#Need a new version that finds a set of grid points within the polygon of the gold reference
		#closest_gid_SQL = "Select p1.gid from %s as p1 ORDER BY ST_Distance(p1.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)'));" % (gtbl ,gold_long, gold_lat)
		closest_gid_SQL = "SELECT p2.gid, ST_Distance(p2.geog, p1.geog) from %s as p1, %s as p2 where p1.placename = %s and p1.docname = %s and p1.wid = %s ORDER BY ST_Distance(p2.geog, p1.geog) ASC;" % (tst_tbl, gtbl, '%s', '%s', '%s')

		cur.execute(closest_gid_SQL, (topobase, gold_doc, gold_wid))

		m = 0
		print topobase
		for gid_point in cur.fetchall():
			gid = gid_point[0]
			distance = gid_point[1]
			print gid, distance
			if distance == 0.0:
				Obervations[j].setdefault('solutions', list()).append(str(gid)+":0")
			elif m < 10:
				m += 1
				Observations[j].setdefault('solutions', list()).append(str(gid)+":"+str(m))
				if m >= 10:
					break

		for word in contextlist:
			if word[0] not in stopwords:
				if word[1] == "MainTopo":
					Observations[j].setdefault('features', list()).append("MainTopo-"+word[0].replace(' ', '|'))
				elif word[1] == "OtherTopo":
					Observations[j].setdefault('features', list()).append("OtherTopo-"+word[0].replace(' ', '|'))
				else:
					Observations[j].setdefault('features', list()).append(word[0])


	return Observations

in_domain_stat_tbl = "trconllf_dev_trainsplit1_kernel100k_epanech_gi"
out_domain_stat_tbl = "enwiki20130102_train_kernel100k_grid5_epanech_allwords_ner_fina"
test_xml = "/home/grant/devel/TopCluster/trconllf/xml/dev_trainsplit1/"
conn_info = "dbname=topodb user=postgres host='localhost' port='5433' password='grant'"
gtbl = "globalgrid_5_clip_geog"
window = 15
percentile = 1.0
tst_tbl = "trconllf_dev"
results_file = "TrainingFeatures_TRCoNLLV2.txt"

opf = io.open(results_file, 'w', encoding='utf-8')
opf.close()



calc(in_domain_stat_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile,
 tst_tbl, results_file)