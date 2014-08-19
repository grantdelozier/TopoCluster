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
			closest_gids |= get_closest_gids(wordref, toporef, xml, tst_tbl, window, out_domain_stat_tbl, in_domain_stat_tbl, cur, stopwords, lat_long_lookup, results_file, total_topo, closest_gids)

		print "Building Feature Files..."
		for xml in files:
			m += 1
			print xml
			print "Left to go: ", len(files) - m
			print "Total Toponyms ", total_topo
			wordref, toporef = parse_xml(test_xml+'/'+xml)

			total_topo = MakeFeatures(wordref, toporef, xml, tst_tbl, window, out_domain_stat_tbl, in_domain_stat_tbl, cur, stopwords, lat_long_lookup, results_file, total_topo, closest_gids)
			
			#point_error_sum, poly_error_sum, total_topo, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct = VectorSum(wordref, toporef, total_topo, point_error_sum, poly_error_sum, cur, lat_long_lookup,  
			#	percentile, window, stopwords, place_name_weight, xml, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, country_tbl, region_tbl, state_tbl,
			#	 geonames_tbl, point_total_correct, poly_total_correct, tst_tbl, cntry_alt, region_alt, state_alt, pplc_alt, in_domain_stat_tbl, in_corp_lamb, out_corp_lamb)
			#error_sum2 = MostOverlap(wordref, toporef, error_sum2, cur, lat_long_lookup, stat_tbl, percentile, window, stopwords, place_name_weight, xml)
		conn.close()


def get_closest_gids(wordref, toporef, xml, tst_tbl, window, table, in_domain_stat_tbl, cur, stopwords, lat_long_lookup, results_file, total_topo, closest_gids):
	tab1 = [chr(item) for item in range(ord('a'), ord('i')+1)]
	tab2 = [chr(item) for item in range(ord('j'), ord('s')+1)]
	tab3 = [chr(item) for item in range(ord('t'), ord('z')+1)]
	for j in toporef:
		topobase = str(toporef[j][0])
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

		closest_gid_SQL = "Select p1.gid from %s as p1 ORDER BY ST_Distance(p1.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) ASC LIMIT 1;" % (gtbl ,gold_long, gold_lat)

		cur.execute(closest_gid_SQL)
		closest_gid = cur.fetchall()[0][0]
		closest_gids |= set([closest_gid])
	return closest_gids

def MakeFeatures(wordref, toporef, xml, tst_tbl, window, table, in_domain_stat_tbl, cur, stopwords, lat_long_lookup, results_file, total_topo, closest_gids):
	tab1 = [chr(item) for item in range(ord('a'), ord('i')+1)]
	tab2 = [chr(item) for item in range(ord('j'), ord('s')+1)]
	tab3 = [chr(item) for item in range(ord('t'), ord('z')+1)]
	for j in toporef:
		topobase = str(toporef[j][0])
		total_topo += 1
		print topobase, total_topo
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

		closest_gid_SQL = "Select p1.gid from %s as p1 ORDER BY ST_Distance(p1.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) ASC LIMIT 1;" % (gtbl ,gold_long, gold_lat)

		cur.execute(closest_gid_SQL)
		closest_gid = cur.fetchall()[0][0]

		totaldict = Counter()
		contrib_dict = {}
		#print contextlist
		FeatureList = []
		reg_word_dict_out = {}
		other_topo_dict_out = {}
		main_topo_dict_out = {}
		reg_word_dict_in = {}
		other_topo_dict_in = {}
		main_topo_dict_in = {}
		for word in contextlist:
			#print word, gold_lat, gold_long, closest_gid, lat_long_lookup[closest_gid]
			if word[0] not in stopwords:
				#print word
				table = getCorrectTable(word[0], tab1, tab2, tab3)
				if len(table) > 0:
					#print word
					SQL = "Select gid, stat FROM %s WHERE word = %s ;" % (table, '%s')
					SQL2 = "Select gid, stat FROM %s WHERE word = %s ;" % (in_domain_stat_tbl, '%s')
					cur.execute(SQL, (word[0], ))
					#if word in topotokens:
					#	weight = place_name_weight
					#else: weight = 1.0
					adict = dict([(int(k), float(v)) for k, v in cur.fetchall()])
					#print len(adict)

					for gstat in adict.items():
						if word[1] == "Word" and gstat[0] in closest_gids:
							reg_word_dict_out[gstat[0]] = reg_word_dict_out.get(gstat[0], 0.0) + float(gstat[1])
						if word[1] == "MainTopo" and gstat[0] in closest_gids:
							#print "Inside Outer MainTopo"
							main_topo_dict_out[gstat[0]] = main_topo_dict_out.get(gstat[0], 0.0) + float(gstat[1])
						if word[1] == "OtherTopo" and gstat[0] in closest_gids:
							other_topo_dict_out[gstat[0]] = other_topo_dict_out.get(gstat[0], 0.0) + float(gstat[1])
					cur.execute(SQL2, (word[0], ))
					adict2 = dict([(int(k), float(v)) for k, v in cur.fetchall()])

					for gstat in adict2.items():
						if word[1] == "Word" and gstat[0] in closest_gids:
							reg_word_dict_in[gstat[0]] = reg_word_dict_in.get(gstat[0], 0.0) + float(gstat[1])
						if word[1] == "MainTopo" and gstat[0] in closest_gids:
							main_topo_dict_in[gstat[0]] = main_topo_dict_in.get(gstat[0], 0.0) + float(gstat[1])
						if word[1] == "OtherTopo" and gstat[0] in closest_gids:
							other_topo_dict_in[gstat[0]] = other_topo_dict_in.get(gstat[0], 0.0) + float(gstat[1])
					#for gstat in adict.items():
					#	FeatureList.append([word[0]+'-'+'in'+'-'+gstat[0], gstat[1]])

					'''adict3 = dict([(k, v * weight) for k, v in dict(Counter(adict)+Counter(adict2)).items()])
					ranked_fetch = sorted(adict3.items(), key=itemgetter(1), reverse=True)
					subset_ranked = dict(ranked_fetch[:int(len(ranked_fetch)*percentile)])
					for gid in subset_ranked:
						#print gid
						contrib_dict.setdefault(gid, list()).append([word, subset_ranked[gid]])
						#contrib_dict[gid] = combine_tuples(contrib_dict.get(gid, (word, 0.0)), gid)
					#print word, ranked_fetch[:5]
					totaldict += Counter(subset_ranked)'''
		print "Reg Word Out Size:", len(reg_word_dict_out)
		print "Reg word In Size: ", len(reg_word_dict_in)
		print "Main Topo Out Size: ", len(main_topo_dict_out)
		print "Main Topo In Size: ", len(main_topo_dict_in)
		print "Other Topo Out Size: ", len(other_topo_dict_out)
		for gstat in reg_word_dict_out.items():
			FeatureList.append(['Word_out'+'-'+str(gstat[0]), gstat[1]])
		for gstat in reg_word_dict_in.items():
			FeatureList.append(['Word_in'+'-'+str(gstat[0]), gstat[1]])
		for gstat in other_topo_dict_out.items():
			FeatureList.append(['OtherTopo_out'+'-'+str(gstat[0]), gstat[1]])
		for gstat in other_topo_dict_in.items():
			FeatureList.append(['OtherTopo_in'+'-'+str(gstat[0]), gstat[1]])
		for gstat in main_topo_dict_in.items():
			FeatureList.append(['MainTopo_in'+'-'+str(gstat[0]), gstat[1]])
		for gstat in main_topo_dict_out.items():
			FeatureList.append(['MainTopo_out'+'-'+str(gstat[0]), gstat[1]])
		opf = io.open(results_file, 'a', encoding='utf-8')
		s1 = [str.join('', [' ', feat[0], ':', str(feat[1])]) for feat in FeatureList]
		s2 = str.join('', s1)
		opf.write(topobase.replace(' ', '|') + '\t' + unicode(str(closest_gid)) + '\t' + s2 + '\r\n')
		#print totaldict
		#s1 = [str.join('', [' ', k, ':', unicode(v)]) for k,v in contextDict.items()]
		#s2 = str.join('', s1)
		#sorted_total = sorted(totaldict.items(), key=itemgetter(1), reverse=True)
		opf.close()
	return total_topo


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