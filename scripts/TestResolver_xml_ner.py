import sys
import os
import copy

import string
import io
import psycopg2
import xml.etree.ElementTree as ET
#from lxml import etree
import math

from collections import Counter
from operator import itemgetter

import datetime

import collections

import UnicodeBlocks
import re

#For use with Tr-ConLL format xml

def parse_xml(afile):
	#print afile
	xmldoc = ET.parse(file(afile))
	root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	topoall = {}
	i = 0
	sid = 0
	for child in root.iter('s'):
		sid += 1
		for sub in child:
			i += 1
			if sub.tag == "w":
				wordref[i] = sub.attrib['tok']
			elif sub.tag == "toponym":
				wordref[i] = sub.attrib['term']
				topoall[i] = sub.attrib['term']
				for sub2 in sub:
					for sub3 in sub2:
						if "selected" in sub3.attrib:
							toporef[i] = [wordref[i], sub3.attrib]
	return wordref, toporef, topoall

#A version of parse_xml that makes sure wordref index is per token
def parse_xml2(afile):
	xmldoc = ET.parse(file(afile))
	root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	topoall = {}
	i = 0
	sid = 0
	for child in root.iter('s'):
		sid += 1
		for sub in child:
			i += 1
			if sub.tag == "w":
				wordref[i] = sub.attrib['tok']
			elif sub.tag == "toponym":			
				topoall[i] = sub.attrib['term']
				for sub2 in sub:
					for sub3 in sub2:
						if "selected" in sub3.attrib:
							toporef[i] = [sub.attrib['term'], sub3.attrib]
				for wrd in sub.attrib['term'].split(" "):
					wordref[i] = wrd
					i += 1
					#if len(sub.attrib['term'].split(" ")) == 1:
					#	wordref[i] = wrd
					#else:
					#	wordref[i] = wrd
					#	i += 1
				i = i -1
	return wordref, toporef, topoall	

def getContext(wordref, i, window, stopwords, toporef):
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
				if len(wordref[j]) == 1 and UnicodeBlocks.block(wordref[j]) == "General Punctuation":
					pass
				else:
					contextlist.append([wordref[j], "Word", (i-j)])
			except: 
				pass
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
				if len(wordref[j]) == 1 and UnicodeBlocks.block(wordref[j]) == "General Punctuation":
					pass
				else:
					contextlist.append([wordref[j], "Word", (i-j)])
			except:
				pass 
	return contextlist

def updateInPlace(a,b):
	a.update(b)
	return a

def writeplaintext(wordref, stan_path, outxml, xml):
	import NERTest as NER
	plaintext = str.join('', [wordref[x]+' ' for x in wordref])
	filename = "/home/grant/devel/TopoCluster/data/tempplain_" + xml
	op = io.open(filename, 'w', encoding='utf-8')
	op.write(unicode(plaintext))
	op.close()
	NER.calc3(stan_path, filename, outxml)
	toporef2, wordref2 = NER.readnerxml2(outxml)
	return wordref2, toporef2

def rebalance(wordref, wordref2, toporef2, index, offset, topoall, toporef):
	done_set = []
	print "Starting Toporef2:", sorted(toporef2.keys())
	print toporef2
	for i in sorted(wordref2.keys()):
		if i > index:
			wordref2[i] = wordref[i]
			if i in topoall and i not in done_set and i+offset not in toporef2 and len(topoall[i].split(" ")) > 1:
				print "Missed this one:", topoall[i]
				offset = offset - (len(topoall[i].split(" ")) - 1)
				"Adjusting offset: ", offset
			if i in toporef2 and i not in done_set:
				print i, "offset", offset
				if i != i+offset:
					toporef2[i+offset] = toporef2[i]
					print "Adding ", i+offset, toporef2[i+offset]
					del toporef2[i]
				done_set.append(i+offset) 
				if i+offset in topoall:
					print "Found match in topoall:", i+offset, topoall[i+offset]
					if toporef2[i+offset] != topoall[i+offset]:
						print toporef2[i+offset], "!=", topoall[i+offset]
						offset = offset + (len(toporef2[i+offset].split(" "))-1) - (len(topoall[i+offset].split(" "))-1)
						print "Adjusting offset:", offset
				else:
					print "++++++++++++++++++++++++++++++"
					print "toporef2 reassignment mismatch"
					print toporef2[i+offset]
					print i+offset
					print "topoall:", sorted(topoall.keys())
					print "toporef2:", sorted(toporef2.keys())
					print "toporef:", sorted(toporef.keys())
					print "done set:", sorted(done_set)
					print toporef2
					#sys.exit()

	return wordref2, toporef2


def redoToporefs(toporef, toporef2, wordref, wordref2, topoall):
	for j in sorted(wordref2.keys()):
		#print j, wordref[j], wordref2[j]
		if j in topoall and j in toporef2 and toporef2[j] == topoall[j]:
			print "CORRECT NER:", j, toporef2[j]
		#if j in topoall and j not in toporef2:
		#	print "False-Negative toporef2", j, topoall[j]
		#if j in toporef2 and j not in topoall:
		#	print "False-Postive toporef2", j, toporef2[j]

	return wordref2, toporef2

def calc(in_domain_stat_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile,
		 main_topo_weight, other_topo_weight, other_word_weight, country_tbl, region_tbl,
		 state_tbl, geonames_tbl, in_corp_lamb, out_corp_lamb, results_file, stan_path):
	print "In Domain Local Statistics Table Name: ", in_domain_stat_tbl
	print "Out of domain Local Statistics Table Name: ", out_domain_stat_tbl
	print "Directory Path containing plain text files to be parsed: ", test_xml
	print "DB connection info: ", conn_info
	print "Grid table used: ", gtbl
	print "Window size", window
	print "Percentile: ", percentile
	print "Main Toponym weight: ", main_topo_weight
	print "Other Toponym weight: ", other_topo_weight
	print "Other Word weight: ", other_word_weight

	print "Country table name: ", country_tbl
	print "Region table name: ", region_tbl
	print "State table name: ", state_tbl
	print "Out of Domain Lambda", out_corp_lamb
	print "In Domain Lambda", in_corp_lamb

	#Test the connection to the database
	conn = psycopg2.connect(conn_info)
	print "Connection Success"

	#These words and characters will not be evaluated or be used for Gi* vector summing
	stopwords = set(['.',',','(',')','-', '--', u'\u2010', u'\u2011', u'\u2012', u'\u2013','=',";",':',"'",'"','$','the','a','an','that','this',
					'to', 'be', 'have', 'has', 'is', 'are', 'was', 'am', "'s",
					'and', 'or','but',
					'by', 'of', 'from','in','after','on','for', 'to', 'TO',
					'I', 'me', 'he', 'him', 'she', 'her', 'we', 'us', 'you', 'your', 'yours' 'they', 'them', 'their', 'it', 'its'])

	#Initialize a cursor for DB connection
	cur = conn.cursor()

	#Check to see if database tables were set up properly
	try:
		ood_table_list = ['enwiki20130102_ner_final_atoi', 'enwiki20130102_ner_final_jtos', 'enwiki20130102_ner_final_ttoz', 'enwiki20130102_ner_final_other']
		for tb in ood_table_list:
			SQL_check = "select * FROM %s LIMIT 1;" % (tb)
			cur.execute(SQL_check)
			result = cur.fetchall()
			if len(result) > 0:	
				print tb, "table loaded correctly"
	except:
		print "Out of domain tables were not loaded correctly"
		print "Exiting..."
		sys.exit()

	try:
		if in_domain_stat_tbl != "None":
			SQL_check = "select * FROM %s LIMIT 1;" % (out_domain_stat_tbl)
			cur.execute(SQL_check)
			result = cur.fetchall()
			if len(result) > 0:
				print out_domain_stat_tbl, "table loaded correctly"
	except:
		print "In domain table provided was not loaded correctly:", in_domain_stat_tbl
		print "Exiting..."
		sys.exit()

	#Intitialize a Ditionary that links GlobalGrid gid values to Latitude/Longitudes
	lat_long_lookup = {}
	SQL2 = "SELECT gid, ST_Y(geog::geometry), ST_X(geog::geometry) from %s ;" % gtbl
	cur.execute(SQL2)
	lat_long_lookup = dict([(int(g[0]), [g[1],g[2]]) for g in cur.fetchall()])
	#print len(lat_long_lookup)

	point_total_correct = 0
	poly_total_correct = 0
	point_ner_fp = 0
	poly_ner_fp = 0
	ner_fn = 0
	poly_ner_tp = 0
	point_ner_tp = 0
	point_incorrect = 0
	poly_incorrect = 0
	total_attempted = 0

	start_time = datetime.datetime.now()

	total_topo = 0

	#Test xml should always be a directory name. System currently only supports directory as an argument, though may change in the future.
	if os.path.isdir(test_xml) == True:
		print "Reading as directory"
		files = os.listdir(test_xml)
		point_bigerror = []
		poly_bigerror = []
		point_dist_list = []
		poly_dist_list = []
		point_error_sum = 0.0
		poly_error_sum = 0.0
		error_sum2 = 0.0
		poly_dist = 0.0
		m = 0

		#These queries are designed to pull all the alternate names from the geonames, country, state, and region tables. Alternate names are used in later steps to enhance gazetteer matching
		SQL1 = "SELECT p1.gid, p1.name, p1.name_long, p1.geonames_gid, p1.altnames FROM %s as p1 ;" % country_tbl
		SQL2 = "SELECT p1.gid, p1.name, p1.name_long, p1.geonames_gid, p1.altnames FROM %s as p1 ;" % region_tbl
		SQL3 = "SELECT p1.gid, p1.name, p1.geonames_gid, p1.altnames FROM %s as p1 ;" % state_tbl
		SQL4 = "SELECT p1.gid, p1.name, p1.asciiname, p1.alternames FROM %s as p1 where p1.featurecode = 'PPLC' or p1.featurecode = 'PPLA' or p1.featurecode = 'PPLA2' or p1.featurecode = 'PPL';" % geonames_tbl

		cur.execute(SQL1)

		cntry_alt = {}
		for row in cur.fetchall():
			alist = [row[1], row[2]]
			if row[4] is not None:
				alist.extend(row[4].split(','))
			#print alist
			for w in alist:
				cntry_alt.setdefault(w, set()).add(row[0])
			#cntry_alt.setdefault(row[0], list()).append(alist)

		cur.execute(SQL3)

		state_alt = {}
		for row in cur.fetchall():
			alist = [row[1], row[2]]
			if row[3] is not None:
				alist.extend(row[3].split(','))
			#print alist
			for w in alist:
				state_alt.setdefault(w, set()).add(row[0])
			#state_alt.setdefault(row[0], list()).append(alist)

		cur.execute(SQL2)

		region_alt = {}
		for row in cur.fetchall():
			alist = [row[1], row[2]]
			#print row
			if len(row) > 3 and row[4] is not None:
				alist.extend(row[4].split(','))
			#print alist
			for w in alist:
				region_alt.setdefault(w, set()).add(row[0])
			#region_alt.setdefault(row[0], list()).append(alist)

		cur.execute(SQL4)
		pplc_alt = {}
		for row in cur.fetchall():
			alist = [row[1], row[2]]
			#print row
			if len(row) > 3 and row[3] is not None:
				alist.extend(row[3].split(','))
			for w in alist:
				pplc_alt.setdefault(w, set()).add(row[0])


		print "Done Creating Alt Names"
		print "Length of PPL AltNames: ", len(pplc_alt)

		#Import script that issues commands to Stanford NER
		import NERTest as NER
		predictions = []

		#Loop through every xml file in directory
		for xml_file in files:
			m += 1
			print xml_file
			filename = os.path.join(test_xml, xml_file)
			print "Files left to go: ", len(files) - m
			print "Total Toponyms ", total_topo

			wordref, toporef, topoall = parse_xml2(filename)
			#try:
			outxml = "ner_" + xml_file 
			wordref2, toporef2 = writeplaintext(wordref, stan_path, outxml, xml_file)

			wordref2, toporef2 = redoToporefs(toporef, toporef2, wordref, wordref2, topoall)

			point_ner_fp, poly_ner_fp, ner_fn, point_ner_tp, poly_ner_tp, predictions, total_topo, point_error_sum, poly_error_sum, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct, point_incorrect, poly_incorrect, total_attempted = VectorSumNER(wordref, toporef, topoall, wordref2, toporef2, total_topo, cur, lat_long_lookup,  
				percentile, window, stopwords, main_topo_weight, other_topo_weight, other_word_weight, xml_file, predictions, country_tbl, region_tbl, state_tbl,
				 geonames_tbl, cntry_alt, region_alt, state_alt, pplc_alt, point_ner_fp, poly_ner_fp, ner_fn, poly_ner_tp, point_ner_tp, in_domain_stat_tbl, in_corp_lamb, out_corp_lamb, point_error_sum, poly_error_sum, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct, point_incorrect, poly_incorrect, total_attempted)
			print "Polygon Accuracy @ 161km : ", float(poly_total_correct) / float(total_topo)
		with io.open(results_file, 'w', encoding='utf-8') as w:
			w.write(u"NER_Toponym,Source_File,Token_index,GeoRefSource,Table,gid,Table_Toponym,Centroid_Lat,Centroid_Long,pointdist_err,polydist_err,point_cor,poly_cor\r\n")
			for p in predictions:
				#The encoding of the toponym can change based on the document being read
				if isinstance(p[0], str):
					toponym = p[0].decode('utf-8')
				if isinstance(p[0], unicode):
					toponym = p[0].encode('utf-8').decode('utf-8')
				#The encoding of the toponym name from the table can change based on the table results were pulled from
				if isinstance(p[6], str):
					table_toponym = p[6].decode('utf-8')
				if isinstance(p[6], unicode):
					table_toponym = p[6].encode('utf-8').decode('utf-8')
				w.write(toponym+u','+p[1]+u','+unicode(p[2])+u','+p[3]+u','+p[4]+u','+unicode(p[5])+u','+table_toponym+u','+unicode(p[7])+u','+unicode(p[8])+u','+unicode(p[9])+u','+unicode(p[10])+u','+unicode(p[11])+u','+unicode(p[12])+'\r\n')
		conn.close()
		point_dist_list.sort()
		poly_dist_list.sort()
		print "=============Vector Sum================"
		print "Total Toponyms: ", total_topo
		print "Window: ", window
		print "Percentile: ", percentile
		print "Main Topo weight:", main_topo_weight
		print "Other Topo weight:", other_topo_weight
		print "Other word weight:", other_word_weight
		print "Point Average Error Distance @ 1: ", ((float(point_error_sum)/float(total_topo)))
		#print "Point Median Error Distance @ 1: ", point_dist_list[total_topo/2]
		print "Point Accuracy @ 161km : ", float(point_total_correct) / float(total_topo)
		print "Polygon Average Error Distance @ 1: ", ((float(poly_error_sum)/float(total_topo)))
		#print "Polygon Median Error Distance @ 1: ", poly_dist_list[total_topo/2]
		print "Polygon Accuracy @ 161km : ", float(poly_total_correct) / float(total_topo)

		print "poly ner tp: ", poly_ner_tp
		print "total topo: ", total_topo

		#point_precision = float(point_ner_tp) / float(point_ner_tp + point_incorrect)
		point_precision = float(point_ner_tp) / float(total_attempted)
		print "Point Precision: ", point_precision
		point_recall = float(point_ner_tp) / float(total_topo)
		print "Point Recall: ", point_recall
		point_fscore = 2*((point_precision * point_recall) / (point_precision + point_recall))
		print "Point F-1 Score: ", 2*((point_precision * point_recall) / (point_precision + point_recall))

		#poly_precision = float(poly_ner_tp) / float(poly_ner_tp + poly_incorrect)
		poly_precision = float(poly_ner_tp) / float(total_attempted)
		print "Poly Precision: ", poly_precision
		poly_recall = float(poly_ner_tp) / float(total_topo)
		print "Poly Recall: ", poly_recall
		poly_fscore = 2*((poly_precision * poly_recall) / (poly_precision + poly_recall))
		print "Poly F-1 Score: ", 2*((poly_precision * poly_recall) / (poly_precision + poly_recall))

	end_time = datetime.datetime.now()

	print total_topo
	print "Check File @ ", results_file+xml_file
	print "Total Time: ", end_time - start_time

#This function evaluates a word to see which out of domain table should be queries to obtain the Gi* vector
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



#Performs actual disambiguation work based on summing Gi* vectors of words in a context window
def VectorSumNER(wordref, toporef, topoall, wordref2, toporef2, total_topo, cur, lat_long_lookup, percentile, window, stopwords, main_topo_weight, other_topo_weight, 
	other_word_weight, xml_file, predictions, country_tbl, region_tbl, state_tbl,
	geonames_tbl, country_alt, region_alt, state_alt, pplc_alt, point_ner_fp, poly_ner_fp, ner_fn, poly_ner_tp, point_ner_tp, in_domain_stat_tbl, in_corp_lamb, out_corp_lamb,
	point_error, poly_error, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct, point_incorrect, poly_incorrect, total_attempted): 

	#Lists of alphabetic characters that help system decide which table partition to query in later steps
	tab1 = [chr(item) for item in range(ord('a'), ord('i')+1)]
	tab2 = [chr(item) for item in range(ord('j'), ord('s')+1)]
	tab3 = [chr(item) for item in range(ord('t'), ord('z')+1)]
	#Loop through all toponyms found in document
	total_topo += len(toporef)
	for j in sorted(toporef2.keys()):
		#print "=======Vector Sum=============="
		#total_topo += 1
		topobase = toporef2[j]
		total_attempted += 1

		print j, topobase
		#print toporef[j][0]
		#print sorted(topoall.keys())
		#print topoall[j]



		topotokens = []

		if j in topoall and topoall[j] == topobase:
			if j in toporef and toporef[j][0] == topobase:
				print "=========="
				print topobase
				print toporef[j]
				#Get all words in a context window around the toponym's index
				contextlist = getContext(wordref, j, window, stopwords, toporef)
				
				#If stanford NER tokenizer breaks and puts ',' with NE, then remove the ending comma or period
				if topobase[-1] == ',':
					topobase = topobase[:-1]
				#if len(topobase) > 1 and topobase[-1] == "." and topobase[-2].islower():
				#	topobase=topobase[:-1]

				#This section attempts to enforce regularity in case. Attempt to force title case on all place names, except for some acronyms
				#acronyms that are avoided are of form CA and USA
				if string.capwords(topobase) != topobase and (len(toporef[j][0]) != 2) and (len(toporef[j][0]) != 3 or re.match(r'\b[A-Z]+.', toporef[j][0])):
					#Remove the all caps topobase from the context
					m = len(contextlist)
					while (m):
						m = m - 1
						if contextlist[m][1] == 'MainTopo':
							#print "Deleting", contextlist[m]
							del contextlist[m]
					contextlist.append([string.capwords(topobase), "MainTopo", 0])
					topobase = string.capwords(topobase)


				#Change acronyms with periods into regular acronyms
				if ". " not in topobase.strip():
					no_period_topobase = re.sub(r"\b([A-Z])\.", r"\1", topobase)
					if topobase != no_period_topobase:
						topotokens.append(no_period_topobase)
						contextlist.append([no_period_topobase, "MainTopo", 0])
					else:
						topotokens.append(topobase)
				else: topotokens.append(topobase)

				#Change acronym with periods into regular acronyms #2
				if ". " in topobase.strip():
					if re.match(r"\b[A-Z]\.\s[A-Z]\.", topobase):
						topotokens.append(topobase.strip()[0]+topobase.strip()[3])
						contextlist.append([topobase.strip()[0]+topobase.strip()[3], "MainTopo", 0])
						topobase = topobase.strip()[0]+topobase.strip()[3]

				gazet_topos = topotokens
				if " " in topobase:
					topotokens.append(topobase.replace(" ", '|'))
					#contextlist.append(topobase.replace(" ", '|'))
					contextlist.append([topobase.replace(" ", '|'), "MainTopo", 0])
				
				#print topotokens

				totaldict = Counter()
				contrib_dict = {}

				gold_lat = float(toporef[j][1]['lat'])
				gold_long = float(toporef[j][1]['long'])

				BaseQuery = "SELECT u1.gid::integer, sum(u1.stat) FROM ( "

				wordlist = []

				other_topo_list = []
				main_topo_list = []

				contextlist2 = copy.deepcopy(contextlist)

				for word in contextlist2:
					if word[0] not in stopwords:
						table = getCorrectTable(word[0], tab1, tab2, tab3)
						if len(table) > 0:
							if word[1] == "MainTopo":
								main_topo_list.append(word[0])
							elif word[1] == "OtherTopo":
								other_topo_list.append(word[0])

				for word in contextlist2:
					if word[0] in main_topo_list and word[1] == "OtherTopo":
						del contextlist[contextlist.index(word)]
					if other_word_weight <= 0.0:
						if word[1] == "Word":
							del contextlist[contextlist.index(word)]

				'''other_topo_list = []
				main_topo_list = []

				for word in contextlist:
					if word[0] not in stopwords:
						table = getCorrectTable(word[0], tab1, tab2, tab3)
						if len(table) > 0:
							if word[1] == "MainTopo":
								main_topo_list.append(word[0])
							elif word[1] == "OtherTopo":
								other_topo_list.append(word[0])

				print "Main Topos:", main_topo_list
				print "Other Topos in Context:", other_topo_list'''

				for word in contextlist:
					if word[0] not in stopwords:
						table = getCorrectTable(word[0], tab1, tab2, tab3)
						if len(table) > 0:
							#Apply different weights to the Gi* vectors of different types of words
							if word[1] == "MainTopo":
								weight = main_topo_weight
							elif word[1] == "OtherTopo":
								weight = other_topo_weight
							elif word[1] == "Word":
								weight = other_word_weight
		 					else: weight = 1.0

		 					wordlist.append(word[0])

		 					if len(wordlist) == 1:
		 						Nested_Select = "SELECT gid, word, stat * %s * %s as stat FROM %s where %s.word = %s " % (str(weight), str(out_corp_lamb), table, table, '%s')
		 					else:
		 						Nested_Select = "\r\n UNION \r\n SELECT gid, word, stat * %s * %s as stat FROM %s where %s.word = %s " % (str(weight), str(out_corp_lamb), table, table, '%s')

		 					if in_domain_stat_tbl != "None":
		 						Nested_Select = Nested_Select + "\r\n UNION \r\n SELECT gid, word, stat * %s * %s as stat FROM %s where %s.word = %s " % (str(weight), str(in_corp_lamb), in_domain_stat_tbl, in_domain_stat_tbl, '%s')
		 						wordlist.append(word[0])
		 					BaseQuery = BaseQuery + Nested_Select
		 		QueryEnd = ") as u1 Group by gid Order by sum(u1.stat) desc;"

				TotalQuery = BaseQuery + QueryEnd
				cur.execute(TotalQuery, wordlist)

				sorted_total = cur.fetchall()

				y = 0
				rank_dict = {}
				#ranked_contrib = sorted(contrib_dict.items(), key=itemgetter(1), reverse=True)
				for t in sorted_total:
					y += 1 
					#contrib_sub = sorted(contrib_dict[t[0]], key=itemgetter(1), reverse=True)
					rank_dict[t[0]] = [y, t[1], lat_long_lookup[t[0]]]

				y = 0

				for i in sorted_total[:5]:
					y += 1
					#print rank_dict[i[0]]
					if y == 1:
						if topobase not in gazet_topos:
							gazet_topos.append(topobase)
						if toporef[j][0] not in gazet_topos:
							gazet_topos.append(toporef[j][0])
						gazet_entry = GetGazets(cur, topotokens, rank_dict[i[0]][2], country_tbl, region_tbl, state_tbl, geonames_tbl, country_alt, region_alt, state_alt, pplc_alt)
						poly_results = []
						tbl = "No Tbl Match"
						gid = 0
						#print "Gazet Entry: ", gazet_entry
						if len(gazet_entry) > 0:
							#print "Gazet Entry: ", gazet_entry
							if len(gazet_entry) == 1:
								#print "Executing Distance SQL for ", gazet_entry
								gid = int(gazet_entry[0][1])
								tbl = gazet_entry[0][0]
								name = gazet_entry[0][2]
								centroid_lat = gazet_entry[0][3]
								centroid_long = gazet_entry[0][4]
								method = "GAZET"
								SQL_dist = "SELECT ST_Distance(ST_GeographyFromText('SRID=4326;POINT(%s %s)'), p2.geog) FROM %s as p2 where p2.gid = %s;" % (gold_long, gold_lat, tbl, '%s')
								cur.execute(SQL_dist, (gid, ))
								poly_results = cur.fetchall()
							else: 
								print "@!@!@!@!@ More than one match found in gazet, error in gazet resolve logic @!@!@!@!@"
								#print gazet_entry
						else:
							method = "TOPO_ESTIMATE"
							gid = i[0]
							name = topobase
							tbl = u"N/A"
							centroid_lat = lat_long_lookup[i[0]][0]
							centroid_long = lat_long_lookup[i[0]][1]
						SQL_Point_Dist = "SELECT ST_Distance(ST_GeographyFromText('SRID=4326;POINT(%s %s)'),ST_GeographyFromText('SRID=4326;POINT(%s %s)'));" % (gold_long, gold_lat, rank_dict[i[0]][2][1], rank_dict[i[0]][2][0])
						cur.execute(SQL_Point_Dist, ())
						point_results = cur.fetchall()
						pointdist = point_results[0][0]
						pointdist = pointdist/1000.0

						if len(poly_results) > 0:
							polydist = (poly_results[0][0]/1000.0)
						else: polydist = pointdist

						print "Point Dist:", pointdist
						print "Poly Dist:", polydist

						point_dist_list.append(pointdist)
						poly_dist_list.append(polydist)

						poly_cor = "0"
						point_cor = "0"

						if pointdist <= 161.0:
							point_total_correct += 1
							point_ner_tp += 1
							point_cor = "1"
						else:
							point_incorrect += 1 
							point_ner_fp += 1
						if polydist <= 161.0:
							poly_total_correct += 1
							poly_cor = "1"
							poly_ner_tp += 1
						else: 
							poly_ner_fp += 1
							poly_incorrect += 1

						predictions.append([toporef[j][0], xml_file, j, method, tbl, gid, name, centroid_lat, centroid_long, pointdist, polydist, point_cor, poly_cor] )

						point_error += pointdist
						poly_error += polydist

						print "Total Topo:", total_topo

						print rank_dict[i[0]]
						print predictions[-1]
		else:
			print "NER False-Positive error: ", j, topobase
			point_ner_fp += 1
			poly_ner_fp += 1
	#Check for false negatives in the NER
	for j in topoall:
		if j in toporef2 and topoall[j] == toporef2[j]:
			pass
		else:
			print "NER False-Negative error:", j, topoall[j]
			ner_fn += 1
	print "Total Attempted: ", total_attempted
	print "Total_Topo:", total_topo
	print "Poly Precision: ", float(poly_ner_tp) / float(total_attempted)
	print "Poly Recall: ", float(poly_ner_tp) / float(total_topo)
	
	return point_ner_fp, poly_ner_fp, ner_fn, point_ner_tp, poly_ner_tp, predictions, total_topo, point_error, poly_error, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct, point_incorrect, poly_incorrect, total_attempted

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el

def GetGazets(cur, placenames, latlong, country_tbl, region_tbl, state_tbl, geonames_tbl, country_alt, region_alt, state_alt, pplc_alt):
	names = tuple(x for x in placenames)
	#print names
	print placenames
	gazet_entry = []
	ranked_gazet = []
	cntry_gid_list = list()
	cntry_gid_list.extend(flatten([country_alt.get(g) for g in placenames if g in country_alt]))
	cntry_gid_list.extend([-99])
	region_gid_list = list()
	region_gid_list.extend(flatten([region_alt.get(g) for g in placenames if g in region_alt]))
	region_gid_list.extend([-99])
	state_gid_list = list()
	state_gid_list.extend(flatten([state_alt.get(g) for g in placenames if g in state_alt]))
	state_gid_list.extend([-99])
	pplc_gid_list = list()
	#print datetime.datetime.now()
	pplc_gid_list.extend(flatten(pplc_alt.get(g, -99) for g in placenames))
	#print datetime.datetime.now()
	#print "PPL gids: ", len(pplc_gid_list)
	pplc_gid_list.extend([-99])
	#print cntry_gid_list
	SQL1 = "SELECT p1.gid, p1.name, ST_Distance(p1.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p1 WHERE p1.name IN %s or p1.gid IN %s or p1.postal IN %s or p1.abbrev IN %s or p1.name_long IN %s;" % (latlong[1], latlong[0], country_tbl, '%s', '%s', '%s', '%s', '%s')
	SQL2 = "SELECT p2.gid, p2.name, ST_Distance(p2.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p2 WHERE p2.name in %s or p2.gid in %s;" % (latlong[1], latlong[0], region_tbl, '%s', '%s')
	SQL3 = "SELECT p3.gid, p3.name, ST_Distance(p3.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p3 WHERE p3.name in %s or p3.gid in %s or p3.abbrev in %s or p3.postal in %s;" % (latlong[1], latlong[0], state_tbl, '%s', '%s', '%s', '%s')
	SQL4 = "SELECT p4.gid, p4.name, ST_Distance(p4.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p4 WHERE p4.name in %s or p4.asciiname in %s or p4.gid in %s;" % (latlong[1], latlong[0], geonames_tbl, '%s', '%s', '%s')
	#print "Got here"
	#print SQL1
	#print "Countries"

	cur.execute(SQL1, (names, tuple(cntry_gid_list), names, names, names))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([country_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	#print "States"
	cur.execute(SQL2, (names, tuple(region_gid_list)))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([region_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	#print "Regions"
	cur.execute(SQL3, (names, tuple(state_gid_list), names, names))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([state_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	#print "PPL"
	cur.execute(SQL4, (names, names, tuple(pplc_gid_list)))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([geonames_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	
	if len(gazet_entry) >= 1:
		ranked_gazet = sorted(gazet_entry, key=itemgetter(3), reverse=False)
		SQL_Centroid = "SELECT p5.gid, ST_Y(ST_Centroid(p5.geog::geometry)), ST_X(ST_Centroid(p5.geog::geometry)) FROM %s as p5 WHERE p5.gid = %s;" % (ranked_gazet[0][0] ,'%s')
		cur.execute(SQL_Centroid, (ranked_gazet[0][1], ))
		results = cur.fetchall()[0]
		latlong = [results[1], results[2]]
		return [[ranked_gazet[0][0], ranked_gazet[0][1], ranked_gazet[0][2], latlong[0], latlong[1]]]
	return gazet_entry