import sys
import os

import io
import psycopg2
import math

import GazetTools

from collections import Counter
from operator import itemgetter

import datetime

import collections

import UnicodeBlocks
import re

#For use with Tr-ConLL
from feature_tools import parse_xml
from feature_tools import getContext

def calc(in_domain_stat_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile,
		 main_topo_weight, other_topo_weight, other_word_weight, country_tbl, region_tbl,
		 state_tbl, geonames_tbl, in_corp_lamb, out_corp_lamb, results_file, stan_path):

	print "In Domain Local Statistics Table Name: ", in_domain_stat_tbl
	print "Out of domain Local Statistics Table Name: ", out_domain_stat_tbl
	print "Directory containg TR-CoNLL format xml files to tested against", test_xml
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

	#Intitialize a Ditionary that links GlobalGrid gid values to Latitude/Longitudes
	lat_long_lookup = {}
	SQL2 = "SELECT gid, ST_Y(geog::geometry), ST_X(geog::geometry) from %s ;" % gtbl
	cur.execute(SQL2)
	lat_long_lookup = dict([(g[0], [g[1],g[2]]) for g in cur.fetchall()])

	point_total_correct = 0
	start_time = datetime.datetime.now()

	#Test xml should always be a directory name. System currently only supports directory as an argument, though may change in the future.
	if os.path.isdir(test_xml) == True:
		print "Reading as directory"
		predictions = []
		point_bigerror = []
		poly_bigerror = []
		point_dist_list = []
		poly_dist_list = []
		total_topo = 0
		point_error_sum = 0.0
		poly_error_sum = 0.0
		error_sum2 = 0.0
		poly_dist = 0.0
		m = 0

		cntry_alt, state_alt, region_alt, pplc_alt = GazetTools.buildAltGazet(cur, country_tbl, region_tbl, state_tbl, geonames_tbl)

		for xml in os.listdir(test_xml):
			m += 1
			print xml
			print "Left to go: ", len(files) - m
			print "Total Toponyms ", total_topo
			#Assumes tr-conll xml format
			toporef, wordref = parse_xml(os.path.join(test_xml, xml))

			point_error, poly_error, total_topo, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct = VectorSum(wordref, toporef, total_topo, cur, lat_long_lookup,  
										percentile, window, stopwords, main_topo_weight, other_topo_weight, other_word_weight, plaintext, predictions, country_tbl, region_tbl, state_tbl,
	 									geonames_tbl, cntry_alt, region_alt, state_alt, pplc_alt, in_domain_stat_tbl, in_corp_lamb, out_corp_lamb)
		point_dist_list.sort()
		poly_dist_list.sort()

		print "=============RESULTS================"
		print "Total Toponyms: ", total_topo
		print "Window: ", window
		print "Percentile: ", percentile
		print "Main Topo weight:", main_topo_weight
		print "Other Topo weight:", other_topo_weight
		print "Other word weight:", other_word_weight
		print "Point Average Error Distance @ 1: ", ((float(point_error_sum)/float(total_topo)))
		print "Point Median Error Distance @ 1: ", point_dist_list[total_topo/2]
		print "Point Accuracy @ 161km : ", float(point_total_correct) / float(total_topo)
		print "Polygon (GAZET) Average Error Distance @ 1: ", ((float(poly_error_sum)/float(total_topo)))
		print "Polygon (GAZET) Median Error Distance @ 1: ", poly_dist_list[total_topo/2]
		print "Polygon (GAZET) Accuracy @ 161km : ", float(poly_total_correct) / float(total_topo)

#Performs actual disambiguation work based on summing Gi* vectors of words in a context window
def VectorSum(wordref, toporef, total_topo, cur, lat_long_lookup, percentile, window, stopwords, main_topo_weight, other_topo_weight, 
	other_word_weight, plaintext_file, predictions, country_tbl, region_tbl, state_tbl,
	geonames_tbl, country_alt, region_alt, state_alt, pplc_alt, in_domain_stat_tbl, in_corp_lamb, out_corp_lamb): 

	#Lists of alphabetic characters that help system decide which table partition to query in later steps
	tab1 = [chr(item) for item in range(ord('a'), ord('i')+1)]
	tab2 = [chr(item) for item in range(ord('j'), ord('s')+1)]
	tab3 = [chr(item) for item in range(ord('t'), ord('z')+1)]
	#Loop through all toponyms found in document
	for j in toporef:
		#print "=======Vector Sum=============="
		total_topo += 1
		topobase = toporef[j]
		print "=========="
		print topobase
		topotokens = []
		#Get all words in a context window around the toponym's index
		contextlist = getContext(wordref, j, window, stopwords, toporef)

		#This section attempts to enforce regularity in case. Attempt to force title case on all place names, except for acronyms
		if topobase.title() != topobase and (len(toporef[j]) != 2 and len(toporef[j]) != 3):
			#contextlist.append(topobase.title())
			contextlist.append([topobase.title(), "MainTopo", 0])
			topobase = topobase.title()

		#Change acronyms with periods into regular acronyms
		if ". " not in topobase.strip():
			no_period_topobase = re.sub(r"\b([A-Z])\.", r"\1", topobase)
			if topobase != no_period_topobase:
				topotokens.append(no_period_topobase)
				contextlist.append([no_period_topobase, "MainTopo", 0])
			else:
				topotokens.append(topobase)
		else: topotokens.append(topobase)

		gazet_topos = topotokens
		#Change spaces in xml read toponym to vertical bar so can match in database
		if " " in topobase:
			topotokens.append(topobase.replace(" ", '|'))
			#contextlist.append(topobase.replace(" ", '|'))
			contextlist.append([topobase.replace(" ", '|'), "MainTopo", 0])

		totaldict = Counter()
		contrib_dict = {}
		wordlist = []

		gold_lat = float(toporef[j][1]['lat'])
		gold_long = float(toporef[j][1]['long'])

		BaseQuery = "SELECT u1.gid::integer, sum(u1.stat) FROM ( "

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

 					BaseQuery = BaseQuery + Nested_Select
 		QueryEnd = ") as u1 Group by gid Order by sum(u1.stat) desc;"

		TotalQuery = BaseQuery + QueryEnd
		#print TotalQuery
		#print wordlist
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
				if toporef[j] not in gazet_topos:
					gazet_topos.append(toporef[j])
				gazet_entry = GazetTools.GetGazetMatches(cur, topotokens, rank_dict[i[0]][2], country_tbl, region_tbl, state_tbl, geonames_tbl, country_alt, region_alt, state_alt, pplc_alt)
				poly_results = []
				tbl = "No Tbl Match"
				gid = 0
				#print "Gazet Entry: ", gazet_entry
				if len(gazet_entry) > 0:
					#print "Gazet Entry: ", gazet_entry
					if len(gazet_entry) == 1:
						#print "Executing Distance SQL for ", gazet_entry
						gid = int(gazet_entry[0][1])
						tbl = gazet_entry[0][0] #Gazet Table
						name = gazet_entry[0][2]
						centroid_lat = gazet_entry[0][3]
						centroid_long = gazet_entry[0][4]
						predictions.append([toporef[j], plaintext_file, j, "GAZET", tbl, gid, name, centroid_lat, centroid_long, gold_lat, gold_long ] )
						SQL_dist = "SELECT ST_Distance(ST_GeographyFromText('SRID=4326;POINT(%s %s)'), p2.geog) FROM %s as p2 WHERE p2.gid = %s;" % (gold_long, gold_lat, tbl, '%s')
						#SQL_Poly_dist = "SELECT ST_Distance(p1.polygeog, p2.geog) FROM trconllf as p1, %s as p2 WHERE p1.placename = %s and p1.docname = %s and p2.gid = %s;" % (tbl, '%s', '%s', '%s')
						#cur.execute(SQL_Point_dist, (toporef[j][0], xml, gid))
						#point_results = cur.fetchall()
						cur.execute(SQL_dist, (gid, ))
						poly_results = cur.fetchall()
					else: 
						print "@!@!@!@!@ More than one match found in gazet, error in gazet resolve logic @!@!@!@!@"
						#print gazet_entry
				else:
					predictions.append([toporef[j], plaintext_file, j, "TOPO_ESTIMATE", tbl, i[0], toporef[j], lat_long_lookup[i[0]][0], lat_long_lookup[i[0]][1], gold_lat, gold_long ] )
					SQL_Point_Dist = "SELECT ST_Distance(ST_GeographyFromText('SRID=4326;POINT(%s %s)'), ST_GeographyFromText('SRID=4326;POINT(%s %s)'));" % (gold_long, gold_lat, rank_dict[i[0]][2][1], rank_dict[i[0]][2][0])
					cur.execute(SQL_Point_Dist, ())
					point_results = cur.fetchall()
					pointdist = point_results[0][0]
					pointdist = pointdist / float(1000)

				if len(poly_results) > 0:
					polydist = (poly_results[0][0] / float(1000))
				else: polydist = pointdist

				print "Point Dist: ", pointdist
				print "Poly Dist: ", polydist


				point_dist_list.append(pointdist)
				poly_dist_list.append(polydist)
				if pointdist <= 161.0:
					point_total_correct += 1
				if polydist <= 161.0:
					poly_total_correct += 1
				point_error += pointdist
				poly_error += polydist
				print "Total Topo:", total_topo

				print rank_dict[i[0]]
				print predictions[-1]

	return point_error, poly_error, total_topo, point_dist_list, poly_dist_list, point_total_correct, poly_total_correct



