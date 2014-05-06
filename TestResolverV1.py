import sys
import os


import psycopg2
import xml.etree.ElementTree as ET
import math

from collections import Counter
from operator import itemgetter

def parse_xml(afile):
	xmldoc = ET.parse(afile)
	root = xmldoc.getroot()

	wordref = {}
	toporef = {}
	i = 0
	
	print root.tag
	print root.attrib
	for child in root.iter('s'):
		#print child.attrib
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
			contextlist.append(wordref[j])
	#print len(contextlist)
	j = i
	while j < len(wordref):
		j = j + 1
		if i + window < j:
			break
		if wordref[j] not in stopwords:
			contextlist.append(wordref[j])
	return contextlist

def updateInPlace(a,b):
	a.update(b)
	return a

def calc(stat_tbl, test_xml, conn_info, gtbl, window, percentile, place_name_weight, country_tbl, region_tbl, state_tbl, US_Prominent_tbl, Wrld_Prominent_tbl):
	print "Local Statistics Table Name: ", stat_tbl
	print "Test XML directory/file path: ", test_xml
	print "DB conneciton info: ", conn_info
	print "Grid table used: ", gtbl
	print "Window size", window
	print "Percentile: ", percentile
	print "Place name weight: ", place_name_weight

	print "Country table name: ", country_tbl
	print "Region table name: ", region_tbl
	print "State table name: ", state_tbl

	conn = psycopg2.connect(conn_info)
	print "Connection Success"

	stopwords = set(['.',',','(',')','-','=',";",':',"'",'"','$','the','a','an','that','this',
					'to', 'be', 'have', 'has', 'is', 'are', 'was', 'am', "'s",
					'and', 'or','but',
					'by', 'of', 'from','in','after','on','for', 'TO',
					'I', 'me', 'he', 'him', 'she', 'her', 'we', 'us', 'you', 'they', 'them', 'their', 'it'])

	cur = conn.cursor()

	lat_long_lookup = {}
	SQL2 = "SELECT gid, ST_Y(geog::geometry), ST_X(geog::geometry) from %s ;" % gtbl
	cur.execute(SQL2)
	lat_long_lookup = dict([(g[0], [g[1],g[2]]) for g in cur.fetchall()])
	print len(lat_long_lookup)

	if os.path.isdir(test_xml) == True:
		print "Reading as directory"
		files = os.listdir(test_xml)
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
		for xml in files:
			m += 1
			print xml
			print "Left to go: ", len(files) - m
			print "Total Toponyms ", total_topo
			wordref, toporef = parse_xml(test_xml + '/' + xml)
			point_error_sum, poly_error_sum, total_topo, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list = VectorSum(wordref, toporef, total_topo, point_error_sum, poly_error_sum, cur, lat_long_lookup, stat_tbl, 
				percentile, window, stopwords, place_name_weight, xml, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, country_tbl, region_tbl, state_tbl, US_Prominent_tbl, Wrld_Prominent_tbl)
			#error_sum2 = MostOverlap(wordref, toporef, error_sum2, cur, lat_long_lookup, stat_tbl, percentile, window, stopwords, place_name_weight, xml)
		point_dist_list.sort()
		poly_dist_list.sort()
		#print "============Most Overlap==============="
		#print "Total Toponyms: ", total_topo
		#print "Window: ", window
		#print "Percentile: ", percentile
		#print "Place name weight:", place_name_weight
		#print "Average Error Distance @ 1: ", (float(error_sum2)/float(total_topo))
		for thing in point_bigerror:
			print "Point Big Errors"
			print thing
		for thing in poly_bigerror:
			print "Poly Big Errors"
			print thing
		print "=============Vector Sum================"
		print "Total Toponyms: ", total_topo
		print "Window: ", window
		print "Percentile: ", percentile
		print "Place name weight:", place_name_weight
		print "Point Average Error Distance @ 1: ", ((float(point_error_sum)/float(total_topo)))
		print "Point Median Error Distance @ 1: ", point_dist_list[total_topo/2]
		print "Polygon Average Error Distance @ 1: ", ((float(poly_error_sum)/float(total_topo)))
		print "Polygon Median Error Distance @ 1: ", poly_dist_list[total_topo/2]
		conn.close()
	elif os.path.isdir(test_xml) == False:
		print "Reading as file"
		wordref, toporef = parse_xml(test_xml)
		simplefile = test_xml[test_xml.rfind('/')+1:]
		point_bigerror = []
		poly_bigerror = []
		point_dist_list = []
		poly_dist_list = []
		total_topo = 0
		point_error_sum = 0.0
		poly_error_sum = 0.0
		error_sum2 = 0.0
		point_error_sum, poly_error_sum, total_topo, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list = VectorSum(wordref, toporef, total_topo, point_error_sum, poly_error_sum, cur, lat_long_lookup, 
			stat_tbl, percentile, window, stopwords, place_name_weight, xml, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, US_Prominent_tbl, Wrld_Prominent_tbl)
		point_dist_list.sort()
		poly_dist_list.sort()
		#error_sum2 = MostOverlap(wordref, toporef, error_sum2, cur, lat_long_lookup, stat_tbl, percentile, window, stopwords, place_name_weight, simplefile)
		#print "============Most Overlap==============="
		#print "Total Toponyms: ", total_topo
		#print "Window: ", window
		#print "Percentile: ", percentile
		#print "Place name weight:", place_name_weight
		#print "Average Error Distance @ 1: ", ((float(error_sum2)/float(total_topo)))
		for thing in point_bigerror:
			print "Point Big Errors"
			print thing
		for thing in poly_bigerror:
			print "Poly Big Errors"
			print thing
		print "=============Vector Sum================"
		print "Total Toponyms: ", total_topo
		print "Window: ", window
		print "Percentile: ", percentile
		print "Place name weight:", place_name_weight
		print "Point Average Error Distance @ 1: ", ((float(point_error_sum)/float(total_topo)))
		print "Point Median Error Distance @ 1: ", point_dist_list[total_topo/2]
		print "Polygon Average Error Distance @ 1: ", ((float(poly_error_sum)/float(total_topo)))
		print "Polygon Median Error Distance @ 1: ", poly_dist_list[total_topo/2]
		conn.close()


def VectorSum(wordref, toporef, total_topo, point_error, poly_error, cur, lat_long_lookup, stat_tbl, percentile, window, stopwords, place_name_weight, xml, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, country_tbl, region_tbl, state_tbl, US_Prominent_tbl, Wrld_Prominent_tbl):
	for j in toporef:
		print "=======Vector Sum=============="
		total_topo += 1
		topobase = toporef[j][0]
		print topobase
		topotokens = []
		contextlist = getContext(wordref, j, window, stopwords)
		#This section attempts to enforce regularity in case. Attempt to force title case on all place names, except for acronyms
		if topobase.title() != topobase and len(toporef[j][0]) != 2:
			contextlist.append(topobase.title())
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
				topotokens.append(token)
				contextlist.append(token)
			#topotokens.append(topobase.replace('.', ''))
			topotokens.append(combinedtokens)
			contextlist.append(combinedtokens)
		else: topotokens.append(topobase)
		gazet_topos = topotokens
		if " " in topobase:
			topotokens.append(topobase.replace(" ", '|'))
			contextlist.append(topobase.replace(" ", '|'))
			#for token in topobase.split(" "):
			#	topotokens.append(token)
			#	contextlist.append(token)
		gold_lat = float(toporef[j][1]['lat'])
		gold_long = float(toporef[j][1]['long'])
		
		#print contextlist
		totaldict = Counter()
		contrib_dict = {}
		print toporef[j][0], float(toporef[j][1]['lat']), float(toporef[j][1]['long'])
		for word in contextlist:
			if word not in stopwords:
				SQL = "Select gid, stat FROM %s WHERE word = %s ;" % (stat_tbl, '%s')
				cur.execute(SQL, (word, ))
				if word in topotokens:
					weight = place_name_weight
				else: weight = 1.0
				adict =  dict([(int(k), weight * float(v)) for k, v in cur.fetchall()])
				ranked_fetch = sorted(adict.items(), key=itemgetter(1), reverse=True)
				subset_ranked = dict(ranked_fetch[:int(len(ranked_fetch)*percentile)])
				for gid in subset_ranked:
					#print gid
					contrib_dict.setdefault(gid, list()).append([word, subset_ranked[gid]])
					#contrib_dict[gid] = combine_tuples(contrib_dict.get(gid, (word, 0.0)), gid)
				#print word, ranked_fetch[:5]
				totaldict += Counter(subset_ranked)
		#print totaldict
		sorted_total = sorted(totaldict.items(), key=itemgetter(1), reverse=True)
		y = 0
		rank_dict = {}
		#ranked_contrib = sorted(contrib_dict.items(), key=itemgetter(1), reverse=True)
		for t in sorted_total:
			y += 1 
			contrib_sub = sorted(contrib_dict[t[0]], key=itemgetter(1), reverse=True)
			rank_dict[t[0]] = [y, t[1], lat_long_lookup[t[0]], math.fabs(lat_long_lookup[t[0]][0]-gold_lat)+math.fabs(lat_long_lookup[t[0]][1]-gold_long), contrib_sub[:5]]

		#print sorted_total[:20]
		y = 0
		for i in sorted_total[:20]:
			y += 1
			#print rank_dict[i[0]]
			if y == 1:
				if topobase not in gazet_topos:
					gazet_topos.append(topobase)
				if toporef[j][0] not in gazet_topos:
					gazet_topos.append(toporef[j][0])
				gazet_entry = GetGazets(cur, topotokens, rank_dict[i[0]][2], country_tbl, region_tbl, state_tbl, US_Prominent_tbl, Wrld_Prominent_tbl)
				poly_results = []
				tbl = "No Tbl Match"
				#print "Gazet Entry: ", gazet_entry
				if len(gazet_entry) > 0:
					print "Gazet Entry: ", gazet_entry
					if len(gazet_entry) == 1:
						print "Executing Distance SQL for ", gazet_entry
						gid = int(gazet_entry[0][1])
						tbl = gazet_entry[0][0]
						name = gazet_entry[0][2]
						SQL_Poly_dist = "SELECT ST_Distance(p1.pointgeog, p2.geog) FROM trconllf as p1, %s as p2 WHERE p1.placename = %s and p1.docname = %s and p2.gid = %s;" % (tbl, '%s', '%s', '%s')
						#SQL_Poly_dist = "SELECT ST_Distance(p1.polygeog, p2.geog) FROM trconllf as p1, %s as p2 WHERE p1.placename = %s and p1.docname = %s and p2.gid = %s;" % (tbl, '%s', '%s', '%s')
						#cur.execute(SQL_Point_dist, (toporef[j][0], xml, gid))
						#point_results = cur.fetchall()
						cur.execute(SQL_Poly_dist, (toporef[j][0], xml, gid))
						poly_results = cur.fetchall()
					else: 
						print "@!@!@!@!@ More than one match found in gazet, error in gazet resolve logic @!@!@!@!@"
						#print gazet_entry
				SQL_Point_Dist = "SELECT ST_Distance(p1.pointgeog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM trconllf as p1 WHERE p1.placename = %s and p1.docname = %s;" % (rank_dict[i[0]][2][1], rank_dict[i[0]][2][0], '%s', '%s')
				#SQL_Poly_Dist = "SELECT ST_Distance(p1.polygeog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM trconllf as p1 WHERE p1.placename = %s and p1.docname = %s and p1.polygeog IS NOT NULL;" % (rank_dict[i[0]][2][1], rank_dict[i[0]][2][0], '%s', '%s')
				cur.execute(SQL_Point_Dist, (toporef[j][0], xml))
				point_results = cur.fetchall()
				#cur.execute(SQL_Poly_Dist, (toporef[j][0], xml))
				#poly_results = cur.fetchall()
				#print toporef[j][0]
				#print xml
				print rank_dict[i[0]]
				#print rank_dict[i[0]][2][1], rank_dict[i[0]][2][0]

				#print results
				pointdist = point_results[0][0]
				pointdist = pointdist / float(1000)

				if len(poly_results) > 0:
					polydist = (poly_results[0][0] / float(1000))
				else: polydist = pointdist

				print "Point Dist: ", pointdist
				print "Poly Dist: ", polydist

				point_dist_list.append(pointdist)
				poly_dist_list.append(polydist)
				if pointdist > 1000.0:
					point_bigerror.append([toporef[j][0], pointdist, tbl, [gold_lat, gold_long], rank_dict[i[0]][2], rank_dict[i[0]][4]])
				if polydist > 1000.0:
					poly_bigerror.append([toporef[j][0], polydist, tbl, [gold_lat, gold_long], rank_dict[i[0]][2], rank_dict[i[0]][4]])
				point_error += pointdist
				poly_error += polydist
		#print "====================="
		#print len(contextlist)
	return point_error, poly_error, total_topo, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list

def GetGazets(cur, placenames, latlong, country_tbl, region_tbl, state_tbl, US_Prominent_tbl, Wrld_Prominent_tbl):
	names = tuple(x for x in placenames)
	print names
	gazet_entry = []
	ranked_gazet = []
	SQL1 = "SELECT p1.gid, p1.name, ST_Distance(p1.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p1 WHERE p1.name IN %s or p1.fips_cntry IN %s or p1.gmi_cntry IN %s or p1.locshrtnam IN %s;" % (latlong[1], latlong[0], country_tbl, '%s', '%s', '%s', '%s')
	SQL2 = "SELECT p2.gid, p2.name, ST_Distance(p2.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p2 WHERE p2.name in %s;" % (latlong[1], latlong[0], region_tbl, '%s')
	SQL3 = "SELECT p3.gid, p3.name, ST_Distance(p3.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p3 WHERE p3.name in %s;" % (latlong[1], latlong[0], state_tbl, '%s')
	SQL4 = "SELECT p4.gid, p4.name, ST_Distance(p4.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p4 WHERE p4.name in %s;" % (latlong[1], latlong[0], US_Prominent_tbl, '%s')
	SQL5 = "SELECT p5.gid, p5.name, ST_Distance(p5.geog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p5 WHERE p5.name in %s;" % (latlong[1], latlong[0], Wrld_Prominent_tbl, '%s')
	cur.execute(SQL1, (names, names, names, names))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([country_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	cur.execute(SQL2, (names, ))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([region_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	cur.execute(SQL3, (names, ))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([state_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	cur.execute(SQL4, (names, ))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([US_Prominent_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	cur.execute(SQL5, (names, ))
	returns = cur.fetchall()
	for row in returns:
		gazet_entry.append([Wrld_Prominent_tbl, row[0], row[1], float(row[2])/1000.0])
		#print "!!!Found Gazet Match!!!"
		#print gazet_entry[-1]
	if len(gazet_entry) > 1:
		ranked_gazet = sorted(gazet_entry, key=itemgetter(3), reverse=False)
		return [ranked_gazet[0]]
	return gazet_entry


def MostOverlap(wordref, toporef, point_error, cur, lat_long_lookup, stat_tbl, percentile, window, stopwords, place_name_weight, xml):
	for j in toporef:
		print "========Most Overlap============="
		gold_lat = float(toporef[j][1]['lat'])
		gold_long = float(toporef[j][1]['long'])
		topotokens = [toporef[j][0]]
		if "." in toporef[j][0]:
			for token in toporef[j][0].split("."):
				topotokens.append(token)
		if " " in toporef[j][0]:
			for token in toporef[j][0].split(" "):
				topotokens.append(token)
		contextlist = getContext(wordref, j, window, stopwords)
		if toporef[j][0].title() != toporef[j][0]:
			contextlist.append(toporef[j][0].title())
			topotokens.append(toporef[j][0].title())
		'''if toporef[j][0].lower() != toporef[j][0]:
			contextlist.append(toporef[j][0].lower())
			topotokens.append(toporef[j][0].lower())
		if toporef[j][0].upper() != toporef[j][0]:
			contextlist.append(toporef[j][0].upper())
			topotokens.append(toporef[j][0].upper())'''
		totaldict = Counter()
		print contextlist
		contrib_dict = {}
		print toporef[j][0], float(toporef[j][1]['lat']), float(toporef[j][1]['long'])
		for word in contextlist:
			if word not in stopwords:
				SQL = "Select gid, stat FROM %s WHERE word = %s ;" % (stat_tbl, '%s')
				cur.execute(SQL, (word, ))
				if word in topotokens:
					weight = place_name_weight
				else: weight = 1.0
				adict =  dict([(int(k), weight * float(v)) for k, v in cur.fetchall()])
				ranked_fetch = sorted(adict.items(), key=itemgetter(1), reverse=True)
				subset_ranked = dict([(row[0], weight * 1.0) for row in ranked_fetch[:int(len(ranked_fetch)*percentile)]])
				for gid in subset_ranked:
					#print gid
					contrib_dict.setdefault(gid, list()).append([word, subset_ranked[gid]])
					#contrib_dict[gid] = combine_tuples(contrib_dict.get(gid, (word, 0.0)), gid)
				#print word, ranked_fetch[:5]
				totaldict += Counter(subset_ranked)
		#print totaldict
		sorted_total = sorted(totaldict.items(), key=itemgetter(1), reverse=True)
		y = 0
		rank_dict = {}
		#ranked_contrib = sorted(contrib_dict.items(), key=itemgetter(1), reverse=True)
		for t in sorted_total:
			y += 1 
			contrib_sub = sorted(contrib_dict[t[0]], key=itemgetter(1), reverse=True)
			rank_dict[t[0]] = [y, t[1], lat_long_lookup[t[0]], math.fabs(lat_long_lookup[t[0]][0]-gold_lat)+math.fabs(lat_long_lookup[t[0]][1]-gold_long), contrib_sub[:5]]

		#print sorted_total[:20]
		y = 0
		for i in sorted_total[:5]:
			y += 1
			if y == 1:
				print rank_dict[i[0]]
				SQL_Dist = "SELECT ST_Distance(p1.pointgeog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM trconllf as p1 WHERE p1.placename = %s and p1.docname = %s;" % (rank_dict[i[0]][2][1], rank_dict[i[0]][2][0], '%s', '%s')
				cur.execute(SQL_Dist, (toporef[j][0], xml))
				results = cur.fetchall()
				pointdist = results[0][0]
				print pointdist / float(1000)
				point_error += (pointdist / float(1000))
		print "====================="
		#print len(contextlist)
	return point_error

def combine_tuples(t1, t2):
	tsum = t1[1] + t2[1]
	return (t1[0], tsum)