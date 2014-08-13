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

def block(ch):
  '''
  Return the Unicode block name for ch, or None if ch has no block.

  >>> block(u'a')
  'Basic Latin'
  >>> block(unichr(0x0b80))
  'Tamil'
  >>> block(unichr(0xe0080))

  '''

  assert isinstance(ch, unicode) and len(ch) == 1, repr(ch)
  cp = ord(ch)
  for start, end, name in _blocks:
    if start <= cp <= end:
      return name

def _initBlocks(text):
  global _blocks
  _blocks = []
  import re
  pattern = re.compile(r'([0-9A-F]+)\.\.([0-9A-F]+);\ (\S.*\S)')
  for line in text.splitlines():
    m = pattern.match(line)
    if m:
      start, end, name = m.groups()
      _blocks.append((int(start, 16), int(end, 16), name))

# retrieved from http://unicode.org/Public/UNIDATA/Blocks.txt
_initBlocks('''
# Blocks-5.1.0.txt
# Date: 2008-03-20, 17:41:00 PDT [KW]
#
# Unicode Character Database
# Copyright (c) 1991-2008 Unicode, Inc.
# For terms of use, see http://www.unicode.org/terms_of_use.html
# For documentation, see UCD.html
#
# Note:   The casing of block names is not normative.
#         For example, "Basic Latin" and "BASIC LATIN" are equivalent.
#
# Format:
# Start Code..End Code; Block Name

# ================================================

# Note:   When comparing block names, casing, whitespace, hyphens,
#         and underbars are ignored.
#         For example, "Latin Extended-A" and "latin extended a" are equivalent.
#         For more information on the comparison of property values, 
#            see UCD.html.
#
#  All code points not explicitly listed for Block
#  have the value No_Block.

# Property: Block
#
# @missing: 0000..10FFFF; No_Block

0000..007F; Basic Latin
0080..00FF; Latin-1 Supplement
0100..017F; Latin Extended-A
0180..024F; Latin Extended-B
0250..02AF; IPA Extensions
02B0..02FF; Spacing Modifier Letters
0300..036F; Combining Diacritical Marks
0370..03FF; Greek and Coptic
0400..04FF; Cyrillic
0500..052F; Cyrillic Supplement
0530..058F; Armenian
0590..05FF; Hebrew
0600..06FF; Arabic
0700..074F; Syriac
0750..077F; Arabic Supplement
0780..07BF; Thaana
07C0..07FF; NKo
0900..097F; Devanagari
0980..09FF; Bengali
0A00..0A7F; Gurmukhi
0A80..0AFF; Gujarati
0B00..0B7F; Oriya
0B80..0BFF; Tamil
0C00..0C7F; Telugu
0C80..0CFF; Kannada
0D00..0D7F; Malayalam
0D80..0DFF; Sinhala
0E00..0E7F; Thai
0E80..0EFF; Lao
0F00..0FFF; Tibetan
1000..109F; Myanmar
10A0..10FF; Georgian
1100..11FF; Hangul Jamo
1200..137F; Ethiopic
1380..139F; Ethiopic Supplement
13A0..13FF; Cherokee
1400..167F; Unified Canadian Aboriginal Syllabics
1680..169F; Ogham
16A0..16FF; Runic
1700..171F; Tagalog
1720..173F; Hanunoo
1740..175F; Buhid
1760..177F; Tagbanwa
1780..17FF; Khmer
1800..18AF; Mongolian
1900..194F; Limbu
1950..197F; Tai Le
1980..19DF; New Tai Lue
19E0..19FF; Khmer Symbols
1A00..1A1F; Buginese
1B00..1B7F; Balinese
1B80..1BBF; Sundanese
1C00..1C4F; Lepcha
1C50..1C7F; Ol Chiki
1D00..1D7F; Phonetic Extensions
1D80..1DBF; Phonetic Extensions Supplement
1DC0..1DFF; Combining Diacritical Marks Supplement
1E00..1EFF; Latin Extended Additional
1F00..1FFF; Greek Extended
2000..206F; General Punctuation
2070..209F; Superscripts and Subscripts
20A0..20CF; Currency Symbols
20D0..20FF; Combining Diacritical Marks for Symbols
2100..214F; Letterlike Symbols
2150..218F; Number Forms
2190..21FF; Arrows
2200..22FF; Mathematical Operators
2300..23FF; Miscellaneous Technical
2400..243F; Control Pictures
2440..245F; Optical Character Recognition
2460..24FF; Enclosed Alphanumerics
2500..257F; Box Drawing
2580..259F; Block Elements
25A0..25FF; Geometric Shapes
2600..26FF; Miscellaneous Symbols
2700..27BF; Dingbats
27C0..27EF; Miscellaneous Mathematical Symbols-A
27F0..27FF; Supplemental Arrows-A
2800..28FF; Braille Patterns
2900..297F; Supplemental Arrows-B
2980..29FF; Miscellaneous Mathematical Symbols-B
2A00..2AFF; Supplemental Mathematical Operators
2B00..2BFF; Miscellaneous Symbols and Arrows
2C00..2C5F; Glagolitic
2C60..2C7F; Latin Extended-C
2C80..2CFF; Coptic
2D00..2D2F; Georgian Supplement
2D30..2D7F; Tifinagh
2D80..2DDF; Ethiopic Extended
2DE0..2DFF; Cyrillic Extended-A
2E00..2E7F; Supplemental Punctuation
2E80..2EFF; CJK Radicals Supplement
2F00..2FDF; Kangxi Radicals
2FF0..2FFF; Ideographic Description Characters
3000..303F; CJK Symbols and Punctuation
3040..309F; Hiragana
30A0..30FF; Katakana
3100..312F; Bopomofo
3130..318F; Hangul Compatibility Jamo
3190..319F; Kanbun
31A0..31BF; Bopomofo Extended
31C0..31EF; CJK Strokes
31F0..31FF; Katakana Phonetic Extensions
3200..32FF; Enclosed CJK Letters and Months
3300..33FF; CJK Compatibility
3400..4DBF; CJK Unified Ideographs Extension A
4DC0..4DFF; Yijing Hexagram Symbols
4E00..9FFF; CJK Unified Ideographs
A000..A48F; Yi Syllables
A490..A4CF; Yi Radicals
A500..A63F; Vai
A640..A69F; Cyrillic Extended-B
A700..A71F; Modifier Tone Letters
A720..A7FF; Latin Extended-D
A800..A82F; Syloti Nagri
A840..A87F; Phags-pa
A880..A8DF; Saurashtra
A900..A92F; Kayah Li
A930..A95F; Rejang
AA00..AA5F; Cham
AC00..D7AF; Hangul Syllables
D800..DB7F; High Surrogates
DB80..DBFF; High Private Use Surrogates
DC00..DFFF; Low Surrogates
E000..F8FF; Private Use Area
F900..FAFF; CJK Compatibility Ideographs
FB00..FB4F; Alphabetic Presentation Forms
FB50..FDFF; Arabic Presentation Forms-A
FE00..FE0F; Variation Selectors
FE10..FE1F; Vertical Forms
FE20..FE2F; Combining Half Marks
FE30..FE4F; CJK Compatibility Forms
FE50..FE6F; Small Form Variants
FE70..FEFF; Arabic Presentation Forms-B
FF00..FFEF; Halfwidth and Fullwidth Forms
FFF0..FFFF; Specials
10000..1007F; Linear B Syllabary
10080..100FF; Linear B Ideograms
10100..1013F; Aegean Numbers
10140..1018F; Ancient Greek Numbers
10190..101CF; Ancient Symbols
101D0..101FF; Phaistos Disc
10280..1029F; Lycian
102A0..102DF; Carian
10300..1032F; Old Italic
10330..1034F; Gothic
10380..1039F; Ugaritic
103A0..103DF; Old Persian
10400..1044F; Deseret
10450..1047F; Shavian
10480..104AF; Osmanya
10800..1083F; Cypriot Syllabary
10900..1091F; Phoenician
10920..1093F; Lydian
10A00..10A5F; Kharoshthi
12000..123FF; Cuneiform
12400..1247F; Cuneiform Numbers and Punctuation
1D000..1D0FF; Byzantine Musical Symbols
1D100..1D1FF; Musical Symbols
1D200..1D24F; Ancient Greek Musical Notation
1D300..1D35F; Tai Xuan Jing Symbols
1D360..1D37F; Counting Rod Numerals
1D400..1D7FF; Mathematical Alphanumeric Symbols
1F000..1F02F; Mahjong Tiles
1F030..1F09F; Domino Tiles
20000..2A6DF; CJK Unified Ideographs Extension B
2F800..2FA1F; CJK Compatibility Ideographs Supplement
E0000..E007F; Tags
E0100..E01EF; Variation Selectors Supplement
F0000..FFFFF; Supplementary Private Use Area-A
100000..10FFFF; Supplementary Private Use Area-B

# EOF
''')

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

def calc(in_domain_stat_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile, main_topo_weight, other_topo_weight, other_word_weight, country_tbl, 
	region_tbl, state_tbl, geonames_tbl, tst_tbl, in_corp_lamb, out_corp_lamb, results_file):
	print "In Domain Local Statistics Table Name: ", in_domain_stat_tbl
	print "Out of domain Local Statistics Table Name: ", out_domain_stat_tbl
	print "Test XML directory/file path: ", test_xml
	print "DB conneciton info: ", conn_info
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

	start_time = datetime.datetime.now()

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

		SQL1 = "SELECT p1.gid, p1.name, p1.name_long, p1.geonames_gid, p1.altnames FROM %s as p1 ;" % country_tbl
		SQl2 = "SELECT p1.gid, p1.name, p1.name_long, p1.geonames_gid, p1.altnames FROM %s as p1 ;" % region_tbl
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
		#Create data structure to contain all median, mean, asccuracy results at each in domain lambda
		lamb_dict_totals = {}
		for i in range(0, 10+1):
			lamb_dict_totals[round((Decimal(i)*Decimal(0.1)), 2)] = {}

		for xml in files:
			m += 1
			print xml
			print "Left to go: ", len(files) - m
			print "Total Toponyms ", total_topo
			wordref, toporef = parse_xml(test_xml+'/'+xml)
			lamb_dict_total, total_topo = VectorSum(wordref, toporef, total_topo, cur, lat_long_lookup,  
				percentile, window, stopwords, place_name_weight, xml, country_tbl, region_tbl, state_tbl,
				 geonames_tbl, tst_tbl, cntry_alt, region_alt, state_alt, pplc_alt, in_domain_stat_tbl, lamb_dict_totals)			
			
		point_dist_list.sort()
		poly_dist_list.sort()
		#print "============Most Overlap==============="
		#print "Total Toponyms: ", total_topo
		#print "Window: ", window
		#print "Percentile: ", percentile
		#print "Place name weight:", place_name_weight
		#print "Average Error Distance @ 1: ", (float(error_sum2)/float(total_topo))
		for in_domain_lamb in lamb_dict_totals:
			point_error_sum = lamb_dict_totals[in_domain_lamb]['point_error']
			poly_error_sum = lamb_dict_totals[in_domain_lamb]['poly_error']
			point_total_correct = lamb_dict_totals[in_domain_lamb]['point_total_correct']
			poly_total_correct = lamb_dict_totals[in_domain_lamb]['poly_total_correct']
			point_dist_list = lamb_dict_totals[in_domain_lamb]['point_dist_list']
			poly_dist_list = lamb_dict_totals[in_domain_lamb]['poly_dist_list']
			print "=============Vector Sum================"
			print "Total Toponyms: ", total_topo
			print "Window: ", window
			print "Percentile: ", percentile
			print "Place name weight:", place_name_weight
			print "Point Average Error Distance @ 1: ", ((float(point_error_sum)/float(total_topo)))
			print "Point Median Error Distance @ 1: ", point_dist_list[total_topo/2]
			print "Point Accuracy @ 161km : ", float(point_total_correct) / float(total_topo)
			print "Polygon Average Error Distance @ 1: ", ((float(poly_error_sum)/float(total_topo)))
			print "Polygon Median Error Distance @ 1: ", poly_dist_list[total_topo/2]
			print "Polygon Accuracy @ 161km : ", float(poly_total_correct) / float(total_topo)
			with open(results_file, 'a') as w:
				w.write("=============Vector Sum================" + '\r\n')
				w.write("In Domain Local Statistics Table Name: " + str(in_domain_stat_tbl) + '\r\n')
				w.write("Out of domain Local Statistics Table Name: " + str(out_domain_stat_tbl) + '\r\n')
				w.write("Test XML directory/file path: " + test_xml + '\r\n')
				w.write("In Domain Corp Lambda: " + str(in_domain_lamb) + '\r\n')
				w.write("Out Domain Corp Lambda: " + str(str((1.0 - float(in_domain_lamb))) + '\r\n'))
				w.write("Window: " + str(window) + '\r\n')
				w.write("Total Toponyms: " + str(total_topo) + '\r\n')
				w.write("Place name weight:"+ str(place_name_weight) + '\r\n')
				w.write("Point Accuracy @ 161km : " +  str(float(point_total_correct) / float(total_topo)) + '\r\n')
				w.write("Point Average Error Distance @ 1: " + str((float(point_error_sum)/float(total_topo))) + '\r\n')
				w.write("Point Median Error Distance @ 1: " + str(point_dist_list[total_topo/2]) + '\r\n')
				w.write("Polygon Average Error Distance @ 1: " + str((float(poly_error_sum)/float(total_topo))) + '\r\n')
				w.write("Polygon Median Error Distance @ 1: " + str(poly_dist_list[total_topo/2]) + '\r\n' )
				w.write("Polygon Accuracy @ 161km : " + str(float(poly_total_correct) / float(total_topo)) + '\r\n' )
		conn.close()

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
			stat_tbl, percentile, window, stopwords, place_name_weight, xml, point_bigerror, poly_bigerror, point_dist_list, poly_dist_list)
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

	end_time = datetime.datetime.now()

	print "Total Time: ", end_time - start_time

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

from collections import defaultdict

def merge4(dicts):
	merged = {}
	for d in dicts:
		for k in d:
			merged[k] = merged.get(k, 0.0) + d[k]
	return merged

def VectorSum(wordref, toporef, total_topo, point_error, poly_error, cur, lat_long_lookup, percentile, window, stopwords, main_topo_weight, other_topo_weight, other_word_weight, xml,
 point_bigerror, poly_bigerror, point_dist_list, poly_dist_list, country_tbl, region_tbl, state_tbl, geonames_tbl, point_total_correct, 
 poly_total_correct, tst_tbl, country_alt, region_alt, state_alt, pplc_alt, in_domain_stat_tbl, in_corp_lamb, out_corp_lamb):
	tab1 = [chr(item) for item in range(ord('a'), ord('i')+1)]
	tab2 = [chr(item) for item in range(ord('j'), ord('s')+1)]
	tab3 = [chr(item) for item in range(ord('t'), ord('z')+1)]
	for j in toporef:
		#print "=======Vector Sum=============="
		total_topo += 1
		topobase = str(toporef[j][0])
		print "=========="
		print topobase
		topotokens = []
		contextlist = getContext(wordref, j, window, stopwords, toporef)
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
		
		#print contextlist
		totaldict = Counter()
		contrib_dict = {}


		for word in contextlist:
			if word[0] not in stopwords:
				table = getCorrectTable(word[0], tab1, tab2, tab3)
				if len(table) > 0:
					if word[1] == "MainTopo":
						weight = main_topo_weight
					elif word[1] == "OtherTopo":
						weight = other_topo_weight
					elif word[1] == "Word":
						weight = other_word_weight
 					else: weight = 1.0
					
					SQL = "Select gid, stat FROM %s WHERE word = %s ;" % (table, '%s')
					cur.execute(SQL, (word[0], ))
					adict =  dict([(int(k), weight * out_corp_lamb * float(v)) for k, v in cur.fetchall()])
					
					if in_domain_stat_tbl != "None":
						SQL2 = "Select gid, stat FROM %s WHERE word = %s ;" % (in_domain_stat_tbl, '%s')
						cur.execute(SQL2, (word[0], ))
						adict2 =  dict([(int(k), weight * in_corp_lamb * float(v)) for k, v in cur.fetchall()])
					else: adict2 = {}
					
					defmergeddict = getAllDicts(indomain_tab_results, outdomain_tab_results, weight, lamb_dict, percentile)
					#lamb_dict_list.append(defmergeddict)

					#print "42031: ", lamb_dict[0.0][42031]
					#print sorted(lamb_dict[0.0].items(), key=itemgetter(1), reverse=True)[:10]
					
					#Remove this part for efficiency, may want to return for debug
					for idl in defmergeddict:
						ranked_fetch = sorted(defmergeddict[idl].items(), key=itemgetter(1), reverse=True)
						subset_ranked = dict(ranked_fetch)
						if idl not in contrib_dict_lambs:
							contrib_dict_lambs[idl] = {}
						for gid in subset_ranked:
							contrib_dict_lambs[idl].setdefault(gid, list()).append([word, subset_ranked[gid]])
						lamb_dict_list[idl].append(defmergeddict[idl])
		#print totaldict
		for in_domain_lamb in sorted(lamb_dict.keys()):
			start_dict_adding = datetime.datetime.now()
			ld_reduced = reduce(updateInPlace, (Counter(x) for x in lamb_dict_list[in_domain_lamb]))
			totaltime_dict_adding += datetime.datetime.now() - start_dict_adding
			sorted_total = sorted(ld_reduced.items(), key=itemgetter(1), reverse=True)
			y = 0
			rank_dict = {}
			#ranked_contrib = sorted(contrib_dict.items(), key=itemgetter(1), reverse=True)
			for t in sorted_total[:20]:
				y += 1 
				#print y
				#contrib_sub = sorted(contrib_dict[t[0]], key=itemgetter(1), reverse=True)
				#contrib_sub = sorted(contrib_dict[t[0]], key=itemgetter(1), reverse=True)
				rank_dict[t[0]] = [y, t[1], lat_long_lookup[t[0]], sorted(contrib_dict_lambs[in_domain_lamb][t[0]], key=itemgetter(1), reverse=True)[:5], t[0]]

			#print sorted_total[:20]
			#print dict(sorted_total)[42031]
			y = 0
			for i in sorted_total[:20]:
				y += 1
				#print rank_dict[i[0]]
				if y == 1:
					if topobase not in gazet_topos:
						gazet_topos.append(topobase)
					if toporef[j][1] not in gazet_topos:
						gazet_topos.append(toporef[j][1])
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
							SQL_Poly_dist = "SELECT ST_Distance(p1.pointgeog, p2.geog) FROM %s as p1, %s as p2 WHERE p1.placename = %s and p1.docid = %s and p1.wid = %s and p2.gid = %s;" % (tst_tbl, tbl, '%s', '%s', '%s', '%s')
							#SQL_Poly_dist = "SELECT ST_Distance(p1.polygeog, p2.geog) FROM trconllf as p1, %s as p2 WHERE p1.placename = %s and p1.docname = %s and p2.gid = %s;" % (tbl, '%s', '%s', '%s')
							#cur.execute(SQL_Point_dist, (toporef[j][0], xml, gid))
							#point_results = cur.fetchall()
							cur.execute(SQL_Poly_dist, (toporef[j][1], xml, wid, gid))
							poly_results = cur.fetchall()
						else: 
							print "@!@!@!@!@ More than one match found in gazet, error in gazet resolve logic @!@!@!@!@"
							#print gazet_entry
					SQL_Point_Dist = "SELECT ST_Distance(p1.pointgeog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM %s as p1 WHERE p1.placename = %s and p1.docid = %s and p1.wid = %s;" % (rank_dict[i[0]][2][1], rank_dict[i[0]][2][0], tst_tbl, '%s', '%s', '%s')
					#SQL_Poly_Dist = "SELECT ST_Distance(p1.polygeog, ST_GeographyFromText('SRID=4326;POINT(%s %s)')) FROM trconllf as p1 WHERE p1.placename = %s and p1.docname = %s and p1.polygeog IS NOT NULL;" % (rank_dict[i[0]][2][1], rank_dict[i[0]][2][0], '%s', '%s')
					cur.execute(SQL_Point_Dist, (toporef[j][1], xml, wid))
					point_results = cur.fetchall()
					#cur.execute(SQL_Poly_Dist, (toporef[j][0], xml))
					#poly_results = cur.fetchall()
					#print toporef[j][0]
					#print i
					#print xml
					print rank_dict[i[0]]
					#sys.exit()
					#print rank_dict[i[0]][2][1], rank_dict[i[0]][2][0]

					#print results
					#print results
					#print toporef[j][1], xml, wid, toporef[j][0]
					#print point_results
					pointdist = point_results[0][0]
					pointdist = pointdist / float(1000)

					if len(poly_results) > 0:
						polydist = (poly_results[0][0] / float(1000))
					else: polydist = pointdist

					print "In Domain Lambda: ", in_domain_lamb
					print "Point Dist: ", pointdist
					print "Poly Dist: ", polydist
					#print total_topo

					lamb_dict_total[in_domain_lamb].setdefault('point_dist_list', list()).append(pointdist)
					#point_dist_list.append(pointdist)
					#poly_dist_list.append(polydist)
					lamb_dict_total[in_domain_lamb].setdefault('poly_dist_list', list()).append(polydist)
					if pointdist > 1000.0:
						#point_bigerror.append([toporef[j][1], pointdist, tbl, [gold_lat, gold_long], rank_dict[i[0]][2], rank_dict[i[0]][4]])
						lamb_dict_total[in_domain_lamb].setdefault('point_bigerror', list()).append([toporef[j][1], pointdist, tbl, [gold_lat, gold_long], rank_dict[i[0]][2]])
					if polydist > 1000.0:
						lamb_dict_total[in_domain_lamb].setdefault('poly_bigerror', list()).append([toporef[j][1], polydist, tbl, gid, [gold_lat, gold_long], rank_dict[i[0]][2]])
						#poly_bigerror.append([toporef[j][1], polydist, tbl, gid, [gold_lat, gold_long], rank_dict[i[0]][2], rank_dict[i[0]][4]])
					if pointdist <= 161.0:
						#point_total_correct += 1
						lamb_dict_total[in_domain_lamb]['point_total_correct'] = lamb_dict_total[in_domain_lamb].get('point_total_correct', 0) + 1
					if polydist <= 161.0:
						#poly_total_correct += 1
						lamb_dict_total[in_domain_lamb]['poly_total_correct'] = lamb_dict_total[in_domain_lamb].get('poly_total_correct', 0) + 1
						print "Poly Total Correct for lambda: ", lamb_dict_total[in_domain_lamb]['poly_total_correct']
					#point_error += pointdist
					lamb_dict_total[in_domain_lamb]['point_error'] = lamb_dict_total[in_domain_lamb].get('point_error', 0) + pointdist
					#poly_error += polydist
					lamb_dict_total[in_domain_lamb]['poly_error'] = lamb_dict_total[in_domain_lamb].get('poly_error', 0) + polydist
			#print "====================="
			#print len(contextlist)
		totaltime_gazet_query += (datetime.datetime.now() - start_gazet_query)
		print "Time for Dict adding: ", totaltime_dict_adding
		print "Gazet Query Time: ", totaltime_gazet_query
		#sys.exit()
	return lamb_dict_total, total_topo

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
	if len(gazet_entry) > 1:
		ranked_gazet = sorted(gazet_entry, key=itemgetter(3), reverse=False)
		return [ranked_gazet[0]]
	return gazet_entry

def GetGazets_DistLimited(cur, placenames, latlong, country_tbl, region_tbl, state_tbl, geonames_tbl):
	names = tuple(x for x in placenames)
	DistLimit = 300.0
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
                dist = float(row[2])/1000.0
                if dist < DistLimit:
                        gazet_entry.append([country_tbl, row[0], row[1], dist])
                        #print "!!!Found Gazet Match!!!"
                        #print gazet_entry[-1]
	cur.execute(SQL2, (names, ))
	returns = cur.fetchall()
	for row in returns:
                dist = float(row[2])/1000.0
                if dist < DistLimit:
                        gazet_entry.append([region_tbl, row[0], row[1], float(row[2])/1000.0])
                        #print "!!!Found Gazet Match!!!"
                        #print gazet_entry[-1]
	cur.execute(SQL3, (names, ))
	returns = cur.fetchall()
	for row in returns:
                dist = float(row[2])/1000.0
                if dist < DistLimit:
                        gazet_entry.append([state_tbl, row[0], row[1], float(row[2])/1000.0])
                        #print "!!!Found Gazet Match!!!"
                        #print gazet_entry[-1]
	cur.execute(SQL4, (names, ))
	returns = cur.fetchall()
	for row in returns:
                dist = float(row[2])/1000.0
                if dist < DistLimit:
                        gazet_entry.append([US_Prominent_tbl, row[0], row[1], float(row[2])/1000.0])
                        #print "!!!Found Gazet Match!!!"
                        #print gazet_entry[-1]
	cur.execute(SQL5, (names, ))
	returns = cur.fetchall()
	for row in returns:
                dist = float(row[2])/1000.0
                if dist < DistLimit:
                        gazet_entry.append([Wrld_Prominent_tbl, row[0], row[1], float(row[2])/1000.0])
                        #print "!!!Found Gazet Match!!!"
                        #print gazet_entry[-1]
	if len(gazet_entry) > 1:
		ranked_gazet = sorted(gazet_entry, key=itemgetter(3), reverse=False)
		return [ranked_gazet[0]]
	return gazet_entry




def combine_tuples(t1, t2):
	tsum = t1[1] + t2[1]
	return (t1[0], tsum)
