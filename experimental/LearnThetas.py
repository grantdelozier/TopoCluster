import os
import io
from random import shuffle

def CreateTrainDev(in_direct, out_direct_train, out_direct_test, train_ratio, test_ratio, train_doc_file, out_train_tbl):

	cores = 2
	kerntype = "epanech"
	kern_dist = 100000
	file_type = "lgl"

	def SplitFiles(in_direct, out_direct_train, out_direct_test, train_ratio, test_ratio):
		train_files = 0
		test_files = 0

		if not os.path.exists(out_direct_train):
			os.makedirs(out_direct_train)
		if not os.path.exists(out_direct_test):
			os.makedirs(out_direct_test)

		files = os.listdir(in_direct)

		shuffle(files)

		#print files

		for f in files:
			test_xml = in_direct + "/" + f
			if float(test_files) <= float(train_files) * test_ratio and train_files >= 1:
				ouf = out_direct_test+"/"+f
				with io.open(ouf, 'w', encoding='utf-8') as w:
					rf = io.open(test_xml, 'r', encoding='utf-8')
					w.write(rf.read())
					rf.close()
				test_files += 1
			else: 
				ouf = out_direct_train+"/"+f
				with io.open(ouf, 'w', encoding='utf-8') as w:
					rf = io.open(test_xml, 'r', encoding='utf-8')
					w.write(rf.read())
					rf.close()
				train_files += 1
		print "Test Files :", test_files
		print "Train Files: ", train_files


	SplitFiles(in_direct, out_direct_train, out_direct_test, train_ratio, test_ratio)

	#Create Pseudo-Documents from Toponyms
	import PseudoDocCreator as PSD
	#PSD.calc(out_direct_train, train_doc_file, 100, file_type)

	#Create Document Table
	import LoadDBV1 as LDB
	#LDB.Load(train_doc_file, out_train_tbl, conn_info, "wiki")

	#Calc Local Stats Tables
	import LocalSpatialStatsV1 as LS
	train_tbl_part1 = out_train_tbl+"_kernel"+str(kern_dist/1000)+"k"+"_"+kerntype
	LS.calc(train_doc_file, "gi", out_train_tbl, gtbl, conn_info, "DummyOutfile.txt", train_tbl_part1, kern_dist, kerntype, "wiki", False, "any", 0, cores, False)
	#LSS.calc(f, statistic, dtbl, gtbl, conn_info, outf, out_tbl, kern_dist, kerntype, traintype, listuse, whitelist_file, grid_min, cores, include_zero)
	train_tbl = train_tbl_part1 + "_gi"
	return train_tbl, out_direct_test

mode = "lgl"
outfile = "LGL_theta_Results_devsplit3.txt"
out_domain_stat_tbl = ""
conn_info = "dbname=topodb user=postgres host='localhost' port='5433' password='grant'"
gtbl = "globalgrid_5_clip_geog"
window = 15
percentile = 1.0
country_tbl = "countries_2012"
region_tbl = "regions_2012"
state_tbl = "states_2012"
geonames_tbl = "geonames_all"


if mode == "trconll":

	########Optimize for TR-CoNLL###########
	import TestResolver_TRCONLL_allits_Thetas as TR
	tst_tbl = "trconllf_dev"

	lamb_wiki = 0.9
	lamb_trconll = 0.1

	in_direct = "/home/grant/devel/TopCluster/trconllf/xml/dev"
	out_direct_train = "/home/grant/devel/TopCluster/trconllf/xml/dev_trainsplit1"
	out_direct_test = "/home/grant/devel/TopCluster/trconllf/xml/dev_testsplit1"
	train_ratio = .80
	test_ratio = .20
	#train_doc_file = "/home/grant/devel/TopCluster/trconllf/dev_trainsplit2_docfile.txt"
	out_train_tbl = "trconllf_dev_trainsplit1"


	it_size = 0.1
	train_tbl = "trconllf_dev_trainsplit1_kernel100k_epanech_gi"
	test_xml = "/home/grant/devel/TopCluster/trconllf/xml/dev_testsplit1"
	#train_tbl, test_xml = CreateTrainDev(in_direct, out_direct_train, out_direct_test, train_ratio, test_ratio, train_doc_file, out_train_tbl)


	#while lamb_wiki <= 1.0:
		
	TR.calc(train_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile, country_tbl, region_tbl, state_tbl, geonames_tbl, tst_tbl, lamb_trconll, lamb_wiki, outfile)

	#	#lamb_wiki += it_size
	#	#lamb_trconll += -1.0 * it_size

if mode == "cwar":
	########Optimize for Cwar###########
	import TestResolver_CWar_allits as TR
	tst_tbl = "cwar_dev"

	lamb_wiki = 0.0
	lamb_trconll = 1.0

	in_direct = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev"
	#out_direct_train = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev_trainsplit1"
	out_direct_train = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev_trainsplit1"
	out_direct_test = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev_testsplit1"
	train_ratio = .80
	test_ratio = .20
	train_doc_file = "/home/grant/devel/TopCluster/cwar/cwar/dev_trainsplit1_docfile.txt"
	out_train_tbl = "cwar_dev_trainsplit1"


	it_size = 0.1
	train_tbl = "cwar_dev_trainsplit1_kernel100k_epanech_gi"
	test_xml = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev_testsplit1"
	#train_tbl, test_xml = CreateTrainDev(in_direct, out_direct_train, out_direct_test, train_ratio, test_ratio, train_doc_file, out_train_tbl)
		
	TR.calc(train_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile, place_name_weight, country_tbl, region_tbl, state_tbl, geonames_tbl, tst_tbl, outfile)

if mode == "lgl":
	import TestResolver_LGL_allits_Thetas as TR
	tst_tbl = "lgl_dev_classic"

	lamb_wiki = 0.5
	lamb_lgl = 0.5

	in_direct = "/home/grant/Downloads/LGL/articles/dev_classicxml"
	#out_direct_train = "/home/grant/devel/TopCluster/cwar/cwar/xml/dev_trainsplit1"
	out_direct_train = "/home/grant/Downloads/LGL/articles/dev_trainsplit3" 
	out_direct_test = "/home/grant/Downloads/LGL/articles/dev_testsplit3"
	train_ratio = .80
	test_ratio = .20
	train_doc_file = "/home/grant/Downloads/LGL/dev_trainsplit3_docfile.txt"
	out_train_tbl = "lgl_dev_trainsplit2"


	#it_size = 0.1
	train_tbl = "lgl_dev_trainsplit3_kernel100k_epanech_gi"
	test_xml = "/home/grant/Downloads/LGL/articles/dev_testsplit3"
	#train_tbl, test_xml = CreateTrainDev(in_direct, out_direct_train, out_direct_test, train_ratio, test_ratio, train_doc_file, out_train_tbl)
	print "Done generating statistics tables"	
	
	TR.calc(train_tbl, out_domain_stat_tbl, test_xml, conn_info, gtbl, window, percentile, country_tbl, region_tbl, state_tbl, geonames_tbl, tst_tbl, lamb_lgl, lamb_wiki, outfile)
