import numpy as np
import io
import sys

#from scipy.optimize import fmin_bfgs, fmin_l_bfgs_b
#from sklearn import linear_model

def calc(tf):

	column_key = {}
	i = 0
	all_feats = []
	m = 0
	gid_set = set()

	with io.open(tf, 'r', encoding='utf-8') as w:
		for line in w:
			m += 1
			if m % 20 == 0:
				print m
			row = line.split('\t')
			toponym = row[0]
			gid = row[1]
			gid_set |= set([gid])
			#print len(row[2].split(' '))
			#for r in row[2].strip().split(" "):
			#	print r.split(':')
			#	print r.split(':')[0], r.split(':')[1]
			feat_vect = dict([[f.split(':')[0], f.split(':')[1]] for f in row[2].strip().split(" ")])
			all_feats.append([toponym, gid, feat_vect])
			for k in feat_vect:
				if k not in column_key:
					column_key[k] = i
					i += 1

	print "Number Unique gids: ", len(gid_set)
	print len(column_key)
	sys.exit()
	X = np.zeros((len(all_feats), len(column_key)))
	Y = np.arange(len(all_feats), dtype=np.uint8)
	print X.shape
	j = 0
	for frow in all_feats:
		#print frow[0], frow[1]
		Y[j] = int(frow[1].strip())
		for f in frow[2]:
			X[j][column_key[f]] = float(frow[2][f])
		j += 1

	#test = fmin_l_bfgs_b(logLikelihoodLogit, x0=x0, args=(X, Y), approx_grad=True)
	sk_logmodel = linear_model.LogisticRegression(penalty='l2', dual=True)
	sk_logmodel.fit(X, Y)
	params = sk_logmodel.get_params()
	print params

#Rewrite into vowpal wabbit friendly version
def calc2(tf):

	opf = io.open("TrainingFeatures_TRCoNLLV2_Vowp.txt", 'w', encoding='utf-8')
	m = 0
	gid_dict = {}
	g = 0

	with io.open(tf, 'r', encoding='utf-8') as w:
		for line in w:
			m += 1
			if m % 20 == 0:
				print m
			row = line.split('\t')
			gid = row[1]
			if gid not in gid_dict:
				g += 1
				gid_dict[gid] = g
			opf.write(str(gid_dict[gid])+' | '+row[2]+'\r\n')
	opf.close()
	print len(gid_dict)
	print "Done"





trainfile = "/home/grant/devel/TopoCluster/TrainingFeatures_TRCoNLLV2.txt"
calc2(trainfile)