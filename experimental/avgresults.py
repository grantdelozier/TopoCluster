import os

lamb_outputs = "/home/grant/devel/TopoCluster_Lambdas/Cwar"

lamb_accs = {}

files = os.listdir(lamb_outputs)
for f in files:
	print f
	last_lamb = -99.9
	with open(lamb_outputs+'/'+f, 'r') as w:
		for line in w:
			if "In Domain Corp Lambda" in line.strip():
				#print line.strip()
				last_lamb = line.strip()[line.strip().find(':')+1:].strip()
				print last_lamb
			if "Point Accuracy" in line.strip():
				#print line.strip()
				pntacc = float(line.strip()[line.strip().find(':')+1:].strip())
				print pntacc
				lamb_accs.setdefault(last_lamb+"_point", list()).append(pntacc)
			if "Polygon Accuracy" in line.strip():
				#print line.strip()
				plyacc = float(line.strip()[line.strip().find(':')+1:].strip())
				print plyacc
				lamb_accs.setdefault(last_lamb+"_poly", list()).append(plyacc)

def avg(alist):
	asum = 0.0
	for i in alist:
		asum += i
	return asum/float(len(alist))

for thing in sorted(lamb_accs.keys()):
	print thing
	#print lamb_accs[thing]
	print avg(lamb_accs[thing])



print "Done"