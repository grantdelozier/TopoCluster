import subprocess
import os
import io
import xml.etree.ElementTree as ET


def readnerxml(xmlfile):
	xmldoc = ET.parse(file(xmlfile))
	root = xmldoc.getroot()

	i = 0
	toporef2 = {}
	wordref2 = {}
	locfound = False

	for child in root.iter('sometext'):
		for w in child:
			i += 1
			if w.attrib['entity'] == 'LOCATION':
				if locfound == True:
					i = i - 1
					#print 'INSIDE IF STATEMENT'
					toporef2[i] = toporef2[i] + ' ' + w.text
					wordref2[i] = wordref2[i] + ' ' + w.text
				else:
					toporef2[i] = w.text
					wordref2[i] = w.text
				locfound = True
			else:  
				wordref2[i] = w.text
				locfound = False
	return toporef2, wordref2

def readnerxml2(xmlfile):
	xmldoc = ET.parse(file(xmlfile))
	root = xmldoc.getroot()

	i = 0
	toporef2 = {}
	wordref2 = {}
	locfound = False

	for child in root.iter('sometext'):
		for w in child:
			i += 1
			if w.attrib['entity'] == 'LOCATION':
				if locfound == True:
					i = i - 1
					#print 'INSIDE IF STATEMENT'
					toporef2[i] = toporef2[i] + ' ' + w.text
					i = i + 1
				else:
					toporef2[i] = w.text
				wordref2[i] = w.text
				locfound = True
			else:  
				wordref2[i] = w.text
				locfound = False
	return toporef2, wordref2

def calc(stan_path, filename, outxml):
	#print text
	os.chdir(stan_path)
	#print os.getcwd()
	#print outxml
	xmlfile = outxml
	op = io.open(xmlfile, 'w', encoding='utf-8')
	op.write(u"<sometext>"+'\r\n')
	op.close()
	with io.open(xmlfile, 'a', encoding='utf-8') as w:
		subprocess.check_call(["java", "-Xmx4g", "-cp", "stanford-ner.jar", "edu.stanford.nlp.ie.crf.CRFClassifier", "-loadClassifier", "classifiers/english.all.3class.distsim.crf.ser.gz", "-textFile", filename, "-outputFormat", "xml", "edu.stanford.nlp.process.PTBTokenizer" ], stdout=w)
		w.write(u"</sometext>")
	#readnerxml(xmlfile)

#This version doesn't default to pre-whitespace tokenized
def calc2(stan_path, filename, outxml):
	os.chdir(stan_path)
	#print os.getcwd()
	#print outxml
	xmlfile = outxml
	op = io.open(xmlfile, 'w', encoding='utf-8')
	op.write(u"<sometext>"+'\r\n')
	op.close()
	with io.open(xmlfile, 'a', encoding='utf-8') as w:
		subprocess.check_call(["java", "-Xmx4g", "-cp", "stanford-ner.jar", "edu.stanford.nlp.ie.crf.CRFClassifier", "-loadClassifier", "classifiers/conll.closed.iob2.crf.ser.gz", "-textFile", filename, "-outputFormat", "xml"], stdout=w)
		w.write(u"</sometext>")

#text = "Avoyelles task force arrests 14. The Task Force officers began their night at the Cottonport Fire Station and arrested 14 in the Cottonport area through the night clearing 18 warrants during the four-hour operation. All arrests were without incident, the Sheriff&#8217;s Office said. They were arrested on outstanding warrants for issuing worthless checks, probation violations and narcotic violations. Anderson again urged all who have a reason to believe there may be an outstanding warrant for their arrest to contact the Avoyelles Parish Sheriff&#8217;s Office immediately in order to resolve any issues. They may contact the Warrant Division at (318) 619-3946 or the Patrol Division at (318) 619-3947."
#filename = "/home/grant/devel/samplenertext.txt"
#op = io.open(filename, 'w', encoding='utf-8')
#op.write(unicode(text, 'utf-8'))
#op.close()

#stan_path = "/home/grant/devel/TopoCluster/stanford-ner-2014-06-16"
#calc(text, stan_path, filename)