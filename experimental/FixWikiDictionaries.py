import sys

import datetime
import io
import math


class Document:

    userID = ""
    userLat = ""
    userLong = ""

    Feature_Freq = {}
    total_words = 0
    Feature_Prob = {}
    outside_word_prob = 0
    fileFrom = ""

    def __init__(self, ID, latit, longit, F_Freq, file_from, listuse, whitelist, aggLM=False):
        self.userID = ID
        self.userLat = latit
        self.userLong = longit
        #self.Feature_Freq = F_Freq
        self.fileFrom = file_from
        tw = 0
        if aggLM == False:
            for f in F_Freq:
                tw += int(F_Freq[f])
        self.total_words = tw
        self.Feature_Freq = F_Freq

def calc(oldwiki, newwiki, outf):
    oldwiki_filename = oldwiki[oldwiki.rfind('/')+1:]
    print oldwiki_filename
    begin_time = datetime.datetime.now()
    docDict = {}
    x = 0
    y = 0
    F_All = set()

    z = 0
    read_time_begin = datetime.datetime.now()

    #Defaulting to false... should probably just remove this variable from the local stats calc
    #keeping it in for now because may want to incorporate later?
    UseAggLMs = False

    traintype = "wiki"
    listuse = 'any'
    whitelist = set()

    #Read in the trainfile data/calc word frequencies
    with io.open(oldwiki, 'r', encoding='utf-8') as w:
        for person in w:
            #print "####NEW Person####"
            #print userID, latit, longit
            try:
                row = person.strip().split('\t')
                #print row[0]

                if traintype == "twitter":
                    #print row[0]
                    userID = row[0]
                    latit = row[1].split(',')[0]
                    longit = row[1].split(',')[1]
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[2].split(" "))
                        if listuse == 'any':
                            F_All |= set(F_Freq.keys())
                        if listuse == 'restricted':
                            F_All |= set([j for j in F_Freq if j in whitelist])
                        newDoc = Document(userID, latit, longit, F_Freq, filename, listuse, whitelist)
                        docDict[userID] = newDoc.Feature_Prob
                    elif UseAggLMs == True:
                        F_Freq = dict([f.split(':')[0],float(f.split(':')[1])] for f in row[2].split(" "))
                        if listuse == 'any':
                            F_All |= set(F_Freq.keys())
                        if listuse == 'restricted':
                            F_All |= set([j for j in F_Freq if j in whitelist])
                        docDict[userID] = F_Freq
                elif traintype == "wiki":
                    userID = int(row[0])
                    page_name = row[1]
                    latit = row[2].split(',')[0]
                    longit = row[2].split(',')[1]
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[9].split(" "))
                        #F_All |= set(F_Freq.keys())
                        #newDoc = Document(userID, latit, longit, F_Freq, filename, listuse, whitelist)
                        docDict[userID] = [userID, page_name, latit, longit, F_Freq]
                    if UseAggLMs == True:
                        print "Use Agg LM mode for wikipedia dataset not implemented"
                        sys.exit("Error")
            except:
                print "@@@@@error reading user@@@@@@"
                print row
                print z
                #sys.exit("Error")
            z += 1
            if z % 5000 == 0:
                print z
                print datetime.datetime.now()

    print "------Done reading in the data-------"
    read_time_end = datetime.datetime.now()
    print read_time_end - read_time_begin

    print "Number of Documents:", len(docDict)


    newwiki_filename = newwiki[newwiki.rfind('/')+1:]
    print newwiki_filename
    begin_time = datetime.datetime.now()
    #docDict = {}
    x = 0
    y = 0
    #F_All = set()

    z = 0
    read_time_begin = datetime.datetime.now()

    #Defaulting to false... should probably just remove this variable from the local stats calc
    #keeping it in for now because may want to incorporate later?
    UseAggLMs = False

    traintype = "wiki"
    listuse == 'any'
    whitelist = set()

    updated = 0
    changedlats = 0

    #Read in the trainfile data/calc word frequencies
    with io.open(newwiki, 'r', encoding='utf-8') as f:
        for person in f:
            #print "####NEW Person####"
            #print userID, latit, longit
            try:
                row = person.strip().split('\t')
                #print row[0]

                if traintype == "twitter":
                    #print row[0]
                    userID = row[0]
                    latit = row[1].split(',')[0]
                    longit = row[1].split(',')[1]
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[2].split(" "))
                        if listuse == 'any':
                            F_All |= set(F_Freq.keys())
                        if listuse == 'restricted':
                            F_All |= set([j for j in F_Freq if j in whitelist])
                        newDoc = Document(userID, latit, longit, F_Freq, filename, listuse, whitelist)
                        docDict[userID] = newDoc.Feature_Prob
                    elif UseAggLMs == True:
                        F_Freq = dict([f.split(':')[0],float(f.split(':')[1])] for f in row[2].split(" "))
                        if listuse == 'any':
                            F_All |= set(F_Freq.keys())
                        if listuse == 'restricted':
                            F_All |= set([j for j in F_Freq if j in whitelist])
                        docDict[userID] = F_Freq
                elif traintype == "wiki":
                    userID = int(row[0])
                    page_name = row[1]
                    latit = row[2].split(',')[0]
                    longit = row[2].split(',')[1]
                    if UseAggLMs == False:
                        F_Freq = dict([y.split(':')[0],int(y.split(':')[1])] for y in row[-1].split(" "))
                        #F_All |= set(F_Freq.keys())
                        #newDoc = Document(userID, latit, longit, F_Freq, filename, listuse, whitelist)
                        if userID in docDict:
                            c = F_Freq.copy()
                            c.update(docDict[userID][4])
                            if 'Johannesburg' in F_Freq and float(latit) > 25.0 and float(latit) < 27.4 and float(longit) > 27.0 and float(longit) < 29.0:
                                latit = (-1.0 * float(latit))
                                changedlats += 1
                            docDict[userID] = [userID, page_name, latit, longit, c]
                            updated += 1
                            #z = docDict[userID][4].copy()
                        else: docDict[userID] = [userID, page_name, latit, longit, F_Freq]
                    if UseAggLMs == True:
                        print "Use Agg LM mode for wikipedia dataset not implemented"
                        sys.exit("Error")
            except:
                print "@@@@@error reading user@@@@@@"
                print row
                print z
                #sys.exit("Error")
            z += 1
            if z % 5000 == 0:
                print z
                print len(docDict)
                print "Updated: ", updated
                print "Changed lats: ", changedlats 
                print datetime.datetime.now()

    print "Writing File"
    with io.open(outf, 'w', encoding='utf-8') as ow:
        for thing in docDict:
            s1 = [str.join('', [' ', k, ':', unicode(v)]) for k,v in docDict[thing][4].items()]
            s2 = str.join('', s1)
            lat_long = unicode(docDict[thing][2]) + ',' + unicode(docDict[thing][3])
            ow.write(str(thing) + '\t' + docDict[thing][1] + '\t' + lat_long + '\t' + '?' + '\t' + '?' + '\t' + '?' + '\t' + '?' + '\t' + '?' + '\t' + s2.strip() + '\r\n')

    print "Done"

calc("/home/grant/devel/TopCluster/Corpora/enwiki-20130102/enwiki-20130102-permuted-training.data.txt", "/home/grant/devel/TopCluster/Corpora/enwiki-20130102-ner/wiki_training_file.txt", "enwiki-20130102-training-NER.txt")