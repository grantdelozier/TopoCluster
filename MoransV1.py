import psycopg2
import datetime
import io
import math
import sys
import numpy

from collections import Counter

import KernelFunctionsV1 as KF

class Document:

    userID = ""
    userLat = ""
    userLong = ""

    Feature_Freq = {}
    total_words = 0
    #Feature_Prob = {}
    outside_word_prob = 0
    fileFrom = ""

    def __init__(self, ID, latit, longit, F_Freq, file_from):
        self.userID = ID
        self.userLat = latit
        self.userLong = longit
        self.Feature_Freq = F_Freq
        self.fileFrom = file_from
        tw = 0
        for f in F_Freq:
            tw += int(F_Freq[f])
        self.total_words = tw
        #self.CalcUnigramProb(self, F_Freq)

    #def CalcUnigramProb(self, F_Freq):
    #    F_Prob = {}
    #    for word in F_Freq:
    #        F_Prob[word] = (float(F_Freq[word])/float(total_words))
    #    self.Feature_Prob = F_Prob

def updateInPlace(a,b):
    a.update(b)
    return a

def getVector(gid_dict_u, reference):
    u_vector = numpy.zeros((len(reference), 1))
    for wd in gid_dict_u:
        u_vector[reference[wd]][0] = gid_dict_u[wd]
    return u_vector

def buildRef(adict):
    x = 0
    dict_ref = {}
    dict_ref2 = {}
    for w in adict:
        dict_ref[w] = x
        dict_ref2[x] = w
        x += 1
    return dict_ref, dict_ref2
        

def MoransCalc(gid_dict, gtbl, means_dict, kern_dist, cur):
    morans_c = {}
    total_denom_weights = 0

    x = 1
    m = 0    
    for u in gid_dict:
        #For each u, get its neighbors
        #In future add condition for handling other types of kernel functions
        neighbors = KF.Uniform(gtbl, u, kern_dist, cur, "Only")
        m = m + 1
        x = 1
        for word in means_dict:
            denom_weights = 0
            #For each grid in neighbors
            for ui in neighbors:
                gid = ui[0]
                if gid in gid_dict:
                    w = float(ui[1])
                    #Kernel weighted variance of a word between neighbors
                    numerator = w * (gid_dict[u].get(word, 0.0) - means_dict[word])*(gid_dict[gid].get(word, 0.0) - means_dict[word])
                    #MC Denominator
                    denominator = math.pow((gid_dict[u].get(word, 0.0) - means_dict[word]), 2)/float(len(gid_dict))
                    morans_c[word] = morans_c.get(word, 0.0) + (float(numerator) / float(denominator))
                    denom_weights += w
            if x == 1:
                total_denom_weights += denom_weights
            x += 1
        if m % 10 == 0:
            print m
            print datetime.datetime.now()
    #Last step, divide by sum of all kernel weights
    for c in morans_c:
        morans_c[c] = morans_c[c]/float(total_denom_weights)

    return morans_c

def MoransCalc2(gid_dict, gtbl, means_dict, kern_dist, cur):
    morans_c = {}
    total_denom_weights = 0

    x = 1
    m = 0

    #First dictionary is word -> index, second is index -> word
    ref_dict, ref_dict2 = buildRef(means_dict)
    
    mean_vector = getVector(means_dict, ref_dict)
    sum_vectors = numpy.zeros((len(mean_vector),1))                           

    for u in gid_dict:
        #For each u, get its neighbors
        #In future add condition for handling other types of kernel functions
        neighbors = KF.Uniform(gtbl, u, kern_dist, cur, "Only")
        target_vector = getVector(gid_dict[u], ref_dict)
        m = m + 1
        x = 1
        for ui in neighbors:
            gid = ui[0]
            if gid in gid_dict:
                            
                w = float(ui[1])
                            
                total_denom_weights += w
                            
                neighbor_vector = getVector(gid_dict[gid], ref_dict)
                            
                numerator = w * numpy.multiply(numpy.subtract(target_vector, mean_vector ), numpy.subtract(neighbor_vector, mean_vector ))
                            
                denom = numpy.multiply(numpy.subtract(target_vector, mean_vector ), numpy.subtract(target_vector, mean_vector ))
                            
                div_vector = numpy.divide(numerator, denom)
                
                sum_vectors += numpy.where(div_vector==div_vector, div_vector, 0.0)
        if m % 10 == 0:
            print m
            print datetime.datetime.now()

    i = 0
    while i < len(sum_vectors):
        morans_c[ref_dict2[i]] = sum_vectors[i][0]/float(total_denom_weights)
        i += 1
        
    return morans_c
        
    
                
            
                
                
        

        
            
        
        

def calc(f, dtbl, gtbl, conn_info, outf, agg_dist, kern_dist, traintype, writeAggLMs, UseAggLMs, writeAggFile):

    print "Morans Calc Parameters:"
    print "Train file: ", f
    print "Document Table Name: ", dtbl
    print "Connection Information: ", conn_info
    print "Morans Score Outfile: ", outf
    print "Aggregate Docs Distance: ", agg_dist
    print "Moran's kernel search bandwidth: ", kern_dist
    print "Document type: ", traintype
    print "Writing Aggregated LMs?: ", writeAggLMs
    print "Using Aggregated LMs for train doc?: ", UseAggLMs
    print "Aggregated LMs write file: ", writeAggFile

    if writeAggLMs==True and UseAggLMs==True:
        print "ERROR: cannot both write and use agg LM, specify one or other option"
        sys.exit()

    if UseAggLMs == True:
        traintype = "twitter"
    
    filename = f[f.rfind('/')+1:]
    print filename
    begin_time = datetime.datetime.now()
    docDict = {}
    x = 0
    y = 0
    F_All = set()

    z = 0
    read_time_begin = datetime.datetime.now()

    #Connecting to Database
    conn = psycopg2.connect(conn_info)
    print "DB Connection Success"

    #Read in the trainfile data/calc word frequencies
    with io.open(f, 'r', encoding='utf-8') as f:
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
                    F_Freq = dict(f.split(':') for f in row[2].split(" "))
                elif traintype == "wiki":
                    userID = row[0]
                    page_name = row[1]
                    latit = row[2].split(',')[0]
                    longit = row[2].split(',')[1]
                    F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[9].split(" "))
                
                F_All |= set(F_Freq.keys())
                newDoc = Document(userID, latit, longit, F_Freq, filename)
                docDict[userID] = newDoc
            except:
                print "@@@@@error reading user@@@@@@"
                print row
                print z
                break
            z += 1
            if z % 5000 == 0:
                print z
                print datetime.datetime.now()

    print "------Done reading in the data-------"
    read_time_end = datetime.datetime.now()
    print read_time_end - read_time_begin

    print "Number of Documents:", len(docDict)

    if UseAggLMs == False:

        #Aggregate Language Models in grid
        
        cur = conn.cursor()
        #Fetch all points in our grid
        gid_lat_long_ref = {}
        SQL_fetchgrid = "SELECT DISTINCT p1.gid, ST_X(p1.geog::geometry), ST_Y(p1.geog::geometry) from %s as p1, %s as p2 WHERE st_dwithin(p1.geog, p2.geog, %s);" % (gtbl, dtbl, '%s')
        cur.execute(SQL_fetchgrid, (agg_dist, ))
        grid = cur.fetchall()
        #Find all content Documents for each grid point
        gid_dict = {}
        gid_totalwords = {}
        print "Building Aggregated gi_dict"
        print "Number of points in grid: ", len(grid)
        m = 0
        for u in grid:
            #A uniform kernel search is called here.
            #In the future should add a kern_type argument and handle other functions
            docs = KF.Uniform(dtbl, u[0], agg_dist, cur, gtbl)
            m += 1
            if len(docs) > 0:
                #print u[0], "Num Docs:", len(docs)
                #print docs
                #print docs[0][0]
                gid_totalwords[u[0]] = sum([docDict[x[0]].total_words for x in docs])
                gid_dict[u[0]] = reduce(updateInPlace, (Counter(docDict[x[0]].Feature_Freq) for x in docs))
                gid_lat_long_ref[u[0]] = [u[1], u[2]]
                #except:
                #    for g in docs:
                #        print docDict[g[0]].Feature_Freq
                #    sys.exit("Error Adding Frequency Dictionaries")
            if m % 200 == 0:
                print m
                print "Points left to go: ", len(grid)-m

        print "Number of points in the grid:", len(gid_dict)

        print "Calculating Word Means + Transforming to Unigram Probabilities..."

        means_dict = {}

        #Simple MLE Unigram estimate
        #In the future allow for other types of Prob Estimates
        for i in gid_dict:
            for w in gid_dict[i]:
                #Freq(word in doc)/Freq(all words in doc)
                gid_dict[i][w] = float(gid_dict[i][w])/float(gid_totalwords[i])
                #Mean Unigram Prob among all points in grid
                means_dict[w] = means_dict.get(w, 0) + gid_dict[i][w]


        for wd in means_dict:
            means_dict[wd] = float(means_dict[wd]) / float(len(gid_dict))

        #If option is set, write the grid aggregated documents to a file (can speed up subsequent loads if multiple calcs done on same file)
        #The output here can be read in as a train file and previous steps can be skipped for future loads.
        if writeAggLMs==True:
            openw = io.open(writeAggFile, 'w', encoding='utf-8')
            #cur.execute("SELECT gid,  FROM ")
            for i in gid_dict:
                s1 = [str.join('', [' ', k, ':', unicode(v)])for k,v in mc_dict[mc].items()]
                s2 = ""
                s2.join(s1)
                lat_long = gid_lat_long_ref[mc][0]+','+gid_lat_long_ref[mc][1]
                openw.write(i + '\t' + lat_long + '\t' + s2 + '\r\n')
            openw.close()

    if UseAggLMs == True:
        cur = conn.cursor()
        print "Using Aggregated LMs..."
        gid_dict = docDict
        print "Getting Mean probs"
        means_dict = {}
        for i in gid_dict:
            for w in gid_dict[i]:
                #Mean Unigram Prob among all points in grid
                means_dict[w] = means_dict.get(w, 0) + gid_dict[i][w]
        for wd in means_dict:
            means_dict[wd] = float(means_dict[wd]) / float(len(gid_dict))
        print "Done obtaining means probs"
        
            
            
    print "Getting Moran's scores"

    print "Number of words: ", len(means_dict)
    
    mc_dict = MoransCalc2(gid_dict, gtbl, means_dict, kern_dist, cur)

    wf = open(outf, 'w')
    for mc in mc_dict:

        wf.write(unicode(mc) + '\t' + unicode(mc_dict[mc]) + '\r\n')

    wf.close()

    #Once the above gets running, need to incorporate significance testing
    #Probably easiest to do this by randomizing the mapping of u's in gid_dict to the probability vectors
    #Then recalculate 100-900 times

    conn.close()
    
    
    