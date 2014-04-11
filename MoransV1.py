import psycopg2
import datetime
import io
import math
import sys

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

def MoransCalc(gid_dict, gtbl, means_dict, kern_dist, cur):
    morans_c = {}
    total_denom_weights = 0

    x = 1
    
    for u in gid_dict:
        #For each u, get its neighbors
        #In future add condition for handling other types of kernel functions
        neibhbors = KF.Uniform(gtbl, u, kern_dist, cur)
        x = 1
        for word in means_dict:
            denom_weights = 0
            #For each grid in neighbors
            for ui in neighbors:
                gid = ui[0]
                w = ui[1]
                #Kernel weighted variance of a word between neighbors
                numerator = w * (gid_dict[u] - means_dict[word])*(gid_dict[gid] - means_dict[word])
                #MC Denominator
                denominator = math.pow((gid_dict[u] - means_dict[word]), 2)/float(len(gid_dict))
                morans_c[word] = morans_c.get(word, 0.0) + (float(numerator) / float(denominator))
                denom_weights += w
            if x == 1:
                total_denom_weights += denom_weights
            x += 1
    #Last step, divide by sum of all kernel weights
    for c in morans_c:
        morans_c[c] = morans_c[c]/float(total_denom_weights)

    return morans_c
        
            
        
        

def calc(f, dtbl, gtbl, conn_info, outf, agg_dist, kern_dist, traintype):
    
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
            if z % 2000 == 0:
                print z
                print datetime.datetime.now()


    print "Check doc dict is working"
    print docDict["2941536"]

    print "------Done reading in the data-------"
    read_time_end = datetime.datetime.now()
    print read_time_end - read_time_begin

    print "Number of Documents:", len(docDict)

    #Aggregate Language Models in grid
    
    cur = conn.cursor()
    #Fetch all points in our grid
    SQL_fetchgrid = "SELECT DISTINCT p1.gid from %s as p1, %s as p2 WHERE st_dwithin(p1.geog, p2.geog, %s);" % (gtbl, dtbl, '%s')
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
            print u[0], "Num Docs:", len(docs)
            #print docs
            #print docs[0][0]
            gid_totalwords[u[0]] = sum([docDict[x[0]].total_words for x in docs])
            gid_dict[u[0]] = reduce(updateInPlace, (Counter(docDict[x[0]].Feature_Freq) for x in docs))
            #except:
            #    for g in docs:
            #        print docDict[g[0]].Feature_Freq
            #    sys.exit("Error Adding Frequency Dictionaries")
        if m % 100 == 0:
            print m

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
            
    
    mc_dict = MoransCalcs(gid_dict, means_dict, kern_dist)

    #Once the above gets running, need to incorporate significance testing
    #Probably easiest to do this by randomizing the mapping of u's in gid_dict to the probability vectors
    #Then recalculate 100-900 times

    conn.close()
    
    
    
