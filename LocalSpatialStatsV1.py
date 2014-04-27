import sys

#sys.path.append('/home/02608/grantdel/pythonlib/lib/python2.7/site-packages')
import psycopg2
import datetime
import io
import math
#import numpy

from collections import Counter

import KernelFunctionsV1 as KF

import operator

import random

class Document:

    userID = ""
    userLat = ""
    userLong = ""

    Feature_Freq = {}
    total_words = 0
    Feature_Prob = {}
    outside_word_prob = 0
    fileFrom = ""

    def __init__(self, ID, latit, longit, F_Freq, file_from, aggLM=False):
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
        if aggLM==False:
            self.CalcUnigramProb(F_Freq)

    def CalcUnigramProb(self, F_Freq):
        F_Prob = {}
        for word in F_Freq:
            F_Prob[word] = (float(F_Freq[word])/float(self.total_words))
        self.Feature_Prob = F_Prob

def chunkIt(seq, num):
  avg = len(seq) / float(num)
  out = []
  last = 0.0

  while last < len(seq):
    out.append(seq[int(last):int(last + avg)])
    last += avg

  return out

def GiCalcs(gtbl, dtbl, id_list, kern_dist, kerntype, conn_info, docDict, word_totals, F_All, out_tbl):

    print "Starting Calculation Branch ", id_list[0] , "\r\n"

    z = 0

    #Connecting to Database
    conn = psycopg2.connect(conn_info)
    print "DB Connection Success ", id_list[0] , "\r\n"

    cur = conn.cursor()

    zero_dict = dict([(x, 0.0) for x in F_All])

    for i in id_list:
        z += 1
        rows = KF.Uniform(ptbl, i, kern_dist, cur, gtbl)
        allData = []
        if len(rows) > 0:
            pid_start = datetime.datetime.now()
            #print "###########", i, "###############"
            #print "Num Neighbors", len(rows)

            newsumDict = zero_dict

            for p in rows:
                uid = p[0]
                weight = p[1]
                for wd in docDict[uid]:
                    newsumDict[wd] = newsumDict.get(wd, 0.0) + (weight * docDict[uid][wd])

            for w in newsumDict:
                gi_stat = newsumDict[w]/word_totals[w]
                allData.append([i, w, gi_stat])

            args_str = ",".join(cur.mogrify("(%s,%s,%s)", x) for x in allData)
                
            qstr = "INSERT INTO %s VALUES " % out_tbl
            cur.execute(qstr + args_str)
                    
        #print datetime.datetime.now()
        if z % 50 == 0:
            print "Left to go: ", (len(id_list) - z), " branch: ", id_list[0], "\r\n"
            print datetime.datetime.now(), "\r\n"

    conn.commit()
    conn.close()

###Main Method###
def calc(f, statistic, dtbl, gtbl, conn_info, outf, out_tbl, kern_dist, kerntype, traintype, listuse, whitelist_file, grid_freq_min, cores):
    print "Local Spatial Statistics Parameters"
    print "Train file: ", f
    print "Document Table Name: ", dtbl
    print "Document type: ", traintype
    print "Grid Table Name: ", gtbl
    print "Connection Information: ", conn_info
    print "Local Statistic Outfile Name: ", outf
    print "Local Statistic Output Table Name: ", out_tbl
    print "Distance Bandwidth of Kernel Function: ", kern_dist
    print "Type of kernel function being used: ", kerntype
    print "minimum number of grid points word must appear in: ", grid_freq_min
    print "Which Statistics are being calculated: ", statistic

    print "Calculating Statistics for which words: ", listuse
    print "Location of whitelist_file: ", whitelist_file

    print "Number of cores you want to devote to multiprocessing (recommended 1/2 the number that exist on the system):", cores
    
    filename = f[f.rfind('/')+1:]
    print filename
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

    #Connecting to Database
    conn = psycopg2.connect(conn_info)
    print "DB Connection Success"

    if listuse == 'restricted':
        F_All = set()
        with io.open(whitelist_file, 'r', encoding='utf-8') as w:
            whitelist = set([x.strip() for x in w])
            
        print "Num Words in whitelist: ", len(whitelist)

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
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[2].split(" "))
                        if listuse == 'any':
                            F_All |= set(F_Freq.keys())
                        if listuse == 'restricted':
                            F_All |= set([j for j in F_Freq if j in whitelist])
                        newDoc = Document(userID, latit, longit, F_Freq, filename)
                        docDict[userID] = newDoc.Feature_Prob
                    elif UseAggLMs == True:
                        F_Freq = dict([f.split(':')[0],float(f.split(':')[1])] for f in row[2].split(" "))
                        if listuse == 'any':
                            F_All |= set(F_Freq.keys())
                        if listuse == 'restricted':
                            F_All |= set([j for j in F_Freq if j in whitelist])
                        docDict[userID] = F_Freq
                elif traintype == "wiki":
                    userID = row[0]
                    page_name = row[1]
                    latit = row[2].split(',')[0]
                    longit = row[2].split(',')[1]
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[9].split(" "))
                        #F_All |= set(F_Freq.keys())
                        newDoc = Document(userID, latit, longit, F_Freq, filename)
                        if listuse == 'any':
                            F_All |= set(F_Freq.keys())
                        if listuse == 'restricted':
                            F_All |= set([j for j in F_Freq if j in whitelist])
                        docDict[newDoc.userID] = newDoc.Feature_Prob
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

    cur = conn.cursor()

    if statistic.lower() == "gi":

        print "Begining Gi* statistic calculation mode"

        out_tbl_gi = out_tbl + "_gi"

        cur.execute("CREATE TABLE IF NOT EXISTS %s (gid varchar(20), word varchar(30), stat float);" % (out_tbl_gi, ))

        cur.execute("DELETE FROM %s ;" % out_tbl_gi)

        word_totals = {}
        
        print "Generating Probability Sums for each word"
        
        for doc in docDict:
            for word in docDict[doc]:
                word_totals[word] = word_totals.get(word, 0.0) + docDict[doc][word]

        print "Done Calculating Word Totals"

        z = 0

        SQL_fetchgrid = "SELECT DISTINCT p1.gid from %s as p1, %s as p2 WHERE st_dwithin(p1.geog, p2.geog, %s);" % (gtbl, dtbl, '%s')
        cur.execute(SQL_fetchgrid, (agg_dist, ))
        grid = [x[0] for x in cur.fetchall()]

        import threading

        id_lists = chunkIt(grid, cores)

        print "Closing Existing DB Connection..."
        conn.commit()
        conn.close()

        print "Starting Actual Calcs"
        print "Spawning ", len(id_lists), " threads"

        for i in id_lists:
            t = threading.Thread(target=GiCalcs, args=[gtbl, dtbl, i, kern_dist, kerntype, conn_info, docDict, word_totals, F_All, out_tbl_gi])
            t.start()

        print "All threads executed"

    print "Finished Calculating... check table: "





    

    

    
