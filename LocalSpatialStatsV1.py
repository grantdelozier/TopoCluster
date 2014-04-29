import sys

#sys.path.append('/home/02608/grantdel/pythonlib/lib/python2.7/site-packages')
import psycopg2
import datetime
import io
import math
import numpy

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
        if aggLM==False:
            self.CalcUnigramProb(F_Freq, listuse, whitelist)

    def CalcUnigramProb(self, F_Freq, listuse, whitelist):
        F_Prob = {}
        for word in F_Freq:
            if listuse == "restricted":
                if word in whitelist:
                    F_Prob[word] = (float(F_Freq[word])/float(self.total_words))
            else:
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

def GiCalcs(x):

    gtbl = x[0]
    dtbl = x[1]
    id_list = x[2]
    kern_dist = x[3]
    kerntype = x[4]
    conn_info = x[5]
    docDict = x[6]
    word_totals = x[7]
    F_All = x[8]
    out_tbl = x[9]

    print "Starting Calculation Branch ", id_list[0]

    z = 0

    #Connecting to Database
    conn = psycopg2.connect(conn_info)
    print "DB Connection Success ", id_list[0]

    cur = conn.cursor()

    zero_dict = dict([(x, 0.0) for x in F_All])

    for i in id_list:
        z += 1
        if kerntype.lower() == "uniform":
            rows = KF.Uniform(dtbl, i, kern_dist, cur, gtbl)
        elif kerntype.lower() == "epanech":
            rows = KF.Epanech(dtbl, i, kern_dist, cur, gtbl)
        allData = []
        if len(rows) > 0:
            pid_start = datetime.datetime.now()
            #print "###########", i, "###############"
            #print "Num Neighbors", len(rows)

            newsumDict = dict([(x, 0.0) for x in F_All])

            for p in rows:
                uid = p[0]
                weight = float(p[1])
                for wd in docDict[uid]:
                    newsumDict[wd] = newsumDict.get(wd, 0.0) + (weight * docDict[uid][wd])

            for w in newsumDict:
                if len(w) <= 30:
                    gi_stat = newsumDict[w]/word_totals[w]
                    if newsumDict[w] > word_totals[w]:
                        print "Broken Entry ", w
                        print word_totals[w]
                        print newsumDict[w]
                        print gi_stat
                    allData.append([i, w, gi_stat])

            args_str = ",".join(cur.mogrify("(%s,%s,%s)", x) for x in allData)
                
            qstr = "INSERT INTO %s VALUES " % out_tbl
            cur.execute(qstr + args_str)
                    
        #print datetime.datetime.now()
        if z % 30 == 0:
            print "Left to go: ", (len(id_list) - z), " branch: ", id_list[0]
            print datetime.datetime.now()

    conn.commit()
    conn.close()

def ZavgCalc(x):

    gtbl = x[0]
    dtbl = x[1]
    id_list = x[2]
    kern_dist = x[3]
    kerntype = x[4]
    conn_info = x[5]
    docDict = x[6]
    word_means = x[7]
    word_stds = x[8]
    F_All = x[9]
    out_tbl = x[10]

    print "Starting Calculation Branch ", id_list[0]

    z = 0

    #Connecting to Database
    conn = psycopg2.connect(conn_info)
    print "DB Connection Success ", id_list[0]

    cur = conn.cursor()

    for i in id_list:
        z += 1
        if kerntype.lower() == "uniform":
            rows = KF.Uniform(dtbl, i, kern_dist, cur, gtbl)
        elif kerntype.lower() == "epanech":
            rows = KF.Epanech(dtbl, i, kern_dist, cur, gtbl)
        allData = []
        if len(rows) > 0:
            pid_start = datetime.datetime.now()
            #print "###########", i, "###############"
            #print "Num Neighbors", len(rows)

            newsumDict = dict([(x, 0.0) for x in F_All])
            weightsum = 0

            for p in rows:
                uid = p[0]
                weight = float(p[1])
                weightsum += weight
                for wd in docDict[uid]:
                    newsumDict[wd] = newsumDict.get(wd, 0.0) + (weight * ((docDict[uid][wd]-word_means[wd])/word_stds[wd]))

            for w in newsumDict:
                if len(w) <= 30:
                    allData.append([i, w, newsumDict[w]/float(weightsum)])
            
            args_str = ",".join(cur.mogrify("(%s,%s,%s)", x) for x in allData)
                
            qstr = "INSERT INTO %s VALUES " % out_tbl
            cur.execute(qstr + args_str)
                    
        #print datetime.datetime.now()
        if z % 30 == 0:
            print "Left to go: ", (len(id_list) - z), " branch: ", id_list[0]
            print datetime.datetime.now()

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

    whitelist = set()

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
                    userID = row[0]
                    page_name = row[1]
                    latit = row[2].split(',')[0]
                    longit = row[2].split(',')[1]
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[9].split(" "))
                        #F_All |= set(F_Freq.keys())
                        newDoc = Document(userID, latit, longit, F_Freq, filename, listuse, whitelist)
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
    print "Number of Words Calculating for: ", len(F_All)

    cur = conn.cursor()


    print statistic.lower()
    if statistic.lower() == "zavg":

        print "Beginning Zavg statistic calculation mode"

        out_tbl_zavg = out_tbl + "_zavg"

        cur.execute("CREATE TABLE IF NOT EXISTS %s (gid varchar(20), word varchar(30), stat float);" % (out_tbl_zavg, ))

        cur.execute("DELETE FROM %s ;" % out_tbl_zavg)

        word_means = {}

        word_stds = {}

        word_lists = {}

        print "Calculating word means and std deviations"

        for doc in docDict:
            for word in docDict[doc]:
                word_lists.setdefault(word, list()).append(docDict[doc][word])

        print "Done building word lists"
        
        for word in word_lists:
            alist = []
            alist = word_lists[word]
            numzeros = len(docDict)-len(word_lists[word])
            zerolist = [0.0 for x in range(0, numzeros)]
            alist.extend(zerolist)
            word_stds[word] = numpy.std(alist)
            word_means[word] = sum(alist)/float(len(docDict))

        del word_lists
        del zerolist
        del alist
        print "Done calculating means and std deviations"

        SQL_fetchgrid = "SELECT DISTINCT p1.gid from %s as p1, %s as p2 WHERE st_dwithin(p1.geog, p2.geog, %s);" % (gtbl, dtbl, '%s')
        cur.execute(SQL_fetchgrid, (kern_dist, ))
        grid = [x[0] for x in cur.fetchall()]

        import multiprocessing

        pool = multiprocessing.Pool(cores)

        id_lists = chunkIt(grid, cores)

        print "Closing Existing DB Connection..."
        conn.commit()
        conn.close()

        print "Starting Actual Calcs"
        print "Spawning ", len(id_lists), " processes"

        pool.map(ZavgCalc, [[gtbl, dtbl, i, kern_dist, kerntype, conn_info, docDict, word_means, word_stds, F_All, out_tbl_zavg] for i in id_lists])

        print "Done Executing all processes"

    if statistic.lower() == "gi":

        print "Beginning Gi* statistic calculation mode"

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
        cur.execute(SQL_fetchgrid, (kern_dist, ))
        grid = [x[0] for x in cur.fetchall()]

        import multiprocessing

        pool = multiprocessing.Pool(cores)

        id_lists = chunkIt(grid, cores)

        print "Closing Existing DB Connection..."
        conn.commit()
        conn.close()

        print "Starting Actual Calcs"
        print "Spawning ", len(id_lists), " processes"

        pool.map(GiCalcs, [[gtbl, dtbl, i, kern_dist, kerntype, conn_info, docDict, word_totals, F_All, out_tbl_gi] for i in id_lists])

        #for i in id_lists:
        #    t2 = threading.Thread(target=GiCalcs, args=[gtbl, dtbl, i, kern_dist, kerntype, conn_info, docDict, word_totals, F_All, out_tbl_gi])
        #    t2.start()

        print "Done Executing all processes"

    print "Finished Calculating... check table: ", out_tbl





    

    

    
