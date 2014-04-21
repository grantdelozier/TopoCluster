import psycopg2
import datetime
import io
import math
import sys
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
    #Feature_Prob = {}
    outside_word_prob = 0
    fileFrom = ""

    def __init__(self, ID, latit, longit, F_Freq, file_from, aggLM=False):
        self.userID = ID
        self.userLat = latit
        self.userLong = longit
        self.Feature_Freq = F_Freq
        self.fileFrom = file_from
        tw = 0
        if aggLM == False:
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

def getAppearsVector(neighbor_vector):
    appear_vector = numpy.where(neighbor_vector <= 0.0, neighbor_vector, 1.0)
    return appear_vector


def buildRef(adict):
    x = 0
    dict_ref = {}
    dict_ref2 = {}
    for w in adict:
        dict_ref[w] = x
        dict_ref2[x] = w
        x += 1
    return dict_ref, dict_ref2

def get_random_gid_dict(gid_dict):
    rand_dict = {k:{} for k in gid_dict.keys()}
    z = 0
    rand_dict_len = len(rand_dict)
    rand_ref = {}
    for ke in rand_dict:
        rand_ref[z] = ke
        z +=1

    for u in gid_dict:
        for w in gid_dict[u]:
            found = False
            while found == False:
                randsel = random.randindt(0, rand_dict_len-1)
                if w not in rand_dict[randsel]:
                    rand_dict[randsel][w] = gid_dict[u][w]
                    found = True
    return rand_dict

        

def MonteCarloMorans(gid_dict, means_dict, iterations, gtbl, kern_dist, cur):
    i = 0
    mc_word_list = {}
    while i <= iterations: 
        random_gid_dict = get_random_gid_dict(gid_dict)
        
        mc_dict2 = MoransCalc(random_gid_dict, gtbl, means_dict, kern_dist, cur)
        for w in mc_dict2:
            mc_word_list[w] = mc_word_list.setdefault(w, list()).append(mc_dict2[w])
        i += 1
    return mc_word_list

def MonteCarloMorans2(gid_dict, means_dict, iterations, neighbor_ref, cores=2):
    i = 0
    import multiprocess

    pool = multiprocessing.Pool(cores)
    
    mc_word_list = {}
    while i <= iterations: 
        results = pool.map(MoransCalc4, [[get_random_gid_dict(gid_dict),means_dict,iterations,neighbor_ref]  in range(1,cores)])
        
        #mc_dict2 = MoransCalc3(random_gid_dict, means_dict, neighbor_ref)
        for w in mc_dict2:
            mc_word_list[w] = mc_word_list.setdefault(w, list()).append(mc_dict2[w])
        i += cores
    return mc_word_list
    
        

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
    #morans_vectors = numpy.zeros((len(mean_vector),1))
    denomsum = numpy.zeros((len(mean_vector),1))
    numerator_sum = numpy.zeros((len(mean_vector),1))
    
    #Should N be equal to total size of gid_dict or the number of gid_dict with at least 1 neighbor?
    #N = float(1)/float(len(gid_dict))
    N = 0
    
    for u in gid_dict:
        #For each u, get its neighbors
        #In future add condition for handling other types of kernel functions
        neighbors = KF.Uniform(gtbl, u, kern_dist, cur, "Only")
        target_vector = getVector(gid_dict[u], ref_dict)
        #print "Num neighbors: ", len(neighbors)
        s1 = set([str(x[0]) for x in neighbors])
        s3 = s1 & set(gid_dict.keys())
        if len(s3) >1:
            N += 1
        #print s3
        m = m + 1
        x = 1
        
        for ui in neighbors:
            gid = str(ui[0])
            if gid in gid_dict and str(gid) != str(u):
                            
                w = float(ui[1])
                            
                total_denom_weights += w
                            
                neighbor_vector = getVector(gid_dict[gid], ref_dict)
                            
                numerator_sum += (w * numpy.multiply(numpy.subtract(target_vector, mean_vector ), numpy.subtract(neighbor_vector, mean_vector )))
                            
                denomsum += numpy.multiply(numpy.subtract(target_vector, mean_vector ), numpy.subtract(target_vector, mean_vector ))
                            
                #div_vector = numpy.divide(numerator, denom)
                
                #sum_vectors += numpy.where(div_vector!=numpy.inf, div_vector, 0.0)
        if m % 50 == 0:
            print "Left to go: ", len(gid_dict) - m
            #print sum_vectors
            #print numerator.sum(axis=0)
            #print denom.sum(axis=0)
            #print sum_vectors.sum(axis=0)
            print datetime.datetime.now()

    i = 0

    N = 1.0/float(N)

    numerator_sum = numpy.multiply((1.0/float(total_denom_weights)), numerator_sum)

    #Should N here be the inverse of the total number of grid points? Or only the number of points with at least 1 neighbor?
    denomsum = numpy.multiply(N, denomsum)
    
    div_vector = numpy.divide(numerator_sum, denomsum)

    morans_vector_wh1 = numpy.where(div_vector==div_vector, div_vector, 0.0)

    morans_vector = numpy.where(morans_vector_wh1!=numpy.inf, morans_vector_wh1, 0.0)
    
    while i < len(morans_vector):
        morans_c[ref_dict2[i]] = morans_vector[i][0]
        i += 1
        
    return morans_c

def MoransCalc2_appears(gid_dict, gtbl, means_dict, kern_dist, cur):
    morans_c = {}

    print "Starting Morans calc 2 in appears mode"

    x = 1
    m = 0

    #First dictionary is word -> index, second is index -> word
    ref_dict, ref_dict2 = buildRef(means_dict)
    
    mean_vector = getVector(means_dict, ref_dict)
    #morans_vectors = numpy.zeros((len(mean_vector),1))
    denomsum = numpy.zeros((len(mean_vector),1))
    numerator_sum = numpy.zeros((len(mean_vector),1))
    total_denom_weights = numpy.zeros((len(mean_vector),1))
    
    #Should N be equal to total size of gid_dict or the number of gid_dict with at least 1 neighbor?
    #N = float(1)/float(len(gid_dict))
    N = numpy.zeros((len(mean_vector),1))
    
    for u in gid_dict:
        #For each u, get its neighbors
        #In future add condition for handling other types of kernel functions
        neighbors = KF.Uniform(gtbl, u, kern_dist, cur, "Only")
        target_vector = getVector(gid_dict[u], ref_dict)
        appears_target_vector = getAppearsVector(target_vector)
        mean_vector2 =  numpy.multiply(appears_target_vector, mean_vector)
        #print "Num neighbors: ", len(neighbors)
        s1 = set([str(x[0]) for x in neighbors])
        s3 = s1 & set(gid_dict.keys())
        
        if len(s3) >1:
            N += appears_target_vector
            denomsum += numpy.multiply(numpy.subtract(target_vector, mean_vector2 ), numpy.subtract(target_vector, mean_vector2 ))
        #print s3
        m = m + 1
        x = 1
        
        for ui in neighbors:
            gid = str(ui[0])
            if gid in gid_dict and str(gid) != str(u):
                            
                w = float(ui[1])
                            
                #total_denom_weights += w
                            
                neighbor_vector = getVector(gid_dict[gid], ref_dict)

                appears_vector = getAppearsVector(neighbor_vector)

                total_denom_weights += appears_vector

                mean_vector3 = numpy.multiply(appears_vector, mean_vector)
                            
                numerator_sum += (w * numpy.multiply(numpy.subtract(target_vector, mean_vector2 ), numpy.subtract(neighbor_vector, mean_vector3 )))
                            
                #div_vector = numpy.divide(numerator, denom)
                
                #sum_vectors += numpy.where(div_vector!=numpy.inf, div_vector, 0.0)
        if m % 50 == 0:
            print "Left to go: ", len(gid_dict) - m
            #print sum_vectors
            #print numerator.sum(axis=0)
            #print denom.sum(axis=0)
            #print sum_vectors.sum(axis=0)
            print datetime.datetime.now()

    i = 0

    #N = 1.0/float(N)

    numerator_sum2 = numpy.divide(numerator_sum, total_denom_weights)

    #Should N here be the inverse of the total number of grid points? Or only the number of points with at least 1 neighbor?
    #denomsum = numpy.multiply(N, denomsum)
    denomsum = numpy.divide(denomsum, N)

    denom_where = numpy.where(denomsum==denomsum, denomsum, 0.0)

    num_where = numpy.where(numerator_sum2==numerator_sum2, numerator_sum2, 0.0)
    
    div_vector = numpy.divide(numpy.where(num_where!=numpy.inf, num_where, 0.0), numpy.where(denom_where!=numpy.inf, denom_where, 0.0))

    #morans_vector = numpy.where(div_vector!=numpy.inf, div_vector, 0.0)

    morans_vector_where = numpy.where(div_vector==div_vector, div_vector, 0.0)

    morans_vector = numpy.where(morans_vector_where!=numpy.inf, morans_vector_where, 0.0)
    
    while i < len(morans_vector):
        morans_c[ref_dict2[i]] = morans_vector[i][0]
        if morans_c[ref_dict2[i]] > 1.0:
            print "#################"
            print ref_dict2[i]
            print ref_dict[ref_dict2[i]]
            print "numerator sum: ", numerator_sum[i][0]
            print "total denom weights: ", total_denom_weights[i][0]
            print "denomsum: ", denomsum[i][0]
            print "N: ", N[i][0]
            print "morans C: ", morans_c[ref_dict2[i]]
            
        i += 1
        
    return morans_c

#This version doesn't need a database connection to work        
def MoransCalc3(gid_dict, means_dict, neighbor_ref):
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
        #print "Num neighbors: ", len(neighbors)
        #s1 = set([str(x[0]) for x in neighbors])
        #s3 = s1 & set(gid_dict.keys())
        #print s3
        m = m + 1
        x = 1
        N = float(1)/float(len(gid_dict))
        if len(neighbors) == 0:
            pass
        for ui in neighbors:
            gid = str(ui[0])
            if gid in gid_dict:
                            
                w = float(ui[1])
                            
                total_denom_weights += w
                            
                neighbor_vector = getVector(gid_dict[gid], ref_dict)
                            
                numerator = w * numpy.multiply(numpy.subtract(target_vector, mean_vector ), numpy.subtract(neighbor_vector, mean_vector ))
                            
                denom = numpy.multiply(N, numpy.multiply(numpy.subtract(target_vector, mean_vector ), numpy.subtract(target_vector, mean_vector )))
                            
                div_vector = numpy.divide(numerator, denom)
                
                sum_vectors += numpy.where(div_vector!=numpy.inf, div_vector, 0.0)
        if m % 50 == 0:
            print "Left to go: ", len(gid_dict) - m
            #print sum_vectors
            #print numerator.sum(axis=0)
            #print denom.sum(axis=0)
            #print sum_vectors.sum(axis=0)
            print datetime.datetime.now()

    i = 0
    while i < len(sum_vectors):
        morans_c[ref_dict2[i]] = sum_vectors[i][0]/float(total_denom_weights)
        i += 1
        
    return morans_c
                    
        
        

def calc(f, dtbl, gtbl, conn_info, outf, agg_dist, kern_dist, traintype, writeAggLMs, UseAggLMs, writeAggFile, sig_test, neighbor_ref_file, mean_method):

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
    print "Significance testing?: ", sig_test
    print "neighbor_ref_file?: ", neighbor_ref_file
    print "mean calculation method: ", mean_method

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
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[2].split(" "))
                        F_All |= set(F_Freq.keys())
                        newDoc = Document(userID, latit, longit, F_Freq, filename, UseAggLMs)
                        docDict[userID] = newDoc
                    elif UseAggLMs == True:
                        F_Freq = dict([f.split(':')[0],float(f.split(':')[1])] for f in row[2].split(" "))
                        docDict[userID] = F_Freq
                elif traintype == "wiki":
                    userID = row[0]
                    page_name = row[1]
                    latit = row[2].split(',')[0]
                    longit = row[2].split(',')[1]
                    if UseAggLMs == False:
                        F_Freq = dict([f.split(':')[0],int(f.split(':')[1])] for f in row[9].split(" "))
                        F_All |= set(F_Freq.keys())
                        newDoc = Document(userID, latit, longit, F_Freq, filename, UseAggLMs)
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
        #Fetch all points in our grid that have at least one document in them
        gid_lat_long_ref = {}
        print "Building Grid"
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
                gid_totalwords[str(u[0])] = sum([docDict[x[0]].total_words for x in docs])
                gid_dict[str(u[0])] = reduce(updateInPlace, (Counter(docDict[x[0]].Feature_Freq) for x in docs))
                #s1 = [str.join('', [' ', k, ':', unicode(v)]) for k,v in gid_dict[u[0]].items()]
                #print s1
                #s2 = str.join('', s1)
                #print s2
                #print "#############"
                gid_lat_long_ref[str(u[0])] = [u[1], u[2]]
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
        grid_freqs = {}

        #Simple MLE Unigram estimate
        #In the future allow for other types of Prob Estimates
        for i in gid_dict:
            for w in gid_dict[i]:
                #Freq(word in doc)/Freq(all words in doc)
                gid_dict[i][w] = float(gid_dict[i][w])/float(gid_totalwords[i])
                #Mean Unigram Prob among all points in grid
                means_dict[w] = means_dict.get(w, 0) + gid_dict[i][w]
                grid_freqs[w] = grid_freqs.get(w, 0) + 1


        if mean_method == "appears":
            for wd in means_dict:
                means_dict[wd] = float(means_dict[wd]) / float(grid_freqs[wd])
        elif mean_method == "all":
            for wd in means_dict:
                means_dict[wd] = float(means_dict[wd]) / float(len(gid_dict))

        print "Done Calculating Means"

        print "Writing to aggregated grid file"
        #If option is set, write the grid aggregated documents to a file (can speed up subsequent loads if multiple calcs done on same file)
        #The output here can be read in as a train file and previous steps can be skipped for future loads.
        if writeAggLMs==True:
            openw = io.open(writeAggFile, 'w', encoding='utf-8')
            #cur.execute("SELECT gid,  FROM ")
            for i in gid_dict:
                s1 = [str.join('', [' ', k, ':', unicode(v)]) for k,v in gid_dict[i].items()]
                #print s1
                s2 = str.join('', s1)
                #s2 = ""
                #s2.join(s1)
                #print s2
                lat_long = unicode(gid_lat_long_ref[i][0])+','+unicode(gid_lat_long_ref[i][1])
                openw.write(unicode(str(i), 'utf-8') + '\t' + lat_long + '\t' + s2.strip() + '\r\n')
            openw.close()
            print "Done writing to file"
    #sys.exit()

    if UseAggLMs == True:
        cur = conn.cursor()
        print "Using Aggregated LMs..."
        gid_dict = docDict
        print "Getting Mean probs"
        means_dict = {}
        grid_freqs = {}

        #testw = io.open('test_write.txt', 'w', encoding='utf-8')
        
        for i in gid_dict:
            for w in gid_dict[i]:
                #Mean Unigram Prob among all points in grid
                #testw.write(w + '\t' + str(gid_dict[i][w]) + '\r\n')
                means_dict[w] = means_dict.get(w, 0) + gid_dict[i][w]
                grid_freqs[w] = grid_freqs.get(w, 0) + 1
        #testw.close()
        if mean_method == "appears":
            for wd in means_dict:
                means_dict[wd] = float(means_dict[wd]) / float(grid_freqs[wd])
        elif mean_method == "all":
            for wd in means_dict:
                means_dict[wd] = float(means_dict[wd]) / float(len(gid_dict))
        print "Done obtaining means probs"
        
            
            
    print "Getting Moran's scores"

    print "Number of words: ", len(means_dict)

    #If specified this will run the morans calculations using a neighbor reference file
    #Intended for use on supercomputer where DB connection is unavailable
    if neighbor_ref_file != "None":

        neighbor_ref = {}
        with io.open(neighbor_ref_file, 'r', encoding='utf-8') as w:
            for line in w:
                row = line.strip().split('\t')
                if len(row) > 1:
                    refs = row[1].split('|')
                    neighbor_ref[row[0]] = refs
                else: neighbor_ref[row[0]] = []
        mc_dict = MoransCalc3(gid_dict, means_dict, neighbor_ref)

        wf = io.open(outf, 'w', encoding='utf-8')
        sorted_mc_dict = sorted(mc_dict.items(), key=operator.itemgetter(1), reverse=True)
        for mc in sorted_mc_dict:
            try:
                wf.write(mc[0] + '\t' + str(grid_freqs[mc[0]]) + '\t' + str(mc[1]) + '\r\n')
            except:
                print "problem writing string", mc[0], str(grid_freqs[mc[0]]), mc[1]

        wf.close()

        print "Done writing moran's scores to outfile"

        if sig_test == True:
            print "Beginning Significance Tests"
            #Need to parameterize this in the future
            iterations = 100
            mc_word_list = MonteCarloMorans2(gid_dict, means_dict, neighbor_ref, iterations) 
        
    elif neighbor_ref_file == "None":
        if mean_method == "appears":
            mc_dict = MoransCalc2_appears(gid_dict, gtbl, means_dict, kern_dist, cur)
        else: mc_dict = MoransCalc2(gid_dict, gtbl, means_dict, kern_dist, cur)

        wf = io.open(outf, 'w', encoding='utf-8')

        sorted_mc_dict = sorted(mc_dict.items(), key=operator.itemgetter(1), reverse=True)
        for mc in sorted_mc_dict:
            try:
                wf.write(mc[0] + '\t' + str(grid_freqs[mc[0]]) + '\t' + str(mc[1]) + '\r\n')
            except:
                print "problem writing string", mc[0], str(grid_freqs[mc[0]]), mc[1]

        wf.close()

        print "Done writing moran's scores to outfile"

        if sig_test == True:
            print "Beginning Significance Tests"
            #Need to parameterize this in the future
            iterations = 100
            mc_word_list = MonteCarloMorans(gid_dict, means_dict, iterations, gtbl, kern_dist, cur) 


    conn.close()
    
    
    
