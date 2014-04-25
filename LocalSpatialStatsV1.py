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
            self.CalcUnigramProb(self, F_Freq)

    def CalcUnigramProb(self, F_Freq):
        F_Prob = {}
        for word in F_Freq:
            F_Prob[word] = (float(F_Freq[word])/float(total_words))
            self.Feature_Prob = F_Prob

def calc(f, statistic, dtbl, gtbl, conn_info, outf, out_tbl, kern_dist, traintype, grid_freq_min, cores):
    print "Local Spatial Statistics Parameters"
    print "Train file: ", f
    print "Document Table Name: ", dtbl
    print "Document type: ", traintype
    print "Grid Table Name: ", gtbl
    print "Connection Information: ", conn_info
    print "Local Statistic Outfile Name: ", outf
    print "Local Statistic Output Table Name: ", out_tbl
    print "Distance Bandwidth of Kernel Function: ", kern_dist
    print "minimum number of grid points word must appear in: ", grid_freq_min
    print "Which Statistics are being calculated: ", statistic

    print "Number of cores you want to devote to multiprocessing (recommended 1 less than max on system):", cores
    
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

    
