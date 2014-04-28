import sys

#sys.path.append('/home/02608/grantdel/pythonlib/lib/python2.7/site-packages')
import psycopg2
import datetime
import io
import math
import numpy
import scipy.stats as st
import random

#Main method
def calc(stat_tbl, synfile, conn_info, pct, randits, outf):
    print "Table Containing Statistics: ", stat_tbl
    print "File path containing synsets: ", synfile
    print "which percentil to select: ", pct
    print "Number of iterations used to produce 'random word distribution'", randits
    print "out file that similarity scores will be written to: ", outf

    syn_link = {}

    with io.open(synfile, 'r', encoding='utf-8') as w:
        for line in w:
            row = line.strip().split('\t')
            for i in range(0, len(row)):
                linklist = [row[j] for j in range(0, len(row)) if j!=i]
                syn_link.setdefault(row[i], list()).extend(linklist)

    print syn_link["abode"]

    #Connecting to Database
    conn = psycopg2.connect(conn_info)
    print "Connecton Success"

    cur = conn.cursor()

    sim_dict = {}

    SQL_Fetch = "Select p1.gid, p1.stat from %s as p1 where p1.word = %s" % (stat_tbl, '%s')

    print "Calculating Similarity Scores for elements in synsets"

    m = 0

    cur.execute("SELECT DISTINCT word from %s;" % stat_tbl)
    appearingwords = [w[0] for w in cur.fetchall()]

    for s in syn_link:
        #print s
        if s in appearingwords:
            cur.execute(SQL_Fetch, (s, ))
            s_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            for s2 in syn_link[s]:
                if s2+'|'+s not in sim_dict:
                    if s2 in appearingwords:
                        #print "Comparing - ", s, " vs ", s2
                        cur.execute(SQL_Fetch, (s2, ))
                        s2_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
                        simscore = WeightedJac(s_dict, s2_dict)
                        sim_dict[s+'|'+s2] = simscore
        m += 1
        if m % 200 == 0:
            print "Left to go: ", len(syn_link) - m
            print datetime.datetime.now()

    print "Done calculating synset similarity scores"

    RandWordDist = RandomWord_SimDistribution(syn_link, cur, randits, stat_tbl, appearingwords)

    print len(RandWordDist)

    std_dev = numpy.std(RandWordDist)
    pop_mean = numpy.mean(RandWordDist)
    sig_hat = std_dev / math.sqrt(randits)

    openw = io.open(outf, 'w', encoding='utf-8')

    print "Writing to outfile"

    for p in sim_dict:
        pval1 = st.norm.sf((sim_dict[p] - mean) / std_dev)
        pval2 = st.norm.sf((sim_dict[p] - mean) / sighat)
        pval3 = st.norm.sf((mean - sim_dict[p]) / std_dev)
        pval4 = st.norm.sf((mean - sim_dict[p]) / sighat)
        print p, sim_dict[p], pval1, pval2, pval3, pval4
        openw.write(p +'\t' + str(sim_dict[p]) + '\t' + str(pop_mean) + '\t' + str(pval1) + '\t'+ str(pval2) + '\t' + str(pval3) + '\t' + str(pval4) + '\r\n')

    openw.close()
    conn.close()

def WeightedJac(d1, d2):
    minsum = sum([min(v, d2[k]) for k,v in d1.items()])
    maxsum = sum([max(v, d2[k]) for k,v in d1.items()])
    return (minsum/maxsum)
    
def RandomWord_SimDistribution(synlist, cur, randits, stat_tbl, appearingwords):
    print "Creating Random Word Distributions"
    y = 0
    m = 0
    SQL_Fetch = "Select p1.gid, p1.stat from %s as p1 where p1.word = %s" % (stat_tbl, '%s')
    keylist = [x for x in synlist.keys() if x in appearingwords]
    print len(appearingwords)
    print "keylist length: ", len(keylist)
    print (y < randits)
    randJacScores = []
    while y < randits:
        if m > len(keylist)*2:
            break
        r1 = random.randint(0, len(keylist)-1)
        s1 = keylist[r1]
        r2 = random.randint(0, len(keylist)-1)
        s2 = keylist[r2]
        #print r2
        #print s1, s2
        m += 1
        if s1 != s2 and s1 not in synlist[s2] and s2 not in synlist[s1]:
            #print s1, s2
            cur.execute(SQL_Fetch, (s1, ))
            s1_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            cur.execute(SQL_Fetch, (s2, ))
            s2_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            randJacScores.append(WeightedJac(s1_dict, s2_dict))            
            y += 1
            if x % 10 == 0:
                print "Random Iteration ", x
                print datetime.datetime.now()
    if m >= len(keylist):
        print "Random searches exceeded 2 x keylist"
    print "Done Creating Random Word Distributions"
    return randJacScores


    
            

