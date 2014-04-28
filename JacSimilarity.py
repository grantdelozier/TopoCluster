import sys

#sys.path.append('/home/02608/grantdel/pythonlib/lib/python2.7/site-packages')
import psycopg2
import datetime
import io
import math
import numpy
import scipy.stats as st

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
    

    for s in syn_link:
        #print s
        cur.execute(SQL_Fetch, (s, ))
        s_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
        for s2 in syn_link[s]:
            #print "Comparing - ", s, " vs ", s2
            cur.execute(SQL_Fetch, (s2, ))
            s2_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            simscore = WeightedJac(s_dict, s2_dict)
            sim_dict[s+'|'+s2] = simscore

    print "Done calculating synset similarity scores"

    RandWordDist = RandomWord_SimDistribution(syn_link, cur, randits, stat_tbl)

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
    
def RandomWord_SimDistribution(synlist, cur, randits, stat_tbl):
    print "Creating Random Word Distributions"
    x = 0
    SQL_Fetch = "Select p1.gid, p1.stat from %s as p1 where p1.word = %s" % (stat_tbl, '%s')
    keylist = synlist.keys()
    randJacScores = []
    while x < randits:
        r1 = random.randint(0, len(keylist))
        s1 = keylist[r1]
        r2 = random.randint(0, len(keylist))
        s2 = keylist[r1]
        if s1 != s2 and s1 not in synlist[s2] and s2 not in synlist[s1]:
            cur.execute(SQL_Fetch, (s1, ))
            s1_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            cur.execute(SQL_Fetch, (s2, ))
            s2_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            randJacScores.append(WeightedJac(s1_dict, s2_dict))            
            x += 1
    print "Done Creating Random Word Distributions"
    return randJacScores


    
            
