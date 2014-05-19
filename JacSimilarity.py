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
def calc(stat_tbl, synfile, conn_info, pct, randits, outf, stat_tbl_func):
    print "Table Containing Statistics: ", stat_tbl
    print "File path containing synsets: ", synfile
    print "which percentil to select: ", pct
    print "Number of iterations used to produce 'random word distribution'", randits
    print "out file that similarity scores will be written to: ", outf

    syn_link = {}

    total_sets = 0
    syn_pairs = set([])
    size_before = 0
    size_after = 0

    with io.open(synfile, 'r', encoding='utf-8') as w:
        for line in w:
            if size_before != size_after:
                total_sets += 1
            size_before = len(syn_pairs)
            row = line.strip().split('\t')
            for i in range(0, len(row)):
                linklist = [row[j] for j in range(0, len(row)) if j!=i]
                addlist = []
                for p in linklist:
                    if p[:len(p)-3] in row[i] and "ing" == p[-3:] or row[i][:len(row[i])-3] in p and "ing" == row[i][-3:]:
                        pass
                        #addlist.append(p)
                        #print row[i], p
                    else: addlist.append(p)
                syn_pairs |= set([row[i]+'|'+m for m in addlist if m+'|'+row[i] not in syn_pairs])
                #syn_pairs += len(linklist)
                syn_link.setdefault(row[i], list()).extend(addlist)
            size_after = len(syn_pairs)


    #for thing in syn_pairs:
    #    print thing
    print total_sets
    print len(syn_pairs)
    #sys.exit()
    #print syn_link["abode"]


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

    for s in syn_pairs:
        s1 = s.split('|')[0]
        s2 = s.split('|')[1]
        if s1 in appearingwords and s2 in appearingwords:
            cur.execute(SQL_Fetch, (s1, ))
            s1_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            cur.execute(SQL_Fetch, (s2, ))
            s2_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            simscore = WeightedJac(s1_dict, s2_dict)
            sim_dict[s] = simscore
        m += 1
        if m % 100 == 0:
            print "Left to go: ", len(syn_pairs) - m
            print datetime.datetime.now()

    print "Number of similarity scores calculated:", len(sim_dict)



    '''for s in syn_link:
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
            print datetime.datetime.now()'''

    print "Done calculating synset similarity scores"

    FuncWordDist = FuncWord_SimDistribution(cur, randits, stat_tbl_func)

    RandWordDist = RandomWord_SimDistribution(syn_link, cur, randits, stat_tbl, appearingwords)

    print len(RandWordDist)

    randfunc_std_dev = numpy.std(FuncWordDist)
    randfunc_pop_mean = numpy.mean(FuncWordDist)
    randfunc_sighat = randfunc_std_dev / math.sqrt(randits)
    

    randword_std_dev = numpy.std(RandWordDist)
    randword_pop_mean = numpy.mean(RandWordDist)
    randword_sighat = randword_std_dev / math.sqrt(randits)

    openw = io.open(outf, 'w', encoding='utf-8')

    print "Writing to outfile"

    for p in sim_dict:
        pval1 = st.norm.sf((sim_dict[p] - randword_pop_mean) / randword_std_dev)
        pval2 = st.norm.sf((sim_dict[p] - randword_pop_mean) / randword_sighat)
        pval3 = st.norm.sf((randword_pop_mean - sim_dict[p]) / randword_std_dev)
        pval4 = st.norm.sf((randword_pop_mean - sim_dict[p]) / randword_sighat)
        #print p, sim_dict[p], pval1, pval2, pval3, pval4

        pval5 = st.norm.sf((sim_dict[p] - randfunc_pop_mean) / randfunc_std_dev)
        pval6 = st.norm.sf((sim_dict[p] - randfunc_pop_mean) / randfunc_sighat)
        pval7 = st.norm.sf((randfunc_pop_mean - sim_dict[p]) / randfunc_std_dev)
        pval8 = st.norm.sf((randfunc_pop_mean - sim_dict[p]) / randfunc_sighat)
        
        #
        openw.write(p +'\t' + str(sim_dict[p]) + '\t' + str(randword_pop_mean) +'\t' + str(randfunc_pop_mean) + '\t' + str(pval1)+'|'+str(pval5) + '\t'+ str(pval2)+'|'+str(pval6) + '\t' + str(pval3)+'|'+str(pval7) + '\t' + str(pval4)+'|'+str(pval8) + '\r\n')

    openw.close()
    conn.close()

def WeightedJac(d1, d2):
    minsum = sum([min(v, d2[k]) for k,v in d1.items()])
    maxsum = sum([max(v, d2[k]) for k,v in d1.items()])
    return (minsum/maxsum)

def FuncWord_SimDistribution(cur, randits, stat_tbl_func):
    print "Creating Function Word Distributions"

    y = 0
    m = 0
    cur.execute("Select Distinct p1.word from %s as p1;" % stat_tbl_func)
    keylist = [x[0] for x in cur.fetchall()]

    print "Number of function words: ", len(keylist)
    randJacScores = []
    already_compared = []
    
    SQL_Fetch = "Select p1.gid, p1.stat from %s as p1 where p1.word = %s" % (stat_tbl_func, '%s')

    while y < randits:
        r1 = random.randint(0, len(keylist)-1)
        s1 = keylist[r1]
        r2 = random.randint(0, len(keylist)-1)
        s2 = keylist[r2]
        if m > 50000:
            print "Possible ininfite loop? Breaking while loop"
            break
        if (s1!=s2) and (s1+'_'+s2 not in already_compared) and (s2+'_'+s1 not in already_compared):
            cur.execute(SQL_Fetch, (s1, ))
            s1_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            cur.execute(SQL_Fetch, (s2, ))
            s2_dict = dict([(x[0], float(x[1])) for x in cur.fetchall()])
            randJacScores.append(WeightedJac(s1_dict, s2_dict))            
            y += 1
            already_compared.append(s1+'_'+s2)
            if y % 10 == 0:
                print "Random Iteration ", y
                print datetime.datetime.now()
            
        m +=1
    print "Done Generating Rand Function Word Similarities"
    return randJacScores

    
    
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
            if y % 10 == 0:
                print "Random Iteration ", y
                print datetime.datetime.now()
    if m >= len(keylist):
        print "Random searches exceeded 2 x keylist"
    print "Done Creating Random Word Distributions"
    return randJacScores


    
            

