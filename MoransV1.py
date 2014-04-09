import psycopg2
import datetime
import io

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

def calc(f, dtbl, gtbl, conn, outf):
    
    filename = f[f.rfind('/')+1:]
    print filename
    begin_time = datetime.datetime.now()
    docDict = {}
    x = 0
    y = 0
    F_All = set()

    read_time_begin = datetime.datetime.now()

    #Read in the trainfile data/calc word frequencies
    with io.open(f, 'r', encoding='utf-8') as f:
        for person in f:
            #print "####NEW Person####"
            #print userID, latit, longit
            try:
                row = person.split('\t')
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
                    F_Freq = dict(f.split(':') for f in row[3].split(" "))
                
                F_All |= set(F_Freq.keys())
                newDoc = Document(userID, latit, longit, F_Freq, filename)
                docDict[userID] = newDoc
            except:
                print "@@@@@error reading user@@@@@@"
                print row
                print z
                break
            z += 1
            if z % 10000 == 0:
                print z
                print datetime.datetime.now()

    print "------Done reading in the data-------"
    read_time_end = datetime.datetime.now()
    print read_time_end - read_time_begin

    #Aggregate Language Models in grid
    #import KernelFunctionsV1 as KF

    #grid = 
    
    
    
