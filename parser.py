import subprocess
import os
import glob
import codecs
MAX_LINES = 1294
BASE_DIR = 'C:\Users\Lori\Documents\CS388\SemesterProject\stanford-ner-2014-01-04'
def GetNer(ner_model):
    
    s = 'C:\\Users\\Lori\\Documents\\CS388\\SemesterProject\\TopCluster\\txt_file\\Puzhuthivakkam.txt'#'C:\Users\Lori\Documents\CS388\SemesterProject\TopCluster\Corpora\enwiki-20130102\enwiki-20130102-permuted-text-only-coord-documents.txt'
    output = codecs.open('C:\Users\Lori\Documents\CS388\SemesterProject\TopCluster\dat_file\\new_annotated_file.txt', 'w', "utf-8");
    subprocess.Popen('java -mx500m -cp '+ BASE_DIR+
                     '\\stanford-ner.jar edu.stanford.nlp.ie.crf.CRFClassifier'+
                     ' -inputEncoding utf-8 -outputEncoding utf-8 -loadClassifier ' +BASE_DIR+
                     '\classifiers\english.conll.4class.distsim.crf.ser.gz -textFile ' + s + 
                     ' -outputFormat slashTags', stdout = output)
    output.close()
    
def create_trainFile(directory):
    os.chdir(directory)
    output = open('C:\Users\Lori\Documents\CS388\SemesterProject\TopCluster\wiki_training.data', 'w')
    for file in glob.glob("*.dat"):
        #GetNer(file.replace(".txt", ""))
        id, dictionary = get_NER_unigrams(file)
        output.write(str(id)+'\t'+file.replace('.dat','')+'\t'+'65,234\t?\t?\t?\t?\t?\t'+str(dictionary).replace('{','').replace("}",'').replace('\'','').replace(',','')+'\n')
    print 'Done creating dat files' 
  
def create_datfiles(directory):
    os.chdir(directory)
    count = 1
    for file in glob.glob("*.txt"):
        GetNer(file.replace(".txt", ""))
        count += 1
    print 'Done creating ' + str(count)+ ' dat files'
    
def create_subfiles(document):
    article_title = ""
    article = None
    count = 0
    with codecs.open(document, 'utf-8') as f:
        for line in f:
            arr = line.split(":")
            if len(arr) != 1:
                if arr[0] == "Article title":
                    if article_title != "":
                        article.close()
                    article_title = arr[1].strip()
                    article_title = article_title.replace("/", '-')
                    article_title = article_title.replace("\\", '-')
                    article_title = article_title.replace("/", '-')
                    article_title = article_title.replace("\"", '')
                    article_title = article_title.replace("\'", '-')
                    article_title = article_title.replace("*", '-')
                    article_title = article_title.replace("?", '-')
                    count+=1
                    try:
                        article = open('C:\Users\Lori\Documents\\CS388\\SemesterProject\\TopCluster\\txt_file\\'+article_title+'.txt', 'w')
                    except:
                        print 'Cannot open ' + article_title
                        article_title = ""
                elif article_title         != "" :
                    article.write(arr[1])
            elif article_title != "":
                article.write(line)
    try:
        article.close()
    except:
        print 'Last text file was closed'
    print "\nCreated " + str(count) +" article files\n"
    
def wiki_NER_tag(document):
    article_title = ""
    article = None
    articleList = []
    with open(document) as f:
        for line in f:
            arr = line.split(":")
            #print str(arr) + "\n"
            if len(arr) != 1:
                if arr[0] == "Article title":
                    if article_title != "":
                        GetNer(article_title)
                        article.close()
                        #get_NER_unigrams('temp.dat')
                        #get_NER_unigrams_thread(thread, article_title)
                    article_title = arr[1].strip()
                    articleList = articleList + [article_title]
                    
                    article = open(article_title+'txt', 'w')#open('C:\Users\Lori\Documents\CS388\SemesterProject\TopCluster\txt_file\\'+article_title+'.txt', 'w')
            else:
                article.write(line)
    article.close()
    GetNer(article_title)
    print "\nFinish Tagging Articles...\n"
               

def create_subset(document):
    linenum = 0
    subset_file = open(r'C:/Users/Lori/Documents/CS388/subset.txt', 'w')
    with open(document) as f:
        for line in f:
            subset_file.write(line)
            linenum +=1
            if linenum >= MAX_LINES:
                break;
    subset_file.close()
    print("Created Subset")
    
    
def get_NER_unigrams(document):
    dictionary = dict()
    id = -1
    with codecs.open(document,'r', 'utf-8') as f:
        found = False
        entity = ""
         
        for line in f:
            token = line.strip().split(" ")
            for item in token: 
                if id == -1:
                    arr = item.split("/")
                    id = arr[0].strip()
                else:
                    arr = item.split("/")
                    if(len(arr) > 1 and arr[1] != "O"):
                        if found == True:
                            entity = entity + "|"+ arr[0]
                        else:
                            entity = arr[0]
                            found = True
                    elif len(arr) > 1:
                        if found == True:
                            if entity in dictionary:
                                dictionary[entity]+=1
                            else:
                                dictionary[entity]=1
                            found = False
                            entity = ""
                        if arr[0] in dictionary:
                            dictionary[arr[0]] += 1
                        else:
                            dictionary[arr[0]] = 1
                
    print "\nCreated Dictionary...."
    jack = codecs.open("jack.txt", 'w', 'utf-8')
    jack.write(str(dictionary).decode('utf-8').replace("'", '').replace('}', '').replace('{',''))                    
    return id, dictionary

get_NER_unigrams('C:\Users\Lori\Documents\CS388\SemesterProject\TopCluster\dat_file\\new_annotated_file.txt')
#create_trainFile('C:\Users\Lori\Documents\\CS388\\SemesterProject\\TopCluster\\dat_file\\')                    
#create_datfiles('C:\Users\Lori\Documents\\CS388\\SemesterProject\\TopCluster\\txt_file\\')
#GetNer('1 Anne Marg')
#get_NER_unigrams('Lori.dat')               
#create_subfiles('C:\Users\Lori\Documents\CS388\SemesterProject\Top'+
#              'Cluster\Corpora\enwiki-20130102\enwiki-20130102-permuted-text-only-coord-documents.txt')
#wiki_NER_tag('subset.txt')
#create_subfiles('subset.txt')