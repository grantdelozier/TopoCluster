import subprocess
import numpy as np
import sys
import os
from threading import Thread
import glob
import io
#Total files in Document:                     694152
#Current files allocated onto CS Machines:    416844
#True number of articles with coordinates:     330243 
MAX_LINES = 1000    #Default subset of data you want to work with
NUM_OF_THREADS = 50    #Multi-threading purposes, how many threads you want to parse
BASE_DIR = '/v/filer4b/v20q001/lrl542/Desktop/NLP_Project/' #So I don't have to keep re-writing, will have to do fix that in code later

#Thread to implement call the stanford ner parser
#NOTE takes in the name of the file and the coord_prep object for outputting information
'''
GetNer - takes input_file and coord_prep object containing information
    on article and outputs an entity tagged article
@param - ner_model    : input file
@param - coord_obj    : coord_prep object for textfile
'''
def GetNer(ner_model, coord_obj, output_dir):
    output = io.open(output_dir+ner_model+'.dat', 'w', encoding="utf-8");
    p = subprocess.Popen('java -mx500m -cp '+
            BASE_DIR+'stanford-ner.jar edu.stanford.nlp.ie.crf.CRFClassifier '+
            '-loadClassifier '+BASE_DIR+'classifiers/english.conll.4class.distsim.crf.ser.gz '+
            '-inputEncoding utf-8 -outputEncoding utf-8 -textfile /v/filer4b/v20q001/lrl542/Desktop/NLP_Project/txtfile/'
            +ner_model+ '.txt -outputFormat slashTags', shell=True, stderr=subprocess.PIPE,stdout=output)
    p.communicate()
    try:
        p.kill()
    except:
        pass
    output.close()
    get_NER_unigrams(output.name, output_dir+ner_model+'.txt', coord_obj)
    
'''
create_trainFile - merge all files in a directoyr into one file
@params - args - [directory, output_file]
'''    
def create_trainFile(args):
    os.chdir(args[0])
    output = io.open(args[1], 'w', encoding='utf-8')
    for file in glob.iglob("*.txt"):
        with io.open(file, 'r', encoding='utf-8') as temp_file:
            output.write(temp_file.read())
        
    print 'Done creating training file' 

'''
create_datfiles - takes directory and puts files into formatted files
    for merging later into another specified directory
    input files must be in textfile format
@params - args - [coordinate_file, directory_of_textfile, start_file, ending_file, num_of_threads]
'''
def create_datfiles(args):
    doc = args[0]#'/v/filer4b/v20q001/lrl542/Desktop/NLP_Project/enwiki-20130102-permuted-coords.txt'
    global_id_dict = dict()
    try:
        NUM_OF_THREADS = int(args[4])
    except:
        print 'NUM_OF_THREADS not correctly specified: Default NUM_OF_THREADS In : 50'    
    with io.open(doc, 'r', encoding='utf-8') as f:
        try:
            while (True):
                title = f.readline().split(":")[1].strip()
                if not title:
                    break
                id_num = f.readline().split(":")[1].strip()
                coordinate = f.readline().split(":")[1].strip()
                global_id_dict[id_num] = coord_prep(id_num, title, coordinate)
        except:
            print 'Loaded coordinates - ' + str(len(global_id_dict))
    os.chdir(args[1])
    count = 0
    beginning = int(args[2])
    ending =  int(args[3])
    num_of_articles = 0
    total_articles = 0
    threadList = np.array([], dtype=Thread)
    for file in glob.iglob("*.txt"):
        filename = file.replace(".txt", "")
        if filename in global_id_dict:
          if total_articles >= beginning:
            threadList = np.append(threadList, Thread(target=GetNer, args=((filename, global_id_dict[filename], args[5]))))
            count +=1
            num_of_articles += 1 
        else:
            print str(filename) + ' coordinates not loaded'
        total_articles += 1
        if count == NUM_OF_THREADS:
            for x in range(NUM_OF_THREADS):
                threadList[x].start()
            for x in range(NUM_OF_THREADS):
                threadList[x].join()
            count = 0
            threadList = np.array([])
        if total_articles >= ending:
            break;

    if count != 0:
        for x in range(count):
            threadList[x].start()
        for x in range(count):
            threadList[x].join()
    print 'Created '+str(num_of_articles) + ' out of ' + str(ending-beginning)+ ' files' 

'''
create_subfiles - Parses gigantic wiki-file into individual articles with txt files named as their IDs
@param - args - [input_file, output_directory]
'''
def create_subfiles(args):
    article_title = ""
    article = None
    count = 0
    with io.open(args[0], 'r', encoding='utf-8') as f:
        for line in f:
            arr = line.split(":")
            if len(arr) != 1:
                if arr[0] == "Article title":
                    if article_title != "":
                        article.close()
                    count+=1
                    article_title = (arr[1]).strip()
                elif arr[0] == "Article ID":
                    article = io.open(args[1]+str(arr[1]).strip()+'.txt', 'w', encoding='utf-8')
                    article.write(unicode(article_title))
                    article.write(unicode("\n", 'utf-8'))
            
            elif article_title != "":
                article.write(unicode(line))
    try:
        article.close()
    except:
        print 'Last text file was closed'
    print "\nCreated " + str(count) +" article files\n"
               

'''
create_subset - creates a subset of the file document parsed in -- Default is 1000
@param - args - [output_file, input_file, num_of_lines]
'''
def create_subset(args):
    print 'Creating subset of input file...'
    linenum = 0
    subset_file = io.open(args[1], 'w', encoding='utf-8')
    try:
        MAX_LINES = int(args[2])
    except:
        print 'MAX_LINES not correctly specified: Default Max Lines Read In : 1000'
    with io.open(args[0], 'r', encoding='utf-8') as f:
        for line in f:
            subset_file.write(line)
            linenum +=1
            if linenum >= MAX_LINES:
                break;
    subset_file.close()
    print "Created Subset of " + str(linenum) + ' lines'
    
'''
get_NER_unigrams - Creates a dictionary with the frequency of words from the document
@note -  parsed File in form of slashTags, entity are deliminated by '|'
@param - document     : input_file of tagged words from original text file
@param - output     : output stream to write the dictionary out to
@param - coord_obj    : coord_obj that contains Article ID, Tital, and Coordinates
'''    
def get_NER_unigrams(document, output, coord_obj):
    dictionary = dict()
    
    #Output Id, title, and coordinates as well as dummy input
    output = io.open(output, 'w', encoding='utf-8')
    output.write(coord_obj.article_id)
    output.write(unicode('\t'))
    try:
        output.write(coord_obj.article_title.encode('utf-8'))
    except:
        output.write(coord_obj.article_title)
    output.write(unicode('\t'))
    output.write(coord_obj.coordinates)
    output.write(unicode('\t?\t?\t?\t?\t?\t')) 
    
    #Create Dictionary
    with io.open(document, 'r', encoding='utf-8') as f:
        found = False
        entity = ""
        entity_type = ""
        for line in f:
            token = line.strip().split(" ")
            for item in token: 
                arr = item.split("/")
                if(len(arr) > 1 and arr[1] != "O"):
                    if found == True and arr[1].strip() == entity_type:
                        entity = entity + "|"+ arr[0]
                    elif found == True:
                        if entity in dictionary:
                            dictionary[entity]+=1
                        else:
                            dictionary[entity]=1
                        entity = ""
                        entity_type = ""
                    else:
                        entity = arr[0]
                        entity_type = arr[1]
                        found = True
                elif len(arr) > 1:
                    if found == True:
                        if entity in dictionary:
                            dictionary[entity]+=1
                        else:
                            dictionary[entity]=1
                        found = False
                        entity = ""
                        entity_type = ""
                    if arr[0] in dictionary:
                        dictionary[arr[0]] += 1
                    else:
                        dictionary[arr[0]] = 1
        if entity != "":
            if entity in dictionary:
                dictionary[entity]+=1
            else:
                dictionary[entity]=1
#Output Dictionary in key:value notation
    for key, value in dictionary.iteritems():
        try:
            output.write(unicode(key)+':'+unicode(value)+' ')
        except:
            output.write(key.encode('utf-8') + ':'+str(value)+' ')
    output.write(unicode("\n"))                     
    output.close()
    #os.remove(document) #If you do have the space to store the intermediate files remove this
    
'''
create_single_datfiles - Given just input file (not including directory) create format_file
@Note - format_file    : ID Title Coordinate ? ? ? ? ? word1:count1 word2:count2....
@param - args - [input_file, directory_of_pre_formatted_file, file_to_be_formatted ]
'''
def create_single_datfiles(args):
    doc = args[0]
    id_dict = dict() #Needed to hold for ids later in the program
    with io.open(doc, 'r', encoding='utf-8') as f:
         try:
            while (True):
                title = f.readline().split(":")[1].strip()
                if not title:
                    break
                id_num = f.readline().split(":")[1].strip()
                coordinate = f.readline().split(":")[1].strip()
                id_dict[id_num] = coord_prep(id_num, title, coordinate)
         except:
            print 'Loaded coordinates - ' + str(len(id_dict))
    os.chdir(args[1])
    if args[2].strip() in id_dict:
        GetNer(args[2].strip(), id_dict[args[2].strip()], args[3])
    else:
        print 'File ' + str(args[2]) + ' has no coordinates'

'''
coord_prep - Class that stores a txtfile's ID, title and global coordinates
'''
class coord_prep():
    def __init__(self,idnum, title, coord):
        self.article_id = idnum
        self.article_title = title
        self.coordinates = coord
    def toString(self):
        tab = '\t'
        try:
            print str(self.article_id) +tab+unicode(self.article_title)+tab+str(self.coordinates)
        except:
            print str(self.article_id).encode('utf-8')+tab+self.article_title.encode('utf-8')+tab+str(self.coordinates).encode('utf-8')

if __name__ == "__main__":
    try:
        if sys.argv[1] == '-mergeFiles':
            if sys.argv[2] == '-directory' and sys.argv[4] == '-output':
                create_trainFile([sys.argv[3], sys.argv[5]])
            else:
                print 'Bad method call: -mergeFiles -directory directory_name -output file_name'
        elif sys.argv[1] == '-formatFiles':
            if sys.argv[2] == '-coordDoc' and sys.argv[4] == '-directory' and  sys.argv[6] == '-beginFile' and sys.argv[8] == '-endFile' and sys.argv[10] == '-threads' and sys.argv[12] == '-outputDir':
                create_datfiles([sys.argv[3], sys.argv[5], sys.argv[7], sys.argv[9], sys.argv[11], sys.argv[13]])
            else:
                print 'Bad method call: -formatFiles -coordDoc cord_name -directory dir_name -beginFile num -endFile num -threads num -outputDir out_dir'
        elif sys.argv[1] == '-createFiles':
            if sys.argv[2] == '-input' and sys.argv[4] == '-outDir':
                create_subfiles([sys.argv[3], sys.argv[5]])
            else:
                print 'Bad method call: -createFiles -input file_name -outDir out_dir'
        elif sys.argv[1] == '-subset':
            if sys.argv[2] == '-input' and sys.argv[4] == '-output' and sys.argv[6] == '-lines':
                create_subset([sys.argv[3], sys.argv[5], sys.argv[7]])
            else:
                print 'Bad method call: -subset -input file_name -output file_name -lines numLines'
        elif sys.argv[1] == '-singleFormat':
            if sys.argv[2] == '-coordDir' and sys.argv[4] == '-inputDir' and sys.argv[6] == '-input' and sys.argv[8] == '-outputDir':
                create_single_datfiles([sys.argv[3], sys.argv[5], sys.argv[7], sys.argv[9]])
            else:
                print 'Bad method call: -singleFormat -coordDoc cord_name -inputDir dir_name -input input_file -ouputDir out_dir'
        elif sys.argv[1] == '--help':
            print '-mergeFiles : merges all files specified through command line in unicode'
            print '\t-mergeFiles -directory directory_name -output file_name'
            print '\t\tdirectory : directory to merge files'
            print '\t\toutput     : name of output file'
            print '-formatFiles : takes in a directory and parses beginFile to endFile into tagged articles'
            print '\t-formatFiles -coordDoc cord_name -directory dir_name -beginFile num -endFile num -threads num -ouputDir out_dir'
            print '\t\tcoordDoc : name of coordinate document'
            print '\t\tdirectory : directory'
            print '\t\tbeginFile : start file number'
            print '\t\tendFile     : end file number'
            print '\t\tthreads     : number of threads'
            print '\t\touputDir : output directory'
            print '-createFiles : parse gigantic file and put subfiles in directory'
            print '\t-createFiles -input file_name -outDir out_dir'
            print '\t\tinput_file : name of input file'
            print '\t\toutDir     : output directory'
            print '-subset : create subset of large file'
            print '\t-subset -input file_name -output file_name -lines numLines'
            print '\t\tinput     : input file name'
            print '\t\toutput     : output file name'
            print '\t\tlines     : number of lines for subset'
            print '-singleFormat : takes in textfile and outputs it as a formated file'
            print '\t-singleFormat -coordDoc cord_name -inputDir dir_name -input input_file -ouputDir out_dir'
            print '\t\tcoordDoc : name of coordinate document'
            print '\t\tinputDir : input file directory'
            print '\t\tinput     : input file'
            print '\t\toutputDir : output file directory'
        else:
            print 'Type in --help to see method calls'
    except:
        print 'Type in --help to see method calls'