import sys
import io
import subprocess
import os
BASE_DIR = 'C:\\Users\\Lori\\Documents\\CS388\\SemesterProject\\TopCluster\\SourceCode\\' #Must modify based on location

'''
broken_files - Identifies files that were not parsed correctly stores IDs in output file, prints count of broken files
@params - args - [input_file, output_file]
'''
def broken_files(args):
    print 'Identifying Broken Files...'
    broken = io.open(args[1], 'w', encoding='utf-8')
    count = 0
    total = 0
    with io.open(args[0], 'r', encoding='utf-8') as large_file :
        for record in large_file:
            line = record.split(unicode('\t'))
            if record != unicode('\n') and len(line) == 9:
                broken.write(record)
                count += 1
            try:
                val = int(line[0])
            except:
                broken.write(record)
                count += 1
            total+=1
    broken.close()
    print str(count) + ' out of ' + str(total) +' bad files'

'''
fix_current_file - creates a new output file by fixing the broken files 
NOTE - dependent upon having the original broken files in the non-parsed
form loaded on disk somewhere
@params - [input_file, output_file]
'''
def fix_current_file(args):
    print 'Fixing Current File: ' + args[0]
    broken = io.open(args[1], 'w', encoding='utf-8')
    total = 0
    count = 0
    with io.open(args[0], 'r', encoding='utf-8') as large_file :
        for record in large_file:
            line = record.split(unicode('\t'))
            if len(line[8]) < 4:
                GetNer(broken, line[0], record)
                count += 1
                if count % 10 == 0:
                    print str(count) + ' files fixed...'
            else:
                broken.write(record)
            total += 1
    broken.close()
    print 'Successfully Fixed ' + str(count) + ' files...'

'''
rm_duplicate - creates a new output file by removing any lines that are duplicated based
on Article ID 
@params - [input_file, output_file]
'''
def rm_duplicate(args):
        dictionary = dict()
        output = io.open(args[1], 'w', encoding='utf-8')
        line_num = 0
        count = 0
        with io.open(args[0] , 'r', encoding='utf-8') as parse_file:
            for line in parse_file:
                token = line.split(unicode('\t'))
                if token[0] in dictionary:
                    print 'Duplicate ID: '+token[0] + ' Line Number: ' + str(line_num) + ' Original: ' + str(dictionary[token[0]])
                    count += 1
                else:    
                    output.write(line)
                    dictionary[token[0]] = line_num
                
        output.close()
        print str(count) + ' duplicate lines'
        
def GetNer(output_file, file_name, original_data):
    output = io.open('C:\\Users\\Lori\\Documents\\CS388\\SemesterProject\\TopCluster\\dat_file\\'+file_name+'.dat', 'w', encoding="utf-8");
    p = subprocess.Popen('java -mx500m -cp '+
            BASE_DIR+'stanford-ner.jar edu.stanford.nlp.ie.crf.CRFClassifier '+
            '-loadClassifier '+BASE_DIR+'english.conll.4class.distsim.crf.ser.gz '+
            '-inputEncoding utf-8 -outputEncoding utf-8 -textfile C:\\Users\\Lori\\Documents\\CS388\\SemesterProject\\TopCluster\\txt_file\\'
            +file_name+ '.txt -outputFormat slashTags',  stderr=subprocess.PIPE, stdout=output)
    p.communicate()
    p.kill()
    output.close()
    get_NER_unigrams(output.name, output_file, original_data)

def get_NER_unigrams(document, output,original_data):
    dictionary = dict()
    new_header = original_data.replace(unicode("\n"), '')
    output.write(new_header)
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
        
    for key, value in dictionary.iteritems():
        try:
            output.write(unicode(key)+':'+unicode(value)+' ')
        except:
            output.write(key.encode('utf-8') + ':'+str(value)+' ')
    output.write(unicode("\n"))                     
    os.remove(document)
    
'''
merge - given an output file, merges all the files in the list to that output file
@params - args - [output_file, file1, file2,...]
'''    
def merge(args):
    new_file = io.open(args[0], 'w', encoding = 'utf-8')
    parse_files = args[1:]
    print 'Merging files: ' + str(parse_files)
    for file in parse_files:
        with io.open(file, 'r', encoding='utf-8') as partial_file:
            for line in partial_file:
                new_file.write(line)
    new_file.close()
    print 'Created new Wiki-File'
if __name__ == '__main__':
    #sys.argv = ['','-identify','-input',"C:\Users\Lori\Documents\CS388\SemesterProject\TopCluster\\wiki_training_file.txt",'-output', "check.txt"]
    try:
        if sys.argv[1] == '-rm_duplicate':
            if sys.argv[2] == '-input' and sys.argv[4] == '-output':
                rm_duplicate([sys.argv[3], sys.argv[5]])
            else:
                print 'Bad method call: -fix -input file_name -output file_name'
        elif sys.argv[1] == '-fix':
            if sys.argv[2] == '-input' and sys.argv[4] == '-output':
                fix_current_file([sys.argv[3], sys.argv[5]])
            else:
                print 'Bad method call: -fix -input file_name -output file_name'
        elif sys.argv[1] == '-identify':
            if sys.argv[2] == '-input' and sys.argv[4] == '-output':
                broken_files([sys.argv[3], sys.argv[5]])
            else:
                print 'Bad method call: -identify -input file_name -output file_name'
        elif sys.argv[1] == '-merge':
            if sys.argv[2] == '-output' and sys.argv[4] == '-files':
                merge([sys.argv[3]] + sys.argv[5:])
            else:
                print 'Bad method call: -merge -output file_name -files file1 file2....'
        elif sys.argv[1] == '--help':
            print '-merge : merges all files specified through command line in unicode'
            print '\t-merge -output output_file -files file1 file2...'
            print '\t\toutput_file : name of output file'
            print '\t\tfile1 file2... : name of files'
            print '-fix : fixes the broken file parses in input_file -- Dependent on files loaded on disk'
            print '\t-fix -input input_file -output output_file'
            print '\t\tinput_file : name of input file'
            print '\t\toutput_file : name of output file'
            print '-identify : identify the number of files not parsed correctly and removes them'
            print '\t-identify -input file_name -output file_name'
            print '\t\tinput_file : name of input file'
            print '\t\toutput_file : name of output file'
            print '-rm_duplicate : identifies duplicate lines and removes them'
            print '\t-rm_duplicate -input file_name -output file_name'
            print '\t\tinput_file : name of input file'
            print '\t\toutput_file : name of output file'
        else:
            print 'Type in --help to see method calls'
    except:
        print 'Type in --help to see method calls'