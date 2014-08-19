'''
Created on May 3, 2014

@author: Lori
'''
import io
class SuperDocument:    
    def __init__(self, input_file = None):
        self.global_dictionary = dict()
        self.global_total = 0L
        if input_file != None:
            with io.open(input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    dictionary = line.split(unicode('\t'))[8].split(unicode(' '))
                    self.add(dictionary)
                    
    '''
    add - takes in a dictionary and adds it to global dictionary
    '''  
    def add(self, dictionary):
        for key, value in dictionary.iteritems():
            if key in self.global_dictionary:
                self.global_dictionary[key] += long(value)
                self.global_total += long(value)
            else:
                self.global_dictionary[key] = long(value)
                self.global_total += long(value)
                
    '''
    print_properties - prints number of unique keys and total number of words (including 
    duplicates in the table
    '''
    def print_properties(self):
        print 'Total Keys: ' + str(len(self.global_dictionary)) +', Total Words: ' + str(self.global_total)