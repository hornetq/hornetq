#!/usr/local/bin/python2.7
# encoding: utf-8
'''
filter -- shortdesc

filter is a description

It defines classes_and_methods

@author:     user_name
        
@copyright:  2013 organization_name. All rights reserved.
        
@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import sys
from Analyzer import Analyzer
from Paths import RecursiveGathering


   

if __name__ == "__main__":
    
    analyzer = Analyzer(sys.argv[1])
    files = RecursiveGathering(sys.argv[2])
    file = open('gigi.txt','w')
    file1 = open ('gigi1.txt','w')
    
    for (key,value) in files.get_assignments_dict().iteritems():
        dep_list = analyzer.get_entry_by_file_name(key)
        for elem in dep_list:
            if files.get_assignments_dict().has_key(elem):
                file1.write(key+'->'+elem+'\n')
                file.write(str(files.get_assignments_dict()[key])+'->'+str(files.get_assignments_dict()[elem])+'\n')
       
    
    
        
    
  
    
   
    