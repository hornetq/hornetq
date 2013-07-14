'''
Created on May 30, 2013

@author: mcadariu
'''
import os
import re
import fnmatch
import types


class RecursiveGathering(object):
    root_dir = ''
    list_files = []
    filtered_files = []
    
    
        
    def recursive_walk(self,rootdir):
        if(os.path.isfile(rootdir)):
                print rootdir
                self.list_files.append(rootdir)
        else:
            for file in os.listdir(rootdir):
                self.recursive_walk( os.path.join(rootdir,file))
                
    def get_list_of_files_unfiltered(self):
        return self.list_files
    
    def get_list_of_files_filtered(self):
        return_list = []
        
        if self.filtered_files.__len__() == 0:
            prog = re.compile('.*/(.*)\.java')
            for file in self.list_files:
                if(os.path.isfile(file)):
                    result = prog.match(file)
                    if not type(result) is types.NoneType:
                        return_list.append(result.group(1))
            self.filtered_files = return_list;
        return self.filtered_files
    
    def get_assignments_dict(self):
        d={}
        if not self.get_list_of_files_filtered().__len__() == 0:
            for elem in self.get_list_of_files_filtered():
                d[elem]=self.get_list_of_files_filtered().index(elem)
        return d
    
    def get_length(self):
        return self.list_files
    
   
    
    
    def __init__(self,root_dir):
        self.root_dir = root_dir
        self.recursive_walk(root_dir)


    
    
   
    
