'''
Created on May 30, 2013

@author: mcadariu
'''
import sys
import os
import xml.etree.ElementTree as ET
import re
from path import path

class Analyzer(object):
    
    d={}
    files_hash = {}
    file_id = 0
    
   
    def create_entry(self,file_name,children_list):
        
        '''Assign an unique ID to children'''
        for child in children_list:
            if not self.files_hash.has_key(child):
             
                self.file_id = self.file_id + 1
                self.files_hash[child]=self.file_id
                
                
        
        '''Assign unique ID to input file_name'''
        if not self.files_hash.has_key(file_name):
                  
            self.file_id = self.file_id + 1
            self.files_hash[file_name]=self.file_id
                
        
        
        self.d[file_name]=children_list
        
    def get_entry_by_file_name(self,file_name):
        if not self.d.has_key(file_name):
            return []
        return self.d[file_name]
    
    def get_hash_value_by_file(self,file_name):
        return self.files_hash[file_name]
    
    def transform(self,full_name):
        prog = re.compile('[a-zA-Z.]*\.([a-zA-Z]*)')
        result = prog.match(full_name)
        return result.group(1)
    
    def get_length(self):
        return self.d.items()
    
    def __init__(self,path_to_xml):
        self.path_to_xml = path_to_xml
        tree = ET.parse(path_to_xml)
        root = tree.getroot()
        for neighbor in root.iter('type'):
            file_name = neighbor.attrib['name']
            file_name_transformed = self.transform(file_name)
            list_children = []
            for child in neighbor.iter('reference'):
                result = self.transform(child.attrib['type'])
                list_children.append(result)
                self.create_entry(file_name_transformed, list_children)
    
