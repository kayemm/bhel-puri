# Original Author : Course staff, "Introduction to data science", coursera.org
# Modified By     : Krishnamoorthy B
# Modifications   : 1. Added support for the following file formats
#                          a. Comma Separated Values
#                          b. XML files containing the StackExchange dump.
#                          c. Plain text files. 
import json
import csv
import xml.etree.ElementTree as ET

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 
    # data - Name of the Input File
    # 
            
    def execute(self, fileName, mapper, reducer,fileFormat):
        if (fileFormat <> "SOXML"):
            data = open(fileName)
        
        if (fileFormat == "JSON"):
            for line in data:
                record = json.loads(line)
                mapper(record)
        
        if (fileFormat == "CSV"):
            csvReader = csv.reader(data,delimiter=',')
            for line in csvReader:
                mapper(line)
                
        if (fileFormat == "TEXT"):
            for line in data:
                mapper(line)
                
        # SOXML is used to identify XML file dumps of StackExchange datasets.
        # In all StackExchange XML files, the main data is stored as attributes
        # of the xml element 'row'. We extract the attributes of the element as
        # a dictionary and pass that to the mapper. The mapper is responsible for
        # extracting the correct attributes from the dictionary.
        if (fileFormat == "SOXML"):
            xmlTree = ET.parse(fileName)
            treeRoot = xmlTree.getRoot()
            for child in root:
                if (child.tag == "row"):
                    mapper(child.attrib)
        
        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        #jenc = json.JSONEncoder(encoding='latin-1')
        
        jenc = json.JSONEncoder()
        
        if (fileFormat == "JSON"):
            for item in self.result:
                print jenc.encode(item)
        else:
            for item in self.result:
                print item
