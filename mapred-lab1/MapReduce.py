import json
import csv

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper, reducer,fileFormat):
        if (fileFormat == "JSON"):
            for line in data:
                record = json.loads(line)
                mapper(record)
        if (fileFormat == "CSV"):
            csvReader = csv.reader(data,delimiter=',')
            for line in csvReader:
                mapper(line)
        
        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        for item in self.result:
            print jenc.encode(item)
