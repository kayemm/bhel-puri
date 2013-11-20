import MapReduce
import sys

"""
This application is a map reduce implementation to implement
a SQL Group By operator.
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  
#  Application Usage: 
#
#      python SQLSelect.py arg-1 
#
#      arg-1 : File name containing table data in csv format. 
#              The first row of the file should contain the column names. 
#              The first column of each row should identify the table name. 
#  The application is hard-coded to implement a SQL statement that answers
#  the following question on the 'Posts" data of stack overflow:
#
#  How many questions have a title that falls within one of the following 
#  length(in characters) ranges?
#     1-10  - 
#     10-20 - <impilication-may not be worded properly?>
#     21-30 - 
#     30+   - 
#
#  The SQL statement that would answer the above question would be: 
#  SELECT count(*)
#  FROM (select title, <case..statement>title_len_window
#        from posts) T
#  Group by title_len_window
#  The <case..statement> will be a DB vendor specific implmentation that would
#  return one of the following string based on the length of the title field.
#    a. "1_10" if length is between 1 and 10
#    b. "11_20" if length is between 11 and 20
#    c. "21_30" if length is between 21 and 30
#    d. "31+" if length is greater than 31
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  Algorithm Design:
#
#  Key Idea : Send all records that belong to one group to the same reducer.
#
#  Initial Setup:
#  1. Read the first line of the input file and store the column names 
#     as a dictionary. The key will be the column name and the value will
#     be it's position in the row.
#     This should be done before calling the execute method of MapReduce
#     The MapReduce module is designed to skip the first line of a CSV file.
# 
#  Mapper:
#  1. Input:  One line from the file. This will be a list containing the
#             values of each column of a record.
#  2. Key   : One of the following values based on the length of the title:
#             a. "1_10" if length is between 1 and 10
#             b. "11_20" if length is between 11 and 20
#             b. "21_30" if length is between 21 and 30
#             b. "30+" if length is greater than 30
#     value : (1)
#     Do not emit anything if the Title field is NULL. 
#     Process the record only if PostTypeId is "1"
#
#  Reducer:
#     Emit key along with the number of elements in the list of values passed
#     to the reducer.     
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++       

"""
  Questions:
  1. How would you extend the algorithm to implement the HAVING clause of the
     GROUP BY operator?
          
  Extensions:
  1. The actual StackOverflow data is provided in XML format. Extend the 
     to work directly on the XML data instead of csv. 
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Global list to hold column names
columnNames = {}

# Record Format : A line from the input file.
def mapper(record):
        
    global columnNames
    
    # Extract the title field from the record
    title = record[columnNames['Title']]
    
    # Set titleRange to one of the 4 values listed in the comment above based
    # on the length of the title field
    titleRange = "UNKNOWN"
    
    lenTitle = len(title)
    if (lenTitle > 0 and lenTitle <= 10):
        titleRange = "1_10"
    elif (lenTitle > 10 and lenTitle <= 20):
        titleRange = "11_20"
    elif (lenTitle > 20 and lenTitle <= 30):
        titleRange = "21_30"
    else:
        titleRange = "30+"
    
    # Emit: Key -> lenTitle, value -> 1
    mr.emit_intermediate(titleRange,1)

def reducer(key, list_of_values):
     
    # Emit: key,length of list_of_values
    emitStr = key + "->" + str(len(list_of_values))
    mr.emit(emitStr)

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':

  # Extract the first line from the file to get the column names.
  tableData = open(sys.argv[1])
  firstLine = tableData.readline()
  tableData.close()
  
  # remove the newline at the end of the line
  firstLine = firstLine.replace('\n','')
  columnNamesList = firstLine.split(",")
  count = 0
  for columnName in columnNamesList:
    columnNames[columnName] = count
    count += 1
  
  inputdata = open(sys.argv[1])
  fileNameList = []
  fileNameList.append(sys.argv[1])
  mr.execute(fileNameList, mapper, reducer,"CSV-SkipFirstLine")
