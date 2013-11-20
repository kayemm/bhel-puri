import MapReduce
import sys

"""
This application is a map reduce implementation to implement a SQL Join.
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 
#  Application Usage:
#
#      python SQLJoin.py arg-1 arg-2
#
#      arg-1:  File name containing table data for Users table in csv format. 
#              The first row of the file should contain the column names. 
#              The first column of each row should identify the table name. 
#      arg-2:  Same as the arg-1 containing data for the Posts table.
#  
#  The application is hard-coded to implement a SQL statement that answers
#  the following question on the 'Posts" and 'Users' data of stack overflow:
#
#  Is it possible that users with a high reputation (>500) only post difficult
#  to answer questions (low number of answers)?   
#  
#  The SQL statement that would provide data to answer the above question 
#  would be: 
#  SELECT Users.DisplayName,Posts.Title, Posts.AnswerCount, Posts.CommentCount
#  FROM Users,Posts
#  WHERE Users.Id = Posts.OwnerUserId AND
#        Users.Reputation > 500
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  
#  Algorithm Design:
#
#  Key Idea: Send matching records from both tables to the same reducer.
#
#  Initial Setup:
#  1. Read the first line of both the input files and store the column names 
#     as a dictionary. The key will be the column name and the value will
#     be it's position in the row.
#     This should be done before calling the execute method of MapReduce
#     The MapReduce module is designed to skip the first line of a CSV file.
#     This should be done for both the tables.
#  Mapper:
#  1. Input:  One line from the file. This will be a list containing the
#             values of each column of a record.
#  2. Key   : For the POSTS table, OwnerUserId
#             For the Users table, Id
#     value : Full record.
#     Do not emit anything if:
#        a. The user Id is NULL in either table.
#        b. Record from post table is not a question.
#           A post can be a question or an answer. The PostTypeId field 
#           identifies a post as a question or an answer. If the value is "1",
#           it is a question.

#  Reducer:
#     In the list of values available in each reducer, there should be only
#     one list item for the user. The remaining list items will be records
#     from the posts table. Extract the DisplayName and Reputation from the 
#     User record and the Title, AnswerCount and CommentCount from each
#     list item that belongs to the posts table. Emit one line for each item
#     belonging to the posts table. 
#     Do not emit anything is the userReputation is less than 500
#       
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

"""
  Questions:
  1. How would you eliminate rows from the result that do not have any data
     from the posts table?
     
  Extensions:
  1. Extend the algorithm to support:
        a. SQL outer joins.
        b. Multi-Way joins on more than one table. You will need to design
           a way for the user to speicy the join columns for each table.
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Global list to hold column names
usersTblColumns = {}
postsTblColumns = {}

# Record Format : A line from the input file.
def mapper(record):
        
    global usersTblColumns
    global postsTblColumns
    
    # Get the column index for column OwnerUserId from the PostsTblColumns dict.
    postsTblJoinCol = postsTblColumns['OwnerUserId']
    
    # Get the column index for column Id from the UsersTblColumns dict.
    usersTblJoinCol = usersTblColumns['Id']
    
    # Determine if the row is from the Users table or from the Posts table.
    # Use the first column to identify the table
    key = ""
    isQuestion = 0
    if (record[0] == "POSTS"):
        key = record[postsTblJoinCol]
        if (record[postsTblColumns["PostTypeId"]] == "1"):
            isQuestion = 1
        
    elif (record[0] == "USERS"):
        key = record[usersTblJoinCol]
        #set isQuestion to "1" for all records from users table.
        isQuestion = 1
        
    if (key <> "" and isQuestion == 1):
        mr.emit_intermediate(key,record)
    
def reducer(key, list_of_values):
    
    # Get the record from the users table from the list of values. 
    userName = ""
    userReputation = "0"
    # we need a minimum of two items in the list to perform a join.
    # Of these one should be from user table and atleast one should
    # be from the posts table. If not, we do not emit anything
    usersTblRecPresent = 0;
    postsTblRecPresent = 0;
    
    if len(list_of_values) > 1 :
        for item in list_of_values:
            if (item[0] == "USERS"):
                userName = item[usersTblColumns['DisplayName']]
                userReputation = item[usersTblColumns['Reputation']]
                usersTblRecPresent = 1
            elif(item[0] == "POSTS"):
                postsTblRecPresent = 1
                
        # Return if we do not find matching recods from both tables 
        if (usersTblRecPresent + postsTblRecPresent <> 2) :
            return
        # Return if user reputation is less than 500
        if (int(userReputation) < 500):
            return
            
        # Emit an output record only for users with reputation         
        # Iterate over all the values and return one record for each 
        # record from posts table
        outStr = ""
        for item in list_of_values:
            if (item[0] == "POSTS"):
                outStr = key + ',' + userName + ',' + userReputation + ","
                outStr += item[postsTblColumns['Title']] + ','
                outStr += item[postsTblColumns['AnswerCount']] + ','
                outStr += item[postsTblColumns['CommentCount']]
                mr.emit(outStr)
                outStr = ""
            

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':

  # Extract the first line from the file to get the column names.
  usersTblData = open(sys.argv[1])
  firstLine = usersTblData.readline()
  usersTblData.close()
  
  # remove the newline at the end of the line
  firstLine = firstLine.replace('\n','')
  columnNamesList = firstLine.split(",")
  count = 0
  for columnName in columnNamesList:
    usersTblColumns[columnName] = count
    count += 1
  
  columnNamesList = []
  firstLine = ""
  postsTblData = open(sys.argv[2])
  firstLine = postsTblData.readline()
  postsTblData.close()
  
  # remove the newline at the end of the line
  firstLine = firstLine.replace('\n','')
  columnNamesList = firstLine.split(",")
  count = 0
  for columnName in columnNamesList:
    postsTblColumns[columnName] = count
    count += 1
  
  
  fileNameList = []
  fileNameList.append(sys.argv[1])
  fileNameList.append(sys.argv[2])
  mr.execute(fileNameList, mapper, reducer,"CSV-SkipFirstLine")
  