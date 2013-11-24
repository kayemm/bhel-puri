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
#      python recommendBook.py arg-1 
#
#      arg-1:  File in csv format containing existing relationships between
#              books. Each line will contain the IBN of a book followed by a 
#              list (variable size for each book) of books that have been
#              issued along withe the book. 
#  
#  DataSet : Use the book_buddies.csv file within datasets/library_sample 
#  
#  The goal of the application is to find friend-of-a-friend w.r.t. books. For 
#  example if A is a friend of B and B is a friend of C, we would like to 
#  recommend C to A and vice-versa.
#  +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  
#  Algorithm Design:
#
#  Key Idea: 1. Each reducer will focus on a pair of books. (Key will be a pair)
#            2. The o/p of the reducer will be a set of recommendations for 
#               both elements of the pair.
#            3. Given a pair of books (A,B), get all the first level 
#               recommendations(this is your dataset) for both A and B into the
#               same reducer. 
#               Example: Given A->BCE &
#                              B->AD
#                              C->A
#                              D->B
#                              E->A
#                        Reducer (AB) will get ((BCE), (AD))
#                        Reducer (AC) will get ((BCE), (A))
#                        Reducer (AE) will get ((BCE), (A))
#                        Reducer (BD) will get ((AD), (B))
#                        Keys (AB) and (BA) will both have to sent to reducer
#                        processing (AB). How will you do this? 
#  Initial Setup:
#  
#  Mapper:
#  1. Input:  One line from the file. This will be a comma separated list of
#             book ISBNs. The first entry is the book and the rest are 
#             first level recommendations for the book.
#  2. Key   : Given a row of the form B,A,C,D emit the following keys with 
#             the corresponding values:
#             The row represents the relationship B->ACD, implying that
#             books A,C and D are first-level connections of B
#             ---------------------------------
#             |    KEY   |       Value        |
#             ---------------------------------
#             |    AB    |   (B,['A','C','D'])| *note that BA is flipped to AB 
#             |    BC    |   (B,['A','C','D'])|
#             |    BD    |   (B,['A','C','D'])|
#             ---------------------------------
#     
#  Reducer:
#     Each reducer will have two items in its list of values. Given the key AB,
#     one value will be the list of first-level recommedations for A and the 
#     other value will be the list of first-level recommendations for B. 
#     To deduce the second level recommendations for A, we need to extract the 
#     elements from the first level recommendations for B that are not present
#     in A already. In a sense this will be equivalent to the set operation:
#     2nd level recommendations for A =   
#      [first level recommendations for B] - [first level recommendations for A]
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

"""
  Questions:
  1. 
     
  Extensions:
  1. Extend the algorithm to support recommendations based on the weight of the 
     relationships. In this case, given a relationship A->B, the weight will be
     the number of times A and B have been issued by the same person.
     
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Global declarations

# Record Format : A line from the input file.
def mapper(record):
    
    # Split the record to extract all the books 
    booksInRecord = record
    
    # Extract the first element from the row. This is the book for which
    # the row contains a list of recommendations.
    mainBook = booksInRecord[0]
    
    # Extract the recommendations for the book from the record
    recommendations = booksInRecord[1:]
    
    # Iterate over elements 1..n of the record and emit a key,value of the form
    # ((K1,K2) , booksInRecord), where K1 < K2.
    for reco in recommendations:
        if (mainBook < reco):
            key = (mainBook,reco)
        else:
            key = (reco,mainBook)
    
        mr.emit_intermediate(key,booksInRecord)
    
    
def reducer(key, list_of_values):
    # There should be two elements in the list of values. Each of these 
    # elements are lists in themselves. The first element of the list
    # identifies where main book for which the list contains recommendations.
    
    # Split the key info bookA and bookB
    bookA = key[0]
    bookB = key[1]
    
    # Find the recos for first-level recos for bookA and bookB
    firstLevelRecosForA = []
    firstLevelRecosForB = []
    
    for item in list_of_values:
        if (item[0] == bookA):
            firstLevelRecosForA = item
        else:
            firstLevelRecosForB = item
    
    # For bookA find the elements in firstLevelReosForB that are not in 
    # firstLevelRecosForA and vice versa and emit the values as a single
    # string
    newRecosForA = [bookA]
    newRecosForB = [bookB]
    commaStr = ","
    
    for itemB in firstLevelRecosForB:
        for itemA in firstLevelRecosForA:
            if (itemB <> itemA) and (itemB not in firstLevelRecosForA) and (itemB not in newRecosForA):
                newRecosForA.append(itemB)
    # Emit a record only if the list has atleast 2 items
    if (len(newRecosForA) > 1 ):            
        mr.emit(newRecosForA)

    for itemA in firstLevelRecosForA:
        for itemB in firstLevelRecosForB:
            if (itemA <> itemB) and (itemA not in firstLevelRecosForB) and (itemA not in newRecosForB):
                newRecosForB.append(itemA)
                
    # Emit a record only if the list has atleast 2 items
    if (len(newRecosForB) > 1 ):            
        mr.emit(newRecosForB)
    
    

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':

  fileNameList=[]
  fileNameList.append(sys.argv[1])
  mr.execute(fileNameList, mapper, reducer,"CSV")
  