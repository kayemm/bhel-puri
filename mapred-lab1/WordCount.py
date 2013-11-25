import MapReduce
import sys

"""
This application is a map reduce implementation to count the number
of words in a  document.
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
# Application Usage
#
#   python WordCount.py arg-1 arg-2
#
#   arg-1 : File on which word count is to be performed.
#   arg-2 : File containing list of common words.
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
# Algorithm:
#
#   Key Idea: Send all occurences of the same word to the same reducer.
#
#   Initial Setup:
#     Store the list of words to be eliminated from the count in a global
#     list named commonWordsList. 
#
#   Mapper:
#     Each invocation of the mapper will be provided one line from the  
#     input file. The mapper should break the line into words and store 
#     them in a list called wordsInLine. It should add a word to the list
#     only if it is not in the common words list. Once the list is
#     created, each item of the list will be emitted as a key with a
#     value of 1.
#
#  Reducer:
#     The number of elements in the list of values will be equal to the number
#     of occurences of the word. Emit the key and the length of the list
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
"""
   Questions:
   1. What happens if the same word repeats more than once in a line?
   2. How will you handle case sensitivity of the terms?
   3. How would you split the line into words without the punctuation characters?
   4. How would you modify the application to work on a set of input files 
      instead of a single file. 
   5. Is it possible to sort the final output by the number of occurences in 
      descending order? (The most frequent word is at the top).
   Extensions:
   1. Use standard stemming techniques to convert to convert a word to it's 
      root form. This will improve the usefulness of the search engine you can 
      build on top of this.
   
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Initialize commonWordList
commonWords = []

def mapper(line):
    # line format : A line from the input file.
    global commonWordsList
    
    # Split the words in the line using the split() method on a string.
    allWordsInLine = line.split()
    
    # Initialize the wordsInLine list
    wordsInLine = []
    
    # Iterate over the allWordsInLine list and add the word to the wordsInLine
    # list only if it is not a common word. (not in commonWords)
    for word in allWordsInLine:
        if word not in commonWords:
            wordsInLine.append(word)
    
    # For each word in wordsInLine emit key -> word, value -> 1
    for word in wordsInLine:
        mr.emit_intermediate(word,1)
       

def reducer(key, list_of_values):
    # list_of_values will be a list with one entry for each value emitted by 
    # the mapper. In this case each of these values will be "1". The number
    # of occurences of the key is hence the length of the list
    # Emit key and the length of the list of values.
     
    numOccurences = len(list_of_values)
    mr.emit((key,numOccurences))
    
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':

  # Build the list of common words by opening
  commonWordsFile = open(sys.argv[2])
  for line in commonWordsFile:
    words = line.split()
    for word in words:
        commonWords.append(word)
        
  # Invoke the Map Reduce algorithm. The execute method expects 4 parameters
  # 1. File containing the data
  # 2. Name of the map routine
  # 3. Name of the reduce routine
  # 4. Type of file. Values can be TEXT, JSON, SOXML, CSV.
  mr.execute(sys.argv[1], mapper, reducer,"TEXT")
