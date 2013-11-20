import MapReduce
import sys

"""
This application is a map reduce implementation to find duplicate unages in a 
corpus of small images.
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 
#  Application Usage:
#
#      python DuplicateImages.py arg-1 
#
#      arg-1 : File containing list of images to be processed. Each line of the 
#              file should contain one image file name (including path)
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  
#  Algorithm Design:
#  
#  Key Idea: Use the pixel array representation of the full image as a key. Send all
#            images with the same pixel array representation to the same reducer.
#
#  Mapper:
#  1. Input:  One line from the file. This will be a list containing the
#             values of each column of a record.
#  2. Key   : AnswerCount
#     value : (Title, Score, ViewCount, CommentsCount)
#     Do not emit anything if the AnswerCount field is NULL. 
#  Reducer:
#     Emit an output only if the key is zero.     
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++       

"""
  Questions:
  1. 
     
  Extensions:
  1. 
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def mapper(record):
  # record : List with two elements. File name and image data as a list. 
  fileName = record[0]  
  imageData = record[1]
  
  # key -> string representation of imageData, value -> fileName
  
  mr.emit_intermediate(str(imageData),fileName)

def reducer(key, list_of_values):
    str = ""
    for name in list_of_values:
        str += name + ","
    mr.emit(str)

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':

  # Build list of image files to be processed
  fileNameList = []
  inputFiles = open(sys.argv[1])
  for line in inputFiles:
    fileNameList.append(line.strip())
    
  mr.execute(fileNameList, mapper, reducer,"IMAGE")
