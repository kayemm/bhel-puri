import MapReduce
import sys

"""
Matrix Multiplication Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Application Usage :
#
#   python MatrixMultiply.py arg-1
#
#   arg-1 : File containg data for both the matrices in csv format.
#           Each row of the file represents one element of the matrix. 
#           Row Format:
#               matrix-name,row-number,column-number,cell-value
#               For ex: If A[1][4] = 9, 
#               the cell would be represented as: A,1,4,9
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
# Algorithm :
#
# Key Idea 1: One reducer for each cell of the output matrix.
#             If dimensions of A and B are M x N and N x P respectively, there 
#             will be M*P reducers.
#
# Key Idea 2: Each element of the input matrices will be sent to multiple 
#             reducers (multiple keys will be generated).
#
# Mapper :
#   Each row of A should be sent to every column of the result.
#   => Element A[i,j] should be sent to every element in Result[i,k] 
#      where k = 0..P-1 where P is the number of columns of B.
#   Each column of B should be sent to every row of the result. 
#   => Element B[i,j] should be sent to every element in Result[k,j] 
#      where k = 0..M-1. M is the number of rows of A.
#
# Reducer:
#   Each reducer computes sum(A[i,j]*B[j,k]).
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
"""
  Questions:
  1. How would you handle sparse matrices? For a sparse matrix, you will not 
     have an entry in the input file for matrix elements that do not exist.
  
  Extensions:
  
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def mapper(record):
    # Record Format : [matrix, i, j, value] where matrix is a string 
    # identifying the matrix ('A' or 'B' in this case).
    # i, j are the row and column identifier of matrix cell. 
    # value is the value of cell [i][j]
    # keys: 
    #    For matrix A, set of all (i,k) where k in 0..NUM_COLS_IN_B-1
    #    For matrix B, set of all (k,j) where j  in 0..NUM_ROWS_IN_A-1
    # Value: Full record as received.
    NUM_COLS_IN_B = 3
    NUM_ROWS_IN_A = 9
    matrixName = record[0]
    if (matrixName == 'A'):
        i = record[1]
        for j in range(0,NUM_COLS_IN_B):
            key = (i,str(j))
            mr.emit_intermediate(key,record)
    else:
        # Matrix B
        j = record[2]
        for i in range(0,NUM_ROWS_IN_A):
            key = (str(i),j)
            mr.emit_intermediate(key,record)        
        


def reducer(key, list_of_values):
    # key: Index of element in result matrix.
    # value: An element of matrix A or B in the following format:
    #        [matrix, i, j, value]
    
    total = 0
    # Extract records of 'A'
    matA = []
    for cell in list_of_values:
        if (cell[0] == "A"):
            matA.append(cell)
            
    # Extract records of 'B'
    matB = []
    for cell in list_of_values:
        if (cell[0] == "B"):
            matB.append(cell)
            
    # For each element from A, find all elements from B such that
    # the column number of A matches with the row number of B. (A[i,j]*B[j,k])
    # For each such pair, multiply the values of the cell and add 
    # it to a running total.
    for cellA in matA:
        for cellB in matB:
            if (cellA[2] == cellB[1]):
                total = total + int(cellA[3])*int(cellB[3])
    mr.emit((key[0],key[1], total))

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':
  
  # Invoke the Map Reduce algorithm
  mr.execute(sys.argv[1], mapper, reducer,"CSV")