import MapReduce
import sys

"""
Matrix Multiplication Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# Record Format : [matrix, i, j, value] where matrix is a string 'a' or 'b'.
	# i, j are the row and column identifier of cell 
	# value is the value of cell [i][j]
	# Key passed out will be (a.colId, b.rowid). 
	# Values emitted will be (matrix_name,value)

	matrix_name = record[0]	
	if (matrix_name == 'a'):
		i = record[1]
		for j in [0,1,2,3,4]:
			key = (i,j)
			mr.emit_intermediate(key,record)
	else:
		j = record[2]
		for i in [0,1,2,3,4]:
			key = (i,j)
			mr.emit_intermediate(key,record)		
		


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
	total = 0
	# extract records of 'a'
	matA = []
	for cell in list_of_values:
		if (cell[0] == "a"):
			matA.append(cell)
	matB = []
	# extract records of 'b'
	for cell in list_of_values:
		if (cell[0] == "b"):
			matB.append(cell)

	for cellA in matA:
		for cellB in matB:
			if (cellA[2] == cellB[1]):
				total = total + cellA[3]*cellB[3]
	mr.emit((key[0],key[1], total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)