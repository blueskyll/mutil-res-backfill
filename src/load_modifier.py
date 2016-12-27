#!/usr/bin/python
import sys

def changeLoad(file_path, factor):
	print(factor)
	with open(file_path, 'r') as f_in:
		with open(file_path + '.' + str(factor), 'w') as f_out:
			array = []			
			for line in f_in:
				line = line.lstrip()
				if line[0].isdigit():
					array = line.split()
					array[1] = str(int(int(array[1]) * factor))
					print >> f_out, '\t'.join(array)
					array = []
if __name__ == "__main__":
	args = sys.argv
	changeLoad(args[1], float(args[2])) 
				


