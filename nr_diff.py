import sys
import os
import json
import datetime

def removeDuplications(theList):
	newList = []
	for n in theList:
		if not n in newList:
			newList.append(n)
	return sorted(newList)
def getDiffPjson():
	start = datetime.datetime.now()
	print ("Start time: ",start)

	filename = (str(start).split(' ')[0]) + ".pjson"

	#os.system("perl nr2pjson.pl >" + filename)
	os.system("rm current_nr.pjson")
	os.system("ln -s " + filename + " current_nr.pjson")


	os.system("diff current_nr.pjson latest_nr.pjson >nr_diff.ppjson")

	out = open("nr_diff.pjson","w")

	filein = open("nr_diff.ppjson","r")
	for line in filein:
		if line.startswith(">"):
			line = line[1:].strip()+"\n"
			out.write(line)

	filein.close()
	out.close()
	print ("Start time: ",start)
	print ("Finish time: ",datetime.datetime.now())

	os.system("ln -s " + filename + "latest_nr.pjson")

getDiffPjson()
