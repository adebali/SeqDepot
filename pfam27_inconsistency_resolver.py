import os
import sys
from pymongo import MongoClient

aseqsCol = seqdepotDB["aseqs"]


allrecords = aseqsCol.find({},timeOut = False)
#allrecords = aseqsCol.find({"_id":"CFuX83oc9Sj5rtEXC_nMZA"},timeOut = False)
count = 0
for i in range(allrecords.count()):
#	print(i)
	record = next(allrecords)
	aseq = record["_id"]
	status = record["_s"]
	#print(status[-1:])
	#if status[-1:] == "d" and not "pfam27" in record["t"].keys():
	#if status[19] == "d" and status[8] == "T":
	if status[19] == "d":
		count += 1
		if count%100000==0: print(count)
		#print(record["_id"])
		#newStatus = status[:-1] + "-"
		#print(newStatus)
		#aseqsCol.update({"_id":aseq},{"$set":{"_s":newStatus}})

#print("Total 26-1, 27-0 sequence number is " + str(count))
print("Total d 27-0 sequence number is " + str(count))
