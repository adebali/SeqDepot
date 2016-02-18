#!/usr/bin/python3
import os
import sys
from pymongo import MongoClient
import json
import datetime
from Common import *


usage = """
Usage: $0 <database json>

  <database json> should consist of an entire json object per line

  Loads into the aseqs collection.

	-id "TheId" cross reference identifier: gi uni ENS
"""

g_File = sys.argv[1]

if '-col' in sys.argv:
	col = sys.argv[sys.argv.index('-col') + 1]
else:
	print("-col not given")
	sys.exit()

aseqs = seqdepotDB[col]

if '-id' in sys.argv:
	identifier = sys.argv[sys.argv.index('-id') + 1]
	cross = 1
else:
	cross = 0

if '--meta' in sys.argv:
	metaFlag = 1
else:
	metaFlag = 0



#identifier = sys.argv[2]
if not g_File:
	print("No input")
	sys.exit()

count = 0;
ptime = datetime.time

#print(ptime)

filein = open(g_File,"r");
for line in filein:
#	print(line.split('\t')[1])
	myjson = json.loads(line.split('\t')[1])
#print("hey");
#    my $son = substr($json,23);
#    print($son);
#    $json = $son;
    #my $aseq = from_json(substr($json,23));
    #print("$aseq->{_id}\n");
	count += 1
	foundAseq = aseqs.find_one({"_id":myjson["_id"]})
	if foundAseq:
		#print(foundAseq["_id"])
		addedDict = {}
		#if ((not metaFlag) and ('m' in foundAseq.keys()) and (foundAseq["m"]==1)):
		#	aseqs.update({"_id":myjson["_id"]},{"$set":{"m":0}})
		if cross == 1:
			theXfield = "x." + identifier
			for idElement in myjson["x"][identifier]:
				aseqs.update({"_id":myjson["_id"]},{"$addToSet":{theXfield:idElement}}) 
	else:
		print(count)
		#print(".")
		if metaFlag:
			myjson["m"] = 1
		else:
			myjson["m"] = 0
		#print(myjson)
		aseqs.insert(myjson)    
