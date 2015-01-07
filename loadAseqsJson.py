#!/usr/bin/python3
import os
import sys
from pymongo import MongoClient
import json
import datetime



aseqs = MongoClient("localhost:27020")["seqdepot"]["aseqs"]

usage = """
Usage: $0 <database json>

  <database json> should consist of an entire json object per line

  Loads into the aseqs collection.

	$1 cross reference identifier: gi uni ENS
"""

g_File = sys.argv[1]

if '-id' in sys.argv:
	identifier = sys.argv[sys.argv.index('-id') + 1]
	cross = 1
else:
	cross = 0
	
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
	if aseqs.find_one({"_id":myjson["_id"]}):
		if count%10000==0:
#			ctime = datetime.time.second
#			timepassed = ctime-ptime
#			ptime = ctime
			print("\n" + str(count))
		if cross == 1:
			theXfield = "x." + identifier
			for idElement in myjson["x"][identifier]:
				aseqs.update({"_id":myjson["_id"]},{"$addToSet":{theXfield:idElement}}) 
	else:
		print(".")
		aseqs.insert(myjson)    
