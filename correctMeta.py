#!/usr/bin/python3
import os
import sys
from pymongo import MongoClient
import json
import datetime
from Common import *



aseqs = seqdepotDB["aseqs"]

#print(aseqs.count({"m":{"$exists":"false"}}))
print(aseqs.count({"m":0}))
sys.exit()
noMflagRecords = aseqs.find({"m":{"$exists":"false"},"m":{"$ne":1},"m":{"$ne":0}})


count = 0

for record in noMflagRecords:
	count += 1
	print(record["_id"] + " " + str(count))
	aseqs.update({"_id":record["_id"]},{"m":0})
