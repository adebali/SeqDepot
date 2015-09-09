import os
import sys
from pymongo import MongoClient
from Common import *


aseqsCol = seqdepotDB["TestAseqs"]

allrecords = aseqsCol.find({},{"timeOut=false"})

for i in range(allrecords.count()):
	record = next(allrecords)
	print(i)
	aseqsCol.update({"_id":record["_id"]},{"$set":{"m":"0"}})
	r = aseqsCol.find_one({"_id":record["_id"]})
