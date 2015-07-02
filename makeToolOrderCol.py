import os
import sys
from pymongo import MongoClient

toolsCol = seqdepotDB["tools"]
toolOrderCol =  seqdepotDB["toolOrder"]

tools = toolsCol.find()

for i in range(tools.count()):
	tool = next(tools)
	toolOrderCol.insert({"_id":tool["_id"],"o":i})
