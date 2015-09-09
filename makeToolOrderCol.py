import os
import sys
from pymongo import MongoClient
from Common import *

toolsCol = seqdepotDB["tools"]
toolOrderCol =  seqdepotDB["toolOrder"]

toolOrderCol.drop()

tools = toolsCol.find()

for i in range(tools.count()):
	tool = next(tools)
	toolOrderCol.insert({"_id":tool["_id"],"o":i})
