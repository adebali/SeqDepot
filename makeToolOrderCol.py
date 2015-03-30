import os
import sys
from pymongo import MongoClient

toolsCol = MongoClient("localhost",27020)["seqdepot"]["tools"]
toolOrderCol =  MongoClient("localhost",27020)["seqdepot"]["toolOrder"]

tools = toolsCol.find()

for i in range(tools.count()):
	tool = next(tools)
	toolOrderCol.insert({"_id":tool["_id"],"o":i})
