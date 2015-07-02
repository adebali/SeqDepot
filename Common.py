from pymongo import MongoClient


portNumber = 27017

seqdepotDB = MongoClient("localhost:" + str(portNumber))["seqdepot"]

