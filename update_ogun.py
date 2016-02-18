import  os
import sys

### Load new sequences

## Update NR databae

#os.system("perl nr2pjson.pl")
#os.system("python3 nr_diff.py")
#os.system("python3 loadAseqsJson.py nr_diff.pjson -id gi")

## Update Uniprot



## Update Ensembl


import datetime
theDate = str(datetime.datetime.now()).split(' ')[0]
JOBNAME = "SDU" + theDate
SEQDEPOT = "/lustre/home/genomics/ogun_data/seqdepot"
#JOBNAME = "DENEMMmeta0"
JOBNAME = "SDU2015-11-30"
print(JOBNAME)

collection = "aseqs"
#collection = "testAseqs"

#######
## 1 ##
#######
#os.system("./createSuperJob-v1.pl -C superjobs -t segs,coils,agfam1,das,ecf,tigrfam15,pfam28,pfam29 " + JOBNAME + " -m 1 -col " + collection)
#os.system("./createSuperJob-v1.pl -C superjobs -t segs,coils,agfam1,das,ecf,tigrfam15,pfam29 " + JOBNAME + " -m 1 -col " + collection)
#os.system("./createSuperJob-v1.pl -C superjobs -t pfam28,segs,coils,agfam1,das,ecf,tigrfam15 " + JOBNAME + " -m 0 -col " + collection)

## ##############################################

#######
## 2 ##
#######
#os.system("./tarSendAndStartSuperJob.sh superjobs/" + JOBNAME)


## If already 'tar'ed and sent just start the job on newton
#os.system("ssh newton nohup " + SEQDEPOT + "/scripts/launchJob.pl " + JOBNAME)
#os.system("ssh newton nohup " + SEQDEPOT + "/scripts/launchJob.pl " + JOBNAME + " > " + SEQDEPOT + "/jobs/" + JOBNAME + ".log 2> " + SEQDEPOT + "/jobs/" + JOBNAME + ".err")
#os.system("ssh newton nohup " + SEQDEPOT + "/scripts/launchJob.pl " + JOBNAME + " > " + SEQDEPOT + "/jobs/" + JOBNAME + ".log 2> " + SEQDEPOT + "/jobs/" + JOBNAME + ".err &")
#os.system("ssh newton nohup " + SEQDEPOT + "/scripts/launchJob.pl " + JOBNAME + " &")
##

## ###############################################

#######
## 3 ##
#######
code = "./watchNewtonUntilJobDone.pl " + JOBNAME + " && ./fetchSuperJobResults.sh superjobs " + JOBNAME + " && ./loadSuperJobData.pl superjobs/" + JOBNAME + "-results -col " + collection + " || exit 1"
#code = "./loadSuperJobData.pl superjobs/" + JOBNAME + "-results -col " + collection + " || exit 1"
os.system(code)
#os.system("./loadSuperJobData.pl superjobs/" + JOBNAME + "-results -col " + collection)

## ###############################################


def cleanupResults():
	os.system("rm -rf superjobs/" + JOBNAME + "-results")
	os.system("rm superjobs/" + JOBNAME + "-results.tar")
	os.system("rm superjobs/" + JOBNAME + ".tar.gz")
	os.system("rm superjobs/" + JOBNAME + ".tar.gz.md5")
	os.system("rm -f superjobs/" + JOBNAME + ".log")
	os.system("rm -f superjobs" + JOBNAME + ".err")

#######
## 4 ##
#######
#cleanupResults()

## ################################################





