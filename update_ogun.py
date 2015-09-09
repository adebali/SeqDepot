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
JOBNAME = "SDU2015-04-02"
#JOBNAME = "DENEM"
#JOBNAME = "single3"
#JOBNAME = "TEST"
print(JOBNAME)

collection = "aseqs"
#collection = "testAseqs"


## os.system("./createSuperJob-v1.pl -C superjobs -t pfam27,segs,coils,agfam1,das,ecf " + JOBNAME)
#os.system("./createSuperJob-v1.pl -C superjobs -t pfam28,segs,coils,agfam1,das,ecf " + JOBNAME)
#os.system("./createSuperJob-v1.pl -C superjobs -t tigrfam14 " + JOBNAME)
#os.system("./createSuperJob-v1.pl -C superjobs -t pfam28,segs,coils,agfam1,das,ecf " + JOBNAME + " -m 0 -col " + collection)


#os.system("./tarSendAndStartSuperJob.sh superjobs/" + JOBNAME)


## If already 'tar'ed just start the job on newton
#os.system("ssh newton nohup " + SEQDEPOT + "/scripts/launchJob.pl " + JOBNAME + " > " + SEQDEPOT + "/jobs/" + JOBNAME + ".log 2> " + SEQDEPOT + "/jobs/" + JOBNAME + ".err < /dev/null &")
## ###############################################

code = "./watchNewtonUntilJobDone.pl " + JOBNAME + " && ./fetchSuperJobResults.sh superjobs " + JOBNAME + " && ./loadSuperJobData.pl superjobs/" + JOBNAME + "-results -col " + collection + " || exit 1"
#os.system(code)

os.system("./loadSuperJobData.pl superjobs/" + JOBNAME + "-results -col " + collection)

def cleanupResults():
	os.system("rm -rf superjobs/" + JOBNAME + "-results")
	os.system("rm superjobs/" + JOBNAME + "-results.tar")
	os.system("rm superjobs/" + JOBNAME + ".tar.gz")
	os.system("rm superjobs/" + JOBNAME + ".tar.gz.md5")
	os.system("rm -f superjobs/" + JOBNAME + ".log")
	os.system("rm -f superjobs" + JOBNAME + ".err")

#cleanupResults()







