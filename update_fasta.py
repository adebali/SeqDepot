import  os
import sys

### Load new sequences



#fastaFile = sys.argv[1]
#pjson = fastaFile.split('.')[0] + ".pjson"



#os.system("perl faa2pjson.pl " + fastaFile + " >" + pjson + " || exit 1")
#os.system("python3 loadAseqsJson.py " + pjson + " -id gi || exit 1")


import datetime
theDate = str(datetime.datetime.now()).split(' ')[0]
JOBNAME = "SDU" + theDate
print(JOBNAME)

col = "aseqs"
col = "testAseqs"

#os.system("./createSuperJob-v1.pl -C superjobs -t pfam27,segs,coils,agfam1,das,ecf,tigrfam14 " + JOBNAME + " ||exit 1")


###
#os.system("./createSuperJob-v1.pl -C superjobs -t segs,coils,agfam1,das,ecf " + JOBNAME + " ||exit 1")
#os.system("./createSuperJob-v1.pl -C superjobs -t pfam27 " + JOBNAME + " ||exit 1")
###

os.system("./tarSendAndStartSuperJob.sh superjobs/" + JOBNAME + " || exit 1")
code = "./watchNewtonUntilJobDone.pl " + JOBNAME + " && ./fetchSuperJobResults.sh superjobs " + JOBNAME + " && ./loadSuperJobData.pl superjobs/" + JOBNAME + "-results || exit 1"
os.system(code)

def cleanupResults():
	os.system("rm -rf superjobs/" + JOBNAME + "-results")
	os.system("rm superjobs/" + JOBNAME + "-results.tar")
	os.system("rm superjobs/" + JOBNAME + ".tar.gz")
	os.system("rm superjobs/" + JOBNAME + ".tar.gz.md5")
	os.system("rm -f superjobs/" + JOBNAME + ".log")
	os.system("rm -f superjobs" + JOBNAME + ".err")

#cleanupResults()







