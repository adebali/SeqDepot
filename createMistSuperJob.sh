#!/bin/bash

# Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
# Get the directory of this script
DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR

JOB_NAME=mist

# What if there are no sequences to be dumped?
./createSuperJob-v1.pl -C superjobs -t pfam26,pfam27,segs,coils,agfam1,das,ecf $JOB_NAME &&
./tarSendAndStartSuperJob.sh superjobs/$JOB_NAME &&
./watchNewtonUntilJobDone.pl $JOB_NAME &&
./fetchSuperJobResults.sh superjobs mist &&
./loadSuperJobData.pl superjobs/$JOB_NAME-results || exit 1

cd superjobs
rm -rf $JOB_NAME-results
rm $JOB_NAME-results.tar
rm $JOB_NAME.tar.gz
rm $JOB_NAME.tar.gz.md5
rm -f mist.log
rm -f mist.err
