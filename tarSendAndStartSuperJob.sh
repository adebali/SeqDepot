#!/bin/bash

#SEQDEPOT="/lustre/projects/zhulin/lulrich/seqdepot"
SEQDEPOT="/lustre/projects/ogun_data/seqdepot"


USAGE="Usage: $0 <path to job directory>"
if [ $# -eq 0 ];
  then
    echo $USAGE
    exit 1
fi

JOB_PATH=$1
if [ ! -e $JOB_PATH ];
  then
    echo "Directory '$JOB_PATH' does not exist"
    exit 1
fi

if [ ! -d $JOB_PATH ];
  then
    echo "Specified path is not a directory"
    exit 1
fi

cd $JOB_PATH
JOB_NAME=${PWD##*/}

TARBALL=$JOB_NAME.tar.gz
MD5_FILE=$TARBALL.md5
# Check that there is at least one batch directory, combination directory, and file called tools.csv and seqs
if [ ! -d '0' ] || [ ! -d '0/a' ] || [ ! -f '0/a/tools.csv' ] || [ ! -f '0/a/seqs' ];
  then
    echo "Unexpected directory structure. Please examine that $JOB_PATH points to a valid super job"
    exit 1
fi

echo "Looks to be a valid job. Creating tarball"
cd ..

tar -czf $TARBALL $JOB_NAME


md5sum $TARBALL > $MD5_FILE


echo "Securely sending $TARBALL to Newton"
scp $TARBALL $MD5_FILE newton:$SEQDEPOT/jobs

echo "Starting job on Newton"
ssh newton "nohup $SEQDEPOT/scripts/launchJob.pl $JOB_NAME > $SEQDEPOT/jobs/$JOB_NAME.log 2> $SEQDEPOT/jobs/$JOB_NAME.err < /dev/null &"

echo "Cleaning up"
rm -rf $JOB_NAME
