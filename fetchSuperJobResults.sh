#!/bin/bash

SEQDEPOT="/lustre/projects/ogun_data/seqdepot"

USAGE="Usage: $0 <path to job directory> <job name>"
if [ $# -ne 2 ];
  then
    echo $USAGE
    exit 1
fi

JOB_PATH=$1
mkdir $JOB_PATH 2>/dev/null
if [ ! -d $JOB_PATH ];
  then
    echo "Specified path is not a directory"
    exit 1
fi

cd $JOB_PATH
JOB_NAME=$2

TARFILE=$JOB_NAME-results.tar

echo "Everything looks good. Fetching results for $JOB_NAME"
scp newton:$SEQDEPOT/jobs/$JOB_NAME/$JOB_NAME-results.tar{,.md5} .

if [ ! -e $TARFILE ]
  then
    echo "Expected tarfile, $TARFILE, does not exist"
    exit 1
fi

if [ ! -e $TARFILE.md5 ]
  then
    echo "Expected tarfile md5, $TARFILE.md5, does not exist"
    exit 1
fi

RESULT=`md5sum -c $TARFILE.md5 2>/dev/null | cut -d ' ' -f2`
if [ $RESULT != 'OK' ];
  then
    echo "MD5 checksums do not match!"
    exit 1
fi

tar xvf $TARFILE
rm $TARFILE.md5
