#!/bin/bash

# Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
# Get the directory of this script
DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATE=`date +"%d%b%Y"`

mkdir -p /binf/seqdepot/flex/simap/$DATE
cd /binf/seqdepot/flex/simap/$DATE

echo "1) Downloading SIMAP data"
$DIR/fetchSimapData.pl

echo "2) Resorting sequences"
if [ ! -e sequences.gz ];
  then
    echo "Sequences.gz file not found!"
    exit 1
fi
time zcat sequences.gz | /binf/seqdepot/perl/util/md5TsvToAseqIdTsv.pl | sort -k1 --parallel=8 | gzip > sorted-sequences.gz

echo "3) JSONizing feature files"
$DIR/allSimap2Json.sh

echo "4) Merging sequences and features"
$DIR/mergeSimap.pl sorted-sequences.gz features_*pjson.gz | gzip > simap.pjson.gz

echo "5) Saving data to SeqDepot database"
# time /binf/seqdepot/perl/simap/loadSimap.pl simap.pjson.gz gene3d hamap panther patscan pir prints proscan signalp smart superfam targetp tigrfam tmhmm
#                                                          ^^^^^^ ... Refresh all SIMAP predicted data
