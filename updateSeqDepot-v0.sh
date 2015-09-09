#!/bin/bash

# Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
# Get the directory of this script
SCRIPT_DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATE=`date +"%d%b%Y"`

WORKDIR=/flex/seqdepot/$DATE
mkdir -p $WORKDIR
cd $WORKDIR

echo "Downloading and processing SIMAP"
$SCRIPT_DIR/simap/fetchSimapData.pl

echo "Processing PDB database"
$SCRIPT_DIR/pdb2pjson.pl | sort -k1 | gzip > pdb.pjson.gz

echo "Processing NR database"
$SCRIPT_DIR/nr2pjson.pl /binf/seqdepot/db/nr.12jan2013.gz | sort -k1 --parallel=4 | gzip > nr.pjson.gz

echo "Processing UniProtKB ids"
# Sort by the md5hex and then after processing sort again by the aseq_id
zcat proteins.gz | sort -k1 --parallel=4 | $SCRIPT_DIR/simap/uniIds2pjson.pl | sort -k1 --parallel=4 | gzip > uni-ids.pjson.gz

# echo "Processing UniProt database"
# $SCRIPT_DIR/uni2pjson.pl /binf/seqdepot/db/uniref100.xml.gz | sort -k1 --parallel=4 | gzip > uni.pjson.gz

echo "  Converting SIMAP sequences to PJSON"
$SCRIPT_DIR/simapSeqs2pjson.pl sequences.gz | sort -k1 --parallel=4 | gzip > simap-seqs.pjson.gz

echo "  JSONizing feature files"
CONVERT=$SCRIPT_DIR/simap/simap2json.pl
for i in Gene3D HAMAP HMMPanther PatternScan HMMPIR FPrintScan ProfileScan SignalP HMMSmart superfamily TargetP HMMTigr TMHMM;
do
    echo "    Converting $i"
    time zcat features_$i.gz | sort -k1 | $CONVERT -type $i | sort -k1 | gzip > features_$i.pjson.gz
done

echo "Merging all JSON results"
$SCRIPT_DIR/mergePjson.pl simap-seqs.pjson.gz uni-ids.pjson.gz nr.pjson.gz pdb.pjson.gz features_*.pjson.gz | gzip > update.json.gz

# -------------------------------------------------
echo "Merging with active database"
# $SCRIPT_DIR/loadUpdateJSON.pl update.json.gz

echo "Resorting database"
# $SCRIPT_DIR/resortDatabase.pl
