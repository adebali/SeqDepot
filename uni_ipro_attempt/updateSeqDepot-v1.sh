#!/bin/bash

# Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
# Get the directory of this script
SCRIPT_DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATE=`date +"%d%b%y"`

WORKDIR=/flex/seqdepot/$DATE
mkdir -p $WORKDIR
cd $WORKDIR

echo "Downloading UniParc sequences"
wget ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/uniparc/uniparc_active.fasta.gz

echo "Downloading UniParc ID Mapping"
wget ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/idmapping_selected.tab.gz








echo "Processing NR database"
$SCRIPT_DIR/nr2pjson.pl /binf/seqdepot/db/nr.12jan2013.gz | sort -k1 --parallel=4 | gzip > nr.pjson.gz

echo "Processing UniProt database"
$SCRIPT_DIR/uni2pjson.pl /binf/seqdepot/db/uniref100.xml.gz | sort -k1 --parallel=4 | gzip > uni.pjson.gz

echo "Processing PDB database"
$SCRIPT_DIR/pdb2pjson.pl | sort -k1 | gzip > pdb.pjson.gz

echo "Downloading and processing SIMAP"
$SCRIPT_DIR/simap/fetchSimapData.pl

echo "  Converting SIMAP sequences to PJSON"
$SCRIPT_DIR/simapSeqs2pjson.pl sequences.gz | sort -k1 --parallel=4 | gzip > simap-seqs.pjson.gz

echo "  JSONizing feature files"
CONVERT=$SCRIPT_DIR/simap/simap2json.pl
for i in Coil Gene3D HAMAP HMMPanther PatternScan HMMPfam HMMPIR FPrintScan ProfileScan Seg SignalP HMMSmart superfamily TargetP HMMTigr TMHMM;
do
    echo "    Converting $i"
    time zcat features_$i.gz | sort -k1 | $CONVERT -type $i | sort -k1 | gzip > features_$i.pjson.gz
done

echo "Merging all JSON results"
$SCRIPT_DIR/mergePjson.pl simap-seqs.pjson.gz nr.pjson.gz uni.pjson.gz pdb.pjson.gz features_*.pjson.gz | gzip > update.json.gz

# -------------------------------------------------
echo "Merging with active database"
$SCRIPT_DIR/loadUpdateJSON.pl update.json.gz

echo "Resorting database"
$SCRIPT_DIR/resortDatabase.pl