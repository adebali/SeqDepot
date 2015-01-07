#!/bin/bash

CONVERT=/binf/seqdepot/perl/simap/simap2json.pl

for i in Gene3D Hamap PANTHER ProSitePatterns PIRSF PRINTS ProSiteProfiles SignalP_EUK SignalP_GRAM_NEGATIVE SignalP_GRAM_POSITIVE SMART SUPERFAMILY TIGRFAM TMHMM;
do
    echo "Converting $i"
    time zcat features_$i.gz | sort -k1 | $CONVERT -type $i | sort -k1 | gzip > features_$i.pjson.gz
done
