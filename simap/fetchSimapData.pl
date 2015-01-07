#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

my $baseUrl = 'http://fileshare.csb.univie.ac.at/simap';

my @otherFiles = qw(sequences proteins);
my @featureFiles = qw(PRINTS Gene3D Hamap PIRSF PANTHER SMART TIGRFAM ProSitePatterns ProSiteProfiles SignalP_EUK SignalP_GRAM_NEGATIVE SignalP_GRAM_POSITIVE TMHMM SUPERFAMILY); # BlastProDom

foreach my $file (@otherFiles) {
    print "Downloading: $baseUrl/$file.gz\n";
    system(qq(curl --retry 20 -o $file.gz "$baseUrl/$file.gz"));
}

foreach my $file (@featureFiles) {
    print "Downloading: $baseUrl/features_${file}.gz\n";
    system(qq(curl --retry 20 -o features_${file}.gz "$baseUrl/features_${file}.gz"));
}
