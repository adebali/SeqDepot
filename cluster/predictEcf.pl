#!/usr/bin/perl
#
# Program: predict_ecf.pl
# Author: Luke Ulrich
# Date: 24 September 2010
# Description: Prediction of ECF sigma factors/domains is not as straightforward as simply searching
#              against an HMM database. Rather, it requires a prefilter step, actually searching all
#              sequences against an ECF HMM database, and finally more special rules to filter out
#              false positives.

$| = 1;

use strict;
use warnings;
use File::Temp 'tempfile';
use Getopt::Long;
use JSON;

use FindBin '$Bin';
use lib "$Bin/lib";
use Common;

my $usage = <<"USAGE";
Usage: $0 [options] <fasta source file>

  Available options
  -----------------
    -e, --error-file = string        : file name to write any errors to.

USAGE

my $g_Help;
my $g_ErrFile;

GetOptions("h|help", \$g_Help,
	   "e|error-file=s", \$g_ErrFile);
die $usage if ($g_Help);

my $g_File = shift or die $usage;

my $g_HmmerPath = "/home/lulrich/tools/hmmer-2.4i/src";
my $g_HmmPfam = $g_HmmerPath . "/hmmpfam";
my $g_HmmSearch = $g_HmmerPath . "/hmmsearch";
my $g_EcfPath = "/home/lulrich/lustre/ecf";
my $g_EcfGeneral = $g_EcfPath . '/ecf_general.hmm';
my $g_EcfHmmDb = $g_EcfPath . "/ecfs.bin";
my $h22pseudo_json = "$Bin/h22pseudo_json.pl";

my @g_Fields = qw(name start stop ali_extent hmm_start hmm_stop hmm_extent score evalue);

# -----------------------------------
my @ids = @{ &Common::readFastaIds($g_File, $g_ErrFile) };

# -----------------------------------
# Prefilter step
my $count = 0;
my %passedPreFilter = ();
open (IN, "$g_HmmSearch --cpu 0 --cut_ga $g_EcfGeneral $g_File |") or die(qq(Unable to execute hmmsearch: $!\n));
while (<IN>) {
    next if (!/^(\S+)\s+\d+\/\d+\s+\d+\s+\d+ ..\s+\d+\s+\d+ ..\s+\S+ \S/);

    $passedPreFilter{$1}++;
    ++$count;
}
close (IN);

# -----------------------------------
# Save all results by id
my %results = ();

# Create fasta file of those in prefilter; also remember the order of ids for when we
# output the JSON result
if ($count) {
    my ($fh, $tmp_fasta_file) = tempfile(UNLINK => 1);
    my $keep_flag = 0;
    if (!open (IN, "< $g_File")) {
	&Common::writeErrorAndDie(qq([$0] Unable to open file '$g_File': $!\n), $g_ErrFile);
    }
    while (<IN>) {
    	if (/^>(\S+)/) {
    	    $keep_flag = $passedPreFilter{$1};
    	}
    	
    	print $fh $_ if ($keep_flag);
    }
    close (IN);
    close($fh);
    
    my $cmd = qq($g_HmmPfam --cpu 0 --cut_ga $g_EcfHmmDb $tmp_fasta_file | $h22pseudo_json);
    if (!open (IN, "$cmd |")) {
	&Common::writeErrorAndDie(qq(Unable to execute '$cmd': $!), $g_ErrFile);
    }
    while (<IN>) {
	$_ =~ /^(\S+)\s+(.*)/;
	my $id = $1;
	my $json = $2;

	my $hasSigma70_r3 = 0;
	my $isSpecific = 0;
	my $domains = &h2_arrayOfHashes(from_json($json));
	foreach my $d (@$domains) {
	    if ($d->{name} eq 'Sigma70_r3') {
		$hasSigma70_r3 = 1;
		next;
	    }

	    $isSpecific = 1 if (!$isSpecific && $d->{name} ne 'ECF_999' && $d->{name} =~ /^ECF_/);
	}
	
	if ($isSpecific || (!$hasSigma70_r3 && @$domains > 0)) {
	    my @result = ();
	    my @sortedDomains = sort { $b->{score} <=> $a->{score} } @$domains;
	    foreach my $d (@sortedDomains) {
		push @result, [ @{$d}{@g_Fields} ];
	    }
	    $results{$id} = \@result;
	}
    }
    close (IN);
}

# Finally, print out the results
foreach my $id (@ids) {
    my $result = (exists $results{$id}) ? $results{$id} : [];
    &Common::printJson($id, $result);
}

sub h2_arrayOfHashes {
    my $domains = shift or die;

    my @result = ();
    foreach my $d (@$domains) {
	my %hit = ();
	@hit{@g_Fields} = @$d;
	push @result, \%hit;
    }
    return \@result;
}
