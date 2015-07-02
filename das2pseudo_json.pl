#!/usr/bin/perl
# Author: Luke Ulrich
# Synopsis: Convert DAS result data into pseudo JSON where each line consists of the results of
#           the sequence id, a tab character, and then the JSON encoded coils for this
#           sequence.
#
# The JSON encoded results consist of an array of arrays.

$| = 1;

use strict;
use warnings;

use FindBin '$Bin';
use lib "$Bin/lib";
use Common;

use Getopt::Long;

my $usage = <<"USAGE";
Usage: $0 [options] <STDIN | das result file>

  Available options
  -----------------
    -i, --src-file = Fasta file      : Source fasta file corresponding
                                       to these results.
    -e, --error-file = string        : file name to write any errors to.

  If the source file is provided, then this script will check that the
  first and last entries correspond to those in the fasta file.

USAGE

# Globals ---------------------------------------
my $g_Help;
my $g_InFile;
my $g_ErrFile;

GetOptions("h|help", \$g_Help,
	   "e|error-file=s", \$g_ErrFile,
	   "i|src-file=s", \$g_InFile);
die $usage if ($g_Help);

my $expectedLastId = &Common::getLastIdOrDieWithError($g_InFile, $g_ErrFile);
my $lastId;

while (<>) {
    if (/^!!!.*Skipped. >(\S+)/) {
	my $id = $1;
	&Common::printJson($id, []);
	$lastId = $id;
	next;
    }

    next if (!/^>(\S+)/ );
    my $id = $1;
    my @tms = ();

    my $non_tm = 0;

    # Read in the remainder of the prediction
    while (<>) {
        last if (/^\/\//);

        if (/^# TMH.*Non-TM protein/) {
	    $non_tm = 1;
	}
        #          $1     $2            $3           $4     $5
        elsif (!$non_tm && /^\@ *(\d+) *(\S+) *core: *(\d+) *\.\. *(\d+) *(\S+)/) {
	    my $from = int($3);
	    my $to = int($4);
	    my $peak = int($1);
	    my $peak_score = $2 + 0;  # Force json_encode to treat as number
	    my $evalue = $5 + 0;
	    push @tms, [$from, $to, $peak, $peak_score, $evalue];
        }
    }
    
    &Common::printJson($id, \@tms);
    $lastId = $id;
}

&Common::writeErrorIfInvalidLastId($expectedLastId, $lastId, $g_ErrFile);
