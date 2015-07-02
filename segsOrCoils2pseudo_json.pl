#!/usr/bin/perl
# Author: Luke Ulrich
# Synopsis: Convert coils fasta into pseudo JSON where each line consists of the results of
#           the sequence id, a tab character, and then the JSON encoded coils for this
#           sequence.
#
# The JSON encoded results consist of an array of arrays.

$| = 1;

use strict;
use warnings;
use IO::Handle;

use FindBin '$Bin';
use lib "$Bin/lib";
use Common;

use Getopt::Long;

my $usage = <<"USAGE";
Usage: $0 [options] <STDIN>

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

my $io = IO::Handle->new_from_fd(fileno(STDIN), 'r');
while (my $seq = &Common::readFastaSequence($io)) {
    if ($seq->[0] !~ /^(\S+)/) {
	&Common::writeErrorAndDie("Unexpected sequence identifier: $seq->[0]", $g_ErrFile);
    }
    my $id = $1;

    my @regions = ();
    my $read = 0;
    while ($seq->[1] =~ /(.*?)(x+)/g)
    {
	$read += length($1);
	my $start = $read+1;
	my $stop = $read + length($2);
	$read += length($2);
	
	push @regions, [$start, $stop];
    }

    &Common::printJson($id, \@regions);
    $lastId = $id;
}

&Common::writeErrorIfInvalidLastId($expectedLastId, $lastId, $g_ErrFile);
