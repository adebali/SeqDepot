#!/usr/bin/perl
# Author: Luke Ulrich
# Synopsis: Convert HMMER2 results into pseudo JSON where each line consists of the results of
#           the sequence id, a tab character, and then the JSON encoded domain hits for this
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
Usage: $0 [options] <STDIN | hmmpfam (hmmer2) file>

  Available options
  -----------------
    -i, --src-file = Fasta file      : Source fasta file corresponding
                                       to these results.
    -e, --error-file = string        : file name to write any errors to.
    -s, --subname                    : Parse subname from domain name

  If the source file is provided, then this script will check that the
  first and last entries correspond to those in the fasta file.

USAGE

# Globals ---------------------------------------
my $g_Help;
my $g_InFile;
my $g_ErrFile;
my $g_Subname; 

GetOptions("h|help", \$g_Help,
	   "e|error-file=s", \$g_ErrFile,
	   "i|src-file=s", \$g_InFile,
	   "s|subname", \$g_Subname);
die $usage if ($g_Help);

my $expectedLastId = &Common::getLastIdOrDieWithError($g_InFile, $g_ErrFile);
my $lastId;

# The results for each sequence is separated by //
$/ = "\n//";

while (my $entry = <>) {
    # Using the record separator // to get records, we get one last record of nothing
    # because the file ends with //\n
    # Thus it get the last entry and reads until eof and then enters this 
    # function. Since this is not a valid entry, just do simple test to skip it
    # if size is negligible
    next if (length($entry) < 100);
    
    # 1. Retrieve sequence id - This is located in the hmmer header line
    #    Query sequence: 
    if ($entry !~ m/\nQuery sequence:\s+(\S+)/) {
	my $sub = substr($entry, 0, 256);
	die "[$0] Could not parse out sequence identifier: $sub\n";
    }
    my $id = $1;
    my $subname;
    if ($g_Subname && $id =~ /(.*):(\S+)/) {
	$id = $1;
	$subname = $2;
    }
    
    # 2. Build lookup hash of coordinate data
    my %coords = ();
    #                   model name         seq-f   seq-t   ali_extents            hmm-f   hmm-t   hmm_extent
    #                   $1                 $2      $3      $4                     $5      $6      $7
    while ($entry =~ /\n(\S+)\s+\d+\/\d+\s+(\d+)\s+(\d+)\s+((?:\.|\[)(?:\.|\]))\s+(\d+)\s+(\d+)\s+((?:\.|\[)(?:\.|\]))/g)
    {
	$coords{$1}->{$2}->{$3} = {
	    ali_extent => $4,
	    hmm_start => int($5),
	    hmm_stop => int($6),
	    hmm_extent => $7
	};
    }

    my @hits = ();

    # 3. Retrieve all domains found for this sequence
    #    For more efficient purposes, chop off beginning header junk
    $entry =~ s/(?:.|\s)*Alignments of top-scoring domains://o;
    while ($entry =~ m/\n(\S+:.*)\n(?:.|\s)*?\n(?=\n\S)/og) {
	my $title = $1;

	# Extract domain score info
	$title =~ m/^(\S+): .*? from (\d+) to (\d+): score (.*?),.*?E = (\S+)/o;
	my ($domain_id, $start, $stop, $score, $evalue) = ($1, $2, $3, $4, $5);

	if (!$coords{$domain_id} ||
	    !$coords{$domain_id}->{$start} ||
	    !$coords{$domain_id}->{$start}->{$stop}) {
	    die qq([$0] Missing coordinates for $domain_id, $start - $stop\n);
	    next;
	}

	my $coord = $coords{$domain_id}->{$start}->{$stop};

	# Force to integer and real types for JSON encoding
	$start = int($start);
	$stop = int($stop);
	$score += 0;
	$evalue += 0;

	if ($g_Subname) {
	    #             0           1         2       3                   4          5         6        7             8       9
	    push @hits, [ $domain_id, $subname, $start, $stop, @{$coord}{qw(ali_extent hmm_start hmm_stop hmm_extent)}, $score, $evalue ];
	}
	else {
	    #             0           1       2                   3          4         5        6             7       8
	    push @hits, [ $domain_id, $start, $stop, @{$coord}{qw(ali_extent hmm_start hmm_stop hmm_extent)}, $score, $evalue ];
	}
    }

    # Sort all hits by their evalue asc, start asc, domain_id asc
    if ($g_Subname) {
	@hits = sort { $$a[9] <=> $$b[9] || $$a[2] <=> $$b[2] || $$a[0] cmp $$b[0] } @hits;
    }
    else {
	@hits = sort { $$a[8] <=> $$b[8] || $$a[1] <=> $$b[1] || $$a[0] cmp $$b[0] } @hits;
    }

    &Common::printJson($id, \@hits);
    $lastId = $id;
}

&Common::writeErrorIfInvalidLastId($expectedLastId, $lastId, $g_ErrFile);
