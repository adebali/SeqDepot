#!/usr/bin/perl
# Author: Luke Ulrich
# Synopsis: Convert HMMER3 results into pseudo JSON where each line consists of the results of
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
Usage: $0 [options] <HMMER3 dom table file> <fasta source file>

  Available options
  -----------------
    -e, --error-file = string        : file name to write any errors to.

USAGE

# Globals ---------------------------------------
my $g_Help;
my $g_ErrFile;

GetOptions("h|help", \$g_Help,
	   "e|error-file=s", \$g_ErrFile);
die $usage if ($g_Help);

my $g_DomTblFile = shift or die $usage;
my $g_FastaSourceFile = shift or die $usage;

print STDERR qq(Reading ids...);
my @ids = @{ &Common::readFastaIds($g_FastaSourceFile, $g_ErrFile) };
print STDERR qq(done\n);
my %results = ();

if (!open(IN, "< $g_DomTblFile")) {
    &Common::writeErrorAndDie("Unable to open file, $g_DomTblFile: $!\n", $g_ErrFile);
}
while (<IN>) {
    next if (/^#/);
    chomp;

    my @values = split(/\s+/);
    if (@values != 23) {
	&Common::writeErrorAndDie("Line is missing expected 23 values: $_\n", $g_ErrFile);
    }

    my $id = $values[0];
    my $seq_len = int($values[2]);
    my $domain_name = $values[3];
    my $profile_len = int($values[5]);
    my $c_evalue = $values[11] + 0;
    my $i_evalue = $values[12] + 0;
    my $dom_score = $values[13] + 0;
    my $dom_bias = $values[14] + 0;
    my $hmm_start = int($values[15]);
    my $hmm_stop = int($values[16]);
    my $ali_start = int($values[17]);
    my $ali_stop = int($values[18]);
    my $env_start = int($values[19]);
    my $env_stop = int($values[20]);
    my $acc = $values[21] + 0;

    my $ali_extent = ($ali_start > 1) ? '.' : '[';
    $ali_extent .= ($ali_stop < $seq_len) ? '.' : ']';

    my $hmm_extent = ($hmm_start > 1) ? '.' : '[';
    $hmm_extent .= ($hmm_stop < $profile_len) ? '.' : ']';

    my $env_extent = ($env_start > 1) ? '.' : '[';
    $env_extent .= ($env_stop < $seq_len) ? '.' : ']';

    my @data = ($domain_name, $ali_start, $ali_stop, $ali_extent,
                $dom_bias, $hmm_start, $hmm_stop, $hmm_extent,
                $env_start, $env_stop, $env_extent,
                $dom_score, $c_evalue, $i_evalue, $acc);

    push @{$results{$id}}, \@data;
}

foreach my $id (@ids) {
    my @hits = ();
    if ($results{$id}) {
	@hits = sort { $a->[13] <=> $b->[13] || $a->[1] <=> $b->[1] || $a->[0] cmp $b->[0] } @{ $results{$id} };
    }
    
    &Common::printJson($id, \@hits);
}

