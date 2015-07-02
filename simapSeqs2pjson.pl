#!/usr/bin/perl

use strict;
use warnings;

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 <simap sequence file>

USAGE

my $g_File = shift or die $usage;

my $fh = &Common::openFileOrGzFile($g_File);

my $baseStatusForNewSimapSeqs = 'd'x(scalar(@Common::tools));
substr($baseStatusForNewSimapSeqs, 0, 1, '-');
substr($baseStatusForNewSimapSeqs, 2, 2, '--');

&Common::startTicker();
while (my $line = <$fh>) {
    chomp($line);
    my ($dummy, $prot_seq) = split(/\t/, $line, 2);
    $prot_seq = uc($prot_seq);  # The SIMAP sequences have low-complexity regions in lower case.
                                # This will screw up the MD5
    $prot_seq =~ s/\s+//g;

    # No minimum length for simap sequences
    my $data = &Common::baseStructure($prot_seq);
    $data->{_s} = $baseStatusForNewSimapSeqs;
    &Common::printPJSON($data);
    &Common::tick();
}
