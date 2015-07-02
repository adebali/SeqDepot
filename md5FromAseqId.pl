#!/usr/bin/perl

use FindBin '$Bin';
use lib "$Bin/../public";

use strict;
use warnings;

use SeqDepot;

my $usage = <<USAGE;
Usage: $0 <aseq _id> [...]

USAGE

foreach my $aseq_id (@ARGV) {
    print "$aseq_id\t", &SeqDepot::MD5HexFromAseqId($aseq_id), "\n";
}