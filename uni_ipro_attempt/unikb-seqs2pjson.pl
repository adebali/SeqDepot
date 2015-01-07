#!/usr/bin/perl

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 <UniPortKB fasta file> [ <UniPortKB fasta file> ... ]

USAGE

die $usage if (!@ARGV);

foreach my $file (@ARGV) {
    print STDERR qq(Processing $file\n);

    &Common::startTicker(2000);
    my $fh = &Common::openFileOrGzFile($file);
    while (my $seq = &Common::readFastaSequence($fh)) {
        die qq(Unable to parse out accession: $$seq[0]\n) if ($$seq[0] !~ /(?:sp|tr)\|(\w+)\|/);

        my $accession = $1;

        my $data = &Common::baseStructure($$seq[1]);
        $data->{_s} = &Common::interproStatus();

        print $accession, "\t", to_json($data), "\n";
        &Common::tick();
    }
}
