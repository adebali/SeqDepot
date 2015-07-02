#!/usr/bin/perl

use strict;
use warnings;

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 <fasta file>

  Note: <fasta file> may be gzipped (but should have a .gz
        extension.

USAGE

my $file = shift or die $usage;
my $fh = &Common::openFileOrGzFile($file);

&Common::startTicker();
while (my $seq = &Common::readFastaSequence($fh)) {
    my $prot_seq = $$seq[1];
    if (index($prot_seq, '@') != -1) {
        print STDERR qq([Warn] sequence contains invalid characters $$seq[0] - $prot_seq\n);
        next;
    }

    my $l = length($prot_seq);
    next if ($l < $Common::g_MinLen);

    my $data = &Common::baseStructure($prot_seq);
    # >gi|123234|...
    # ABCACBC
    #
    # parseGis(...) --> [123234, ...]
    $data->{x}->{gi} = &parseGis($$seq[0]);
    &Common::printPJSON($data);
    &Common::tick();
}

sub parseGis {
    my $header = shift;

    my @gis = ();
    chomp($header);
    my @deflines = split(/\cA/, $header);
    foreach my $defline (@deflines) {
        if ($defline =~ /gi\|(\d+)/) {
            push @gis, int($1);
        }
    }

    return \@gis;
}
