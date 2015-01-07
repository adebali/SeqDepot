#!/usr/bin/perl

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib "$Bin/..";
use Common;

my $usage = <<USAGE;
Usage: $0 <proteins.gz>

    Converts the tab-delimited SIMAP file of uniprot identifiers
    into a PJSON equivalent.

USAGE

my $lastMd5;
my @ids = ();

&Common::startTicker(10000);
while (my $line = <>) {
    chomp($line);
    my ($md5hex, $id, $ncbiTaxId, $dbname) = split(/\t/, $line, 4);
    if ($dbname eq 'uniprot_swissprot' || $dbname eq 'uniprot_trembl') {
        if (length($lastMd5) > 0 && $md5hex ne $lastMd5 && @ids) {
            &dumpData($lastMd5, \@ids);
            @ids = ();
        }

        push @ids, $id;
    }

    $lastMd5 = $md5hex;
}

if ($lastMd5 && @ids) {
    &dumpData($lastMd5, \@ids);
}
print STDERR "\n\n";


sub dumpData {
    my $md5hex = shift;
    my $ids = shift;

    my $aseqId = &Common::md5hex_toAseqID($md5hex);

    my $data = {
        x => {
            uni => $ids
        }
    };

    print $aseqId, "\t", to_json($data), "\n";
    &Common::tick();
}
