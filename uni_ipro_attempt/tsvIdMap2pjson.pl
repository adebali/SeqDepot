#!/usr/bin/perl
#
# Receives a tab delimited input from stdin or file and merges the adjacent
# records into a single non-overlapping representation. The expected columns
# are:
#
# upi \t gis \t pdb \t uni
#
# gis, pdb, and uni are expected to be delimited with spaces.

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib $Bin;
use Common;

my %data = ();

my $lastUpi;
&Common::startTicker(5000);
while (<>) {
    chomp;
    my ($upi, $gis, $pdb, $uni) = split(/\t/);

    &processAndPrintData() if ($lastUpi && $upi ne $lastUpi);

    push @{$data{gis}}, split(' ', $gis);
    push @{$data{pdbs}}, split(' ', $pdb);
    push @{$data{unis}}, split(' ', $uni);

    $lastUpi = $upi;
    &Common::tick();
}

&processAndPrintData() if ($lastUpi);

# --------------------------------------------------------------------------------------------------------------------
sub processAndPrintData {
    my %xs = (
        uni => [sort &Common::unique($data{unis}) ]
    );
    $xs{gi} = [sort {$a <=> $b} &Common::unique($data{gis}) ] if (@{$data{gis}});
    $xs{pdb} = [sort &Common::unique($data{pdbs}) ] if (@{$data{pdbs}});

    print $lastUpi, "\t", to_json({x => \%xs}), "\n";

    %data = ();
}