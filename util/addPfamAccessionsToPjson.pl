#!/usr/bin/perl

use strict;
use warnings;
use JSON;
use List::MoreUtils 'firstidx';
use Data::Dumper;

use FindBin '$Bin';
use lib "$Bin/..";
use Common;

my $usage = qq(Usage: $0 <pfam map> <pjson file>\n\n);

my $g_MapFile = shift or die $usage;
my $g_File = shift or die $usage;

my $fh = &Common::openFileOrGzFile($g_MapFile);
undef $/;
my $map_json = <$fh>;
close ($fh);

my $map = from_json($map_json);

$/ = "\n";
$fh = &Common::openFileOrGzFile($g_File);
while (my $line = <$fh>) {
    chomp($line);
    my ($aseqId, $json) = split(/\t/, $line, 2);
    my $data = from_json($json);

    my @newData = ();
    foreach my $pfam (@{ $data->{'t.pfam'}}) {
        my $acc = $map->{$pfam->[0]};
        die if (!$acc);
        unshift @$pfam, $acc;
    }

    print $aseqId, "\t", to_json($data), "\n";
}