#!/usr/bin/perl

use strict;
use warnings;

use JSON;

use FindBin '$Bin';
use lib "$Bin/..";
use Common;


my $usage = qq(Usage: $0 [Pfam-A.hmm.dat.gz]\n\n);

my $g_File = shift;
if (!$g_File) {
    system(qq(wget ftp://ftp.sanger.ac.uk/pub/databases/Pfam/current_release/Pfam-A.hmm.dat.gz));
    $g_File = 'Pfam-A.hmm.dat.gz';
}

my %map = ();

my $fh = &Common::openFileOrGzFile($g_File);
$/ = "//";
while (my $entry = <$fh>) {
    last if ($entry !~ /GF ID\s+(\S+)/);
    my $name = $1;
    last if ($entry !~ /GF AC\s+(\S+)/);
    my $acc = $1;

    $acc =~ s/\.\d+$//;

    $map{$name} = $acc;
}

print to_json(\%map);
