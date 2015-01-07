#!/usr/bin/perl

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib "$Bin";
use Common;

my $usage = <<USAGE;
Usage: $0 <uni ids>

USAGE

my $g_File = shift or die $usage;
my $aseqs = &Common::aseqs();

my $fh = &Common::openFileOrGzFile($g_File);
&Common::startTicker(5000);
while (my $line = <$fh>) {
    chomp($line);
    my ($aseqId, $json) = split(/\t/, $line, 2);
    my $data = from_json($json);

    my %set = (
        'x.uni' => $data->{x}->{uni}
    );

    $aseqs->update({_id => $aseqId}, {'$set' => \%set});
    &Common::tick();
}
print STDERR "\n\n";