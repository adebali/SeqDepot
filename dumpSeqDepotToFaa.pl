#!/usr/bin/perl

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib "$Bin";
use Common;

my $aseqs = &Common::aseqs();

&Common::startTicker(10000);
my $cursor = $aseqs->find({});
$cursor->sort({_id => 1});
while (my $aseq = $cursor->next()) {
    print '>X', $aseq->{_id}, "\n", $aseq->{s}, "\n";
    &Common::tick();
}
