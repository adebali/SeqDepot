#!/usr/bin/perl

use strict;
use warnings;

use FindBin '$Bin';
use lib "$Bin";
use Common;

my $aseqs = &Common::aseqs();

my $n = 0;

&Common::startTicker(20000);
my $cursor = $aseqs->find({});
while (my $aseq = $cursor->next()) {
    foreach my $tool (keys %{ $aseq->{t} }) {
        my $ref = $aseq->{t}->{$tool};
        if (ref($ref) eq 'ARRAY') {
            $n += @$ref;
        }
        else {
            ++$n;
        }
    }
    &Common::tick();
}

print "\n\nFeature count: $n\n";