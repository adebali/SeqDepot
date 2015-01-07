#!/usr/bin/perl

use strict;
use warnings;
# use Data::Dumper;
use JSON;

use FindBin '$Bin';
use lib "$Bin";
use Common;

my @tools = @Common::tools;
my %toolStatusPos = ();
my $i = 0;
foreach my $fieldName (@tools) {
    $toolStatusPos{$fieldName} = $i;
    ++$i;
}
my $nTools = @tools;

my $labeledWithoutData = 0;
my $dataWithoutLabel = 0;

$Common::ticker{callback} = sub {
    print STDERR "\tLabeled without data: $labeledWithoutData\n";
    print STDERR "\tData without label: $dataWithoutLabel\n";

    $labeledWithoutData = 0;
    $dataWithoutLabel = 0;
};

my $aseqs = &Common::aseqs();
&Common::startTicker(10000);
my $cursor = $aseqs->find();
while (my $aseq = $cursor->next()) {
    my @statuses = split('', $aseq->{_s});

    my $type1 = 0;
    my $type2 = 0;

    for (my $i=0; $i< $nTools; ++$i) {
        my $toolStatus = $statuses[$i];
        my $toolName = $tools[$i];
        if ($toolStatus eq 'T') {
            if (!$aseq->{t}->{$toolName}) {
                $type1 = 1;
                last;
            }
        }
        elsif ($aseq->{t}->{$toolName}) {
            $type2 = 1;
            last;
        }
    }

    $labeledWithoutData += $type1;
    $dataWithoutLabel += $type2;

    # if ($type1 || $type2) {
    #     print Dumper($aseq);
    #     <STDIN>;
    # }

    &Common::tick();
}