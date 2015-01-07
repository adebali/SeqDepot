#!/usr/bin/perl

use strict;
use warnings;

use FindBin '$Bin';
use lib "$Bin";
use Common;

my $aseqs = &Common::aseqs();

my %xrefs = ();
my %ts = ();
my %other = ();

&Common::startTicker(20000);
my $cursor = $aseqs->find({});
while (my $aseq = $cursor->next()) {
    $other{aseqs}++;
    $other{aa} += $aseq->{l};

    foreach my $xtype (keys %{ $aseq->{x} }) {
        $xrefs{$xtype} += scalar(@{ $aseq->{x}->{$xtype} });
    }

    foreach my $tool (keys %{ $aseq->{t} }) {
        my $ref = $aseq->{t}->{$tool};
        my $n = (ref($ref) eq 'ARRAY') ? @$ref : 1;
        $ts{$tool} += $n;
    }
    &Common::tick();
}


print "$other{aseqs} Aseqs\n";
print "$other{aa} amino acids\n";
print "\n";

foreach my $xtype (keys %xrefs) {
    print "$xrefs{$xtype}\t$xtype\n";
}
my $total = 0;
map { $total += $xrefs{$_} } keys %xrefs;
print "$total total\n\n";

foreach my $type (keys %ts) {
    print "$ts{$type}\t$type\n";
}
$total = 0;
map { $total += $ts{$_} } keys %ts;
print "$total total\n\n";
