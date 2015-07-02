#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

# use Data::Dumper;
use FindBin '$Bin';
use lib "$Bin";
use Common;

my $usage = <<USAGE;
Usage: $0 <number of slots>

  Increases the status string by appending <number of slots> dashes
  to each aseq status string in preparation for accommondating new
  tools.

  For example, when integrating Pfam27, an additional slot had to
  be added. To provide for future expansion, more can be added than
  there are tools currently available.

USAGE

my $nNewSlots = shift or die $usage;
die qq(New slots must be positive\n) if ($nNewSlots !~ /^[1-9]\d*$/);

my $tools_col = &Common::collection('tools');
my $nTools = $tools_col->count();

my $finalLen = $nTools + $nNewSlots;

$MongoDB::Cursor::timeout = -1;
my $aseqs = &Common::collection('aseqs');

&Common::startTicker();
my $cursor = $aseqs->find();
$cursor->immortal(1);
$cursor->fields({_s => 1});
while (my $aseq = $cursor->next()) {
    my $l = length($aseq->{_s});
    my $n = $finalLen - $l;
    next if ($n <= 0);

    my $s = $aseq->{_s} . '-'x$n;
    $aseqs->update({_id => $aseq->{_id}}, {'$set' => {_s => $s}});
    &Common::tick();
}
