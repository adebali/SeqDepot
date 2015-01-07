#!/usr/bin/perl

$| = 1;

use strict;
use warnings;
use boolean;
use Getopt::Long;
use Data::Dumper;

use FindBin '$Bin';
use lib "$Bin/..";
use Common;

my $usage = <<USAGE;
Usage: $0 <tool alias>

USAGE

my $alias = shift or die $usage;
die qq(Invalid tool: $alias\n) if (!Common::isValidTool($alias));

print "Are you sure you want to remove all $alias data!? [type 'y' to continue] ";
my $response = <STDIN>;
chomp($response);
exit if ($response ne 'y');

my $statusPos = Common::toolStatusPos($alias);

my $aseqs = &Common::aseqs();
&Common::startTicker(10000);
my $cursor = $aseqs->find();
$cursor->fields({'_s' => true});
while (my $aseq = $cursor->next()) {
    my $status = $aseq->{_s};
    my $toolStatus = substr($status, $statusPos, 1);
    next if ($toolStatus eq '-');

    substr($status, $statusPos, 1, '-');
    my $changes = {'$set' => {_s => $status}};

    if ($toolStatus eq 'T') {
        $changes->{'$unset'}->{'t.' . $alias} = 1;
    }

    $aseqs->update({_id => $aseq->{_id}}, $changes);
    &Common::tick();
}