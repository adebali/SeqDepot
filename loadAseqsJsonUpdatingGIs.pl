#!/usr/bin/perl

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib "$Bin";
use Common;


my $usage = <<USAGE;
Usage: $0 <database json>

  <database json> should consist of an entire json object per line

  Loads into the aseqs collection.

USAGE

my $g_File = shift or die $usage;
my $aseqs = &Common::aseqs();

my $fh = &Common::openFileOrGzFile($g_File);
&Common::startTicker(10000);
while (my $json = <$fh>) {
    my $son = substr($json,23);
#    print($son);
    $json = $son;
    my $aseq = from_json($json);

    if ($aseqs->find_one({_id => $aseq->{_id}})) {
        # This assumes that the $aseq->{x}->{gi} is the most recent gi
	# list and will overwrite any pre-existing ones.
	my %set = (
	    'x.gi' => $aseq->{x}->{gi}
	);
	$aseqs->update({_id => $aseq->{_id}}, {'$set' => \%set});
	&Common::tick();
	next;
    }

    print($aseq); my $hasUndoneData = index($aseq->{_s}, '-') != -1;
    if ($hasUndoneData) { $aseq->{$Common::paddingKey} =
    $Common::padding{basic}->{buffer}; }

    $aseqs->insert($aseq);
    &Common::removePadding($aseqs, $aseq->{_id});
    &Common::tick();
}
