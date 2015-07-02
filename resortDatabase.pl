#!/usr/bin/perl

use strict;
use warnings;

use FindBin '$Bin';
use lib "$Bin";
use Common;

use MongoDB;
$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $aseqs = $db->get_collection('aseqs');

my $aseqs_new = $db->get_collection('aseqs_new');

&Common::startTicker(10000);
my $cursor = $aseqs->find({});
$cursor->sort({_id => 1});
while (my $aseq = $cursor->next()) {
    my $hasUndoneData = index($aseq->{_s}, '-') != -1;
    if ($hasUndoneData) {
        $aseq->{$Common::paddingKey} = $Common::padding{basic}->{buffer};
    }

    $aseqs_new->insert($aseq);
    &Common::removePadding($aseqs_new, $aseq->{_id});
    &Common::tick();
}

# Check that the counts are the same
my $old = $aseqs->count();
my $new = $aseqs_new->count();
if ($old != $new) {
    die qq(Number of records does not match! $old old vs $new new\n);
}

print STDERR qq(Removing old collection\n);
$aseqs->drop();

print STDERR qq(Renaming aseqs_new -> aseqs\n);
$aseqs_new->rename('aseqs');
