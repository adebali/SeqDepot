#!/usr/bin/perl

use strict;
use warnings;
use Time::HiRes qw(gettimeofday tv_interval);

use MongoDB;
$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $collection = $db->get_collection('aseqs');

my $cursor = $collection->find({}, {_id => 1, 'x.gi' => 1});
my $i = 0;
my $t0 = [gettimeofday];
while (my $doc = $cursor->next()) {
    $i++;
    if ($i % 2000 == 0) {
        print STDERR '.';
        if ($i % 100000 == 0) {
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i sequences [%.3f seconds <> %.1f / s]\n", $elapsed, 100000 / $elapsed;
            $t0 = [gettimeofday];
        }
    }
}