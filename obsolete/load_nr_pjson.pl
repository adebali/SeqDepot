#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use JSON;

use lib '/binf/lib/perl/modules';
use BinfUtil;

use FindBin '$Bin';
use lib $Bin;
use Common;

my $g_File = shift or die;

use MongoDB;
$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $aseqs = $db->get_collection('aseqs');

my $newStatus = &Common::emptyStatus();

my $g_File = shift or die;
&Common::startTicker();
my $fh = &BinfUtil::openFileOrGzFile($g_File);
my $i =0;
while ($i < 1810000) {
    <$fh>;
    ++$i;
    &Common::tick();
}

while (my $line = <$fh>) {
    chomp($line);

    my ($aseqId, $json) = split(/\t/, $line);
    my $data = from_json($json);

    my $object = $aseqs->find_one({_id => $aseqId}, {_id => 1, 'x.gi' => 1});
    if (!$object) {
        ++$Common::ticker{new};

        # Data as though we are inserting a fresh record
        $data->{t} = {};
        $data->{_s} = $newStatus;
        $data->{_id} = $aseqId;
        $data->{$Common::paddingKey} = $Common::padding{nr}->{buffer};
        # $collection->insert(\%data, {safe => 1});
        $aseqs->insert($data);
        &Common::removePadding($aseqs, $aseqId);
    }
    else {
        my $update = undef;
        my $newIds = [];
        if ($object->{x}->{gi}) {
            $newIds = &findNewIds($data->{x}->{gi}, $object->{x}->{gi});
        }

        if (@$newIds > 0 || !$object->{x}->{gi}) {
            # Always use $set instead of push/pull, etc.
            $update = {
                '$set' => {
                    'x.gi' => $data->{x}->{gi}
                }
            };
            # $aseqs->update({_id => $aseqId}, $update, {safe => 1});
            $aseqs->update({_id => $aseqId}, $update);
            ++$Common::ticker{updated};
        }
    }

    &Common::tick();
}

sub findNewIds {
    my $fileIds = shift or die;
    my $dbIds = shift or die;

    my %hash = ();
    map {$hash{$_} = 1} @$dbIds;

    my $newIds = [];
    foreach my $id (@$fileIds) {
        next if ($hash{$id});

        push @$newIds, $id;
    }

    return $newIds;
}
