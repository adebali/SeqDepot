#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

use JSON;
use List::MoreUtils 'firstidx';
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;

my $usage = <<USAGE;
Usage: $0 <simap feature data> [updateTool updateTool2 ...]

  updateTool refers to a new release of this tool and thus indicates
  that all results for this tool should be processed regardless.

USAGE

my $g_File = shift or die $usage;
my @g_UpdatedTools = @ARGV;

use MongoDB;
$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $collection = $db->get_collection('aseqs');

my $cmd = ($g_File =~ /\.gz$/) ? qq(zcat $g_File |) : "< $g_File";
open (IN, "$cmd") or die qq([$0] Unable to open file '$g_File': $!\n);

my @tools = qw(agfam1 coils das ecf gene3d hamap panther patscan pfam pir prints proscan segs signalp smart superfam targetp tigrfam tmhmm);
my %updatedTools = ();
foreach my $updatedTool (@g_UpdatedTools) {
    if (firstidx {$_ eq $updatedTool } @tools == -1) {
        die qq($updatedTool is not a valid tool name\n);
    }
    $updatedTools{$updatedTool} = 1;
}

my %toolStatusPos = ();
my $i = 0;
foreach my $fieldName (@tools) {
    $toolStatusPos{$fieldName} = $i;
    ++$i;
}

$i = 0;
my $updated = 0;
my $t0 = [gettimeofday];
while (my $line = <IN>) {
    chomp($line);
    my ($aseqId, $json) = split(/\t/, $line, 2);

    my $data = from_json($json);
    my $aseq = $collection->find_one({_id => $aseqId}, {_s => 1});
    die qq(Sequence not found! $aseqId\n) if (!$aseq);

    my %set = ();
    my $newStatus = $aseq->{_s};
    foreach my $fieldName (keys %$data) {
        my $pos = $toolStatusPos{$fieldName};
        my $isUpdate = $updatedTools{$fieldName};
        if ($isUpdate || substr($aseq->{_s}, $pos, 1) ne 'T') {
            $set{'t.' . $fieldName} = $data->{$fieldName};
            substr($newStatus, $pos, 1, 'T');
        }
    }

    if (scalar(keys %set) > 0) {
        $set{_s} = $newStatus;
        # $collection->update({_id => $aseqId}, {'$set' => \%set}, {safe => 1});
        $collection->update({_id => $aseqId}, {'$set' => \%set});
        ++$updated;
    }
    else {
        print Dumper($aseq);
        print Dumper($data);
        print Dumper(\%set);
        <STDIN>;
    }

    $i++;
    if ($i % 200 == 0) {
        print STDERR '.';
        if ($i % 10000 == 0) {
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i sequences, [%.3f seconds, $updated updated <> %.1f / s]\n", $elapsed, 10000 / $elapsed;
            $t0 = [gettimeofday];
            $updated = 0;
        }
    }
}
close (IN);
