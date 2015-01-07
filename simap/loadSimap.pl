#!/usr/bin/perl

use strict;
use warnings;
use JSON;
use List::MoreUtils 'firstidx';
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;

use FindBin '$Bin';
use lib "$Bin/..";
use Common;

my $usage = <<USAGE;
Usage: $0 <simap sequences + features> [updateTool updateTool2 ...]

  <simap sequences + features> should contain the merged data of all
  sequences in simap their associated features in pseudo JSON format
  (each line consists of aseq_id [TAB] json data).

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

my @tools = @Common::tools;

my %toolStatusPos = ();
my $i = 0;
foreach my $fieldName (@tools) {
    $toolStatusPos{$fieldName} = $i;
    ++$i;
}

my %updatedTools = ();
foreach my $updatedTool (@g_UpdatedTools) {
    if (!exists $toolStatusPos{$updatedTool}) {
        die qq($updatedTool is not a valid tool name\n);
    }
    $updatedTools{$updatedTool} = 1;
}

# Agfam1, coils, das, ecf, pfam26, and segs are not complete (unless already in the database)
my $baseStatusForNewSeqs = 'd'x(scalar(@tools));
foreach my $tool (qw(agfam1 coils das ecf pfam26 segs)) {
    my $pos = $toolStatusPos{$tool};
    substr($baseStatusForNewSeqs, $pos, 1, '-');
}

$i = 0;
my $new = 0;
my $updated = 0;
my $t0 = [gettimeofday];
while (my $line = <IN>) {
    chomp($line);
    my ($aseqId, $json) = split(/\t/, $line, 2);

    my $data = from_json($json);
    my $aseq = $collection->find_one({_id => $aseqId}, {_s => 1});
    if ($aseq) {
        $updated += &updateAseq($aseq, $data);
    }
    else {
        &createNewAseq($aseqId, $data);
        &removePadding($aseqId);
        ++$new;
    }

    $i++;
    if ($i % 200 == 0) {
        print STDERR '.';
        if ($i % 10000 == 0) {
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i sequences [%.3f seconds, $new new, $updated updated <> %.1f / s]\n", $elapsed, 10000 / $elapsed;
            $t0 = [gettimeofday];
            $updated = 0;
            $new = 0;
        }
    }
}
close (IN);

# --------------------------------------------------------------------------------------------------------------------
sub updateAseq {
    my $aseq = shift or die;
    my $data = shift or die;

    my %set = ();
    my $newStatus = $aseq->{_s};
    my $t = $data->{t};

    foreach my $fieldName (keys %$t) {
        my $pos = $toolStatusPos{$fieldName};
        my $isUpdate = $updatedTools{$fieldName};
        if ($isUpdate || substr($aseq->{_s}, $pos, 1) ne 'T') {
            $set{'t.' . $fieldName} = $t->{$fieldName};
            substr($newStatus, $pos, 1, 'T');
        }
    }

    if (scalar(keys %set) > 0) {
        $set{_s} = $newStatus;
        # $collection->update({_id => $aseq->{_id}}, {'$set' => \%set}, {safe => 1});
        $collection->update({_id => $aseq->{_id}}, {'$set' => \%set});
        return 1;
    }

    return 0;
}

sub createNewAseq {
    my $aseqId = shift;
    my $data = shift or die;

    $data->{_id} = $aseqId;
    $data->{x} = {};
    $data->{$Common::paddingKey} = $Common::padding{simap}->{buffer};

    my $status = $baseStatusForNewSeqs;
    my $t = $data->{t};

    foreach my $fieldName (keys %$t) {
        my $pos = $toolStatusPos{$fieldName};
        my $isUpdate = $updatedTools{$fieldName};
        if ($isUpdate || substr($status, $pos, 1) ne 'T') {
            substr($status, $pos, 1, 'T');
        }
    }

    $data->{_s} = $status;
    # $collection->insert(\%data, {safe => 1});
    $collection->insert($data);
}

sub removePadding {
    my $aseqId = shift;

    $collection->update({_id => $aseqId}, {'$unset' => {$Common::paddingKey => 1}});
}