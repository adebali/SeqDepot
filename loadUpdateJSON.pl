#!/usr/bin/perl

use strict;
use warnings;
use JSON;
use List::MoreUtils 'firstidx';
use Data::Dumper;

use FindBin '$Bin';
use lib "$Bin";
use Common;

my $usage = <<USAGE;
Usage: $0 <update json> [updateTool updateTool2 ...]

  Each line of <update json> must contain all data encoded in JSON
  necessary to update this sequence.

  updateTool refers to a new release of this tool and thus indicates
  that all results for this tool should be processed regardless.

USAGE

my $g_File = shift or die $usage;
my @g_UpdatedTools = @ARGV;

my @tools = @Common::tools;
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

my $aseqs = &Common::aseqs();

my $fh = &Common::openFileOrGzFile($g_File);
&Common::startTicker();
while (my $json = <$fh>) {
    chomp($json);
    my $data = from_json($json);
    my $aseqId = $data->{_id};
    my $aseq = $aseqs->find_one({_id => $aseqId}, {_s => 1, x => 1});
    if ($aseq) {
        $Common::ticker{updated} += &updateAseq($aseq, $data);
    }
    else {
        &createNewAseq($data);
        &Common::removePadding($aseqs, $aseqId);
        ++$Common::ticker{new};
    }

    &Common::tick();
}
close ($fh);

# --------------------------------------------------------------------------------------------------------------------
sub updateAseq {
    my $aseq = shift or die;
    my $data = shift or die;

    my %set = ();
    my $newStatus = $aseq->{_s};

    my $t = $data->{t};
    if ($t) {
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
        }
    }

    my $x = $data->{x};
    if ($x) {
        # Update the cross-references if there are new ones
        foreach my $xrefType (keys %$x) {
            my $xrefs = $x->{$xrefType};
            if (!$aseq->{x}->{$xrefType} ||
                &idsDiffer($xrefs, $aseq->{x}->{$xrefType})) {
                $set{'x.' . $xrefType} = $xrefs;
            }
        }
    }

    if (scalar(keys %set) > 0) {
        # $aseqs->update({_id => $aseq->{_id}}, {'$set' => \%set}, {safe => 1});
        $aseqs->update({_id => $aseq->{_id}}, {'$set' => \%set});
        return 1;
    }

    return 0;
}

sub createNewAseq {
    my $data = shift or die;

    die if (!exists $data->{s} || !exists $data->{l});
    $data->{x} = {} if (!exists $data->{x});
    $data->{t} = {} if (!exists $data->{t});
    $data->{$Common::paddingKey} = $Common::padding{basic}->{buffer};

    my $t = $data->{t};

    my $status = $data->{_s};
    foreach my $fieldName (keys %$t) {
        my $pos = $toolStatusPos{$fieldName};
        my $isUpdate = $updatedTools{$fieldName};
        if ($isUpdate || substr($status, $pos, 1) ne 'T') {
            substr($status, $pos, 1, 'T');
        }
    }

    $data->{_s} = $status;
    # $aseqs->insert(\%data, {safe => 1});
    $aseqs->insert($data);
}

sub idsDiffer {
    my $xrefs = shift or die;
    my $dbXrefs = shift or die;

    return 1 if (@$xrefs != @$dbXrefs);

    my @alpha = sort @$xrefs;
    my @beta = sort @$dbXrefs;
    for (my $i = 0, my $z = @alpha; $i<$z; ++$i) {
        return 1 if ($alpha[$i] ne $beta[$i]);
    }

    return 0;
}