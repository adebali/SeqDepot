#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use Hash::Merge 'merge';
use JSON;

use FindBin '$Bin';
use lib $Bin;
use Common;


my $usage = <<USAGE;
Usage: $0 [options] <data files>

   Options
   -------
     -t, --tabular  : Output two columns, the first with the aseq_id
                      and the second with the json

   All files must be tab-delimited with the first column
   containing the aseq_id and have a gzip extension. Moreover,
   all files should be in pseudo json.

USAGE

die $usage if (@ARGV == 0);

my $g_Tabular;
GetOptions("t|tabular", \$g_Tabular);

my %sources = ();
my %data = ();      # {fieldName => {aseq_id, decoded json}}

my $i = 0;
foreach my $file (@ARGV) {
    my $fh = &Common::openFileOrGzFile($file);
    $sources{$i} = $fh;
    ++$i;
}

$i = 0;
&Common::startTicker();
&initializeData();
while (my $aseq_id = &nextLowestAseqId()) {
    my $rowData = &dataForAseqId($aseq_id);
    print $rowData->{_id}, "\t" if ($g_Tabular);
    print to_json($rowData), "\n";
    &Common::tick();
}

if (scalar(keys %data) > 0) {
    print STDERR qq(Unmerged data:\n);
    print STDERR Dumper(\%data);
}

if (scalar(keys %sources) > 0) {
    print STDERR qq(Unfinished sources\n);
    print STDERR Dumper(\%sources);
}

# --------------------------------------------------------------------------------------------------------------------
sub nextLowestAseqId {
    my $lowest = undef;
    foreach my $num (keys %data) {
        my $aseqId = $data{$num}->{aseq_id};
        $lowest = $aseqId if (!defined($lowest) || $aseqId lt $lowest);
    }

    return $lowest;
}

sub dataForAseqId {
    my $aseqId = shift;
    my $merged = {};

    foreach my $num (keys %data) {
        my $ref = $data{$num};
        my $otherAseqId = $ref->{aseq_id};
        if ($otherAseqId eq $aseqId) {
            if ($merged->{_s} && $ref->{data}->{_s}) {
                $merged->{_s} = &Common::mergeStatuses($merged->{_s}, $ref->{data}->{_s});
            }
            $merged = merge($merged, $ref->{data});
            &readNextRecord($num);
        }
    }

    return $merged;
}

sub readNextRecord {
    my $num = shift;

    my $fh = $sources{$num};
    my $line = $fh->getline();
    if ($line) {
        chomp($line);
        my ($aseqId, $json) = split(/\t/, $line, 2);
        $data{$num} = {
            aseq_id => $aseqId,
            data => from_json($json)
        };
    }
    else {
        $fh->close();
        delete $sources{$num};
        delete $data{$num};
    }
}

# Reads the first record from all handles for each field type
sub initializeData {
    foreach my $num (keys %sources) {
        &readNextRecord($num);
    }
}
