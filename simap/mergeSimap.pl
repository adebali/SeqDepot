#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use IO::File;
use JSON;
use Time::HiRes qw(gettimeofday tv_interval);

my $usage = <<USAGE;
Usage: $0 <gzipped sequences> <feature file> [...]

   All files must be tab-delimited with the first column
   containing the aseq_id and have a gzip extension. Moreover,
   all feature files should be in pseudo json.

USAGE

my $g_SeqFile = shift or die $usage;
die $usage if (@ARGV == 0);

my %featureDbMap = (
    Coil => 'coils',
    Gene3D => 'gene3d',
    HAMAP => 'hamap',
    HMMPanther => 'panther',
    PatternScan => 'patscan',
    HMMPfam => 'pfam',
    HMMPIR => 'pir',
    FPrintScan => 'prints',
    ProfileScan => 'proscan',
    Seg => 'segs',
    SignalP => 'signalp',
    HMMSmart => 'smart',
    superfamily => 'superfam',
    TargetP => 'targetp',
    HMMTigr => 'tigrfam',
    TMHMM => 'tmhmm'
);

my %sources = ();
my %data = ();      # {fieldName => {aseq_id, decoded json}}

foreach my $file (@ARGV) {
    die qq(Invalid feature file\n) if ($file !~ /features_(\w+)\.pjson.gz$/);
    my $toolType = $1;
    die qq(No configuration for that tool type: $toolType\n) if (!$featureDbMap{$toolType});

    my $fieldName = $featureDbMap{$toolType};
    my $fh = new IO::File("zcat $file |") || die qq(Unable to open file, $file: $!\n);
    $sources{$fieldName} = $fh;
}

my $i = 0;
my $t0 = [gettimeofday];
&initializeData();
open (IN, "zcat $g_SeqFile |") or die qq([$0] Unable to zcat $g_SeqFile: $!\n);
while (my $line = <IN>) {
    chomp($line);

    my ($aseqId, $protSeq) = split(/\t/, $line);

    $protSeq = uc($protSeq);
    $protSeq =~ s/\s+//g;
    $protSeq =~ s/\W|\d/@/g;

    if (index($protSeq, '@') != -1) {
        die qq([Warn] sequence contains invalid characters $aseqId - $protSeq\n);
    }

    my %rowData = (
        s => $protSeq,
        l => length($protSeq),
        t => &dataForAseqId($aseqId)
    );

    print $aseqId, "\t", to_json(\%rowData), "\n";

    ++$i;
    if ($i % 2000 == 0) {
        print STDERR '.';
        if ($i % 100000 == 0) {
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i *merged* [%.3f seconds <> %.1f / s]\n", $elapsed, 100000 / $elapsed;
            $t0 = [gettimeofday];
        }
    }
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
sub dataForAseqId {
    my $aseqId = shift;
    my %merged = ();

    foreach my $fieldName (keys %data) {
        my $ref = $data{$fieldName};
        my $otherAseqId = $ref->{aseq_id};
        if ($otherAseqId eq $aseqId) {
            $merged{$fieldName} = $ref->{data}->{t}->{$fieldName};

            &readNextRecord($fieldName);
        }
    }

    return \%merged;
}

sub readNextRecord {
    my $fieldName = shift or die;

    my $fh = $sources{$fieldName};
    my $line = $fh->getline();
    if ($line) {
        chomp($line);
        my ($aseq_id, $json) = split(/\t/, $line, 2);
        $data{$fieldName} = {
            aseq_id => $aseq_id,
            data => from_json($json)
        };
    }
    else {
        $fh->close();
        delete $sources{$fieldName};
        delete $data{$fieldName};
    }
}

# Reads the first record from all handles for each field type
sub initializeData {
    foreach my $fieldName (keys %sources) {
        &readNextRecord($fieldName);
    }
}
