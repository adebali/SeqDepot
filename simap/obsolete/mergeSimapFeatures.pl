#!/usr/bin/perl

use strict;
use warnings;
use IO::File;
use JSON;
use Time::HiRes qw(gettimeofday tv_interval);

my $usage = <<USAGE;
Usage: $0 <feature file> [...]

USAGE

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
while (my $aseq_id = &nextLowestAseqId()) {
    my $rowData = &dataForAseqId($aseq_id);

    print $aseq_id, "\t", to_json($rowData), "\n";

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

die qq(What!?\n) if (scalar(keys %data) > 0);


# --------------------------------------------------------------------------------------------------------------------
sub nextLowestAseqId {
    my $lowest = undef;
    foreach my $fieldName (keys %data) {
        my $aseqId = $data{$fieldName}->{aseq_id};
        $lowest = $aseqId if (!defined($lowest) || $aseqId lt $lowest);
    }

    return $lowest;
}

sub dataForAseqId {
    my $aseqId = shift;
    my %merged = ();

    foreach my $fieldName (keys %data) {
        my $ref = $data{$fieldName};
        my $otherAseqId = $ref->{aseq_id};
        if ($otherAseqId eq $aseqId) {
            $merged{$fieldName} = $ref->{data};

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
