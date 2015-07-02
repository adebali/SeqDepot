#!/usr/bin/perl

$| = 1;

use strict;
use warnings;
use Data::Dumper;
use JSON;

use FindBin '$Bin';
use lib "$Bin";
use Common;

my $usage = <<USAGE;
Usage: $0 <result directory>

USAGE

my $g_Directory = shift or die $usage;
$g_Directory .= '/' if ($g_Directory !~ /\/$/);

my @files = @{ &readResultFiles($g_Directory) };

die qq(No result files found\n) if (@files == 0);

my %toolStatusPos = ();
my $i = 0;
foreach my $fieldName (@Common::tools) {
    $toolStatusPos{$fieldName} = $i;
    ++$i;
}

#foreach my $file (sort @files){
#	print  $file;}
#exit 0;

my $processedFiles = 0;
chdir($g_Directory);
my $aseqs = &Common::aseqs();
foreach my $file (sort @files) {
    print STDERR '-'x60, "\n";
    if ($file =~ /^\d+\.errors$/) {
        print STDERR ">> Batch contains errors: $file\n";
        &showErrors($file);
        next;
    }

    next if ($file !~ /^\d+\.pseudo_json\.gz$/);

    print STDERR ">> Processing: $file\n";

    open (IN, "zcat $file | cut -b2- |") or die qq(Unable to open pipe: $!\n);
    #                       ^^^^^^^^ Remove the X prefixed to each sequence ID
    &Common::startTicker();
    while (<IN>) {
        chomp;
        my ($aseqId, $json) = split(/\t/, $_, 2);
        my $data = from_json($json);

        my $aseq = $aseqs->find_one({_id => $aseqId}, {_s => 1});
        if (!$aseq) {
            die qq(Unable to find record with id: $aseqId\n);
        }

        my $newStatus = $aseq->{_s};

        # The output from the cluster job is an entry for every sequence; however, in SeqDepot we
        # do not add a field to the t if there are no predictions found for this tool. For example,
        # if a given sequence does not have any DAS transmembrane regions, the job output will
        # have an empty array, but this field should not exist in the schema. Only its status
        # should be updated.
        my %set = ();
        foreach my $key (keys %$data) {
            if ($key =~ /^t.(\w+)/) {
                my $fieldName = $1;
                my $toolStatus = 'd';
                if (scalar(@{$data->{$key}}) > 0) {
                    $set{$key} = $data->{$key};
                    $toolStatus = 'T';
                }
                substr($newStatus, $toolStatusPos{$fieldName}, 1, $toolStatus);
            }
            else {
                $set{$key} = $data->{$key};
            }
        }

        $set{_s} = $newStatus;

        # $aseqs->update({_id => $aseqId}, {'$set' => \%set}, {safe => 1});
        $aseqs->update({_id => $aseqId}, {'$set' => \%set});
        &Common::tick();
    }
    close (IN);

    print STDERR "\n\n";

    ++$processedFiles;
}

die qq(No result files were found\n) if ($processedFiles == 0);

# --------------------------------------------------------------------------------------------------------------------
sub readResultFiles {
    my $dir = shift or die;
    opendir(my $dh, $dir) or die qq(Unable to read directory, $dir: $!\n);
    my @files = grep { !/^\./ && -f "$dir/$_" } readdir($dh);
    closedir($dh);

    return \@files;
}

sub showErrors {
    my $file = shift or die;
    my $i = 0;
    open (IN, "< $file") or die $!;
    while (<IN>) {
        ++$i;
        print;
        last if ($i == 5);
    }
    close (IN);
}
