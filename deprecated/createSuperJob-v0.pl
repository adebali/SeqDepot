#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use IO::File;
use MongoDB;
use Time::HiRes qw(gettimeofday tv_interval);

my $usage = <<USAGE;
Usage: $0 [options] <job name>

  Available options
  -----------------
    -C, --directory DIR        : Output directory; defaults to the
                                 current working directory
    -n, --number-of-batches N  : Max number of batches to create;
                                 0 (default) indicates to do as
                                 many as are needed.
    -s, --batch-size SIZE      : Number of sequences per batch;
                                 defaults to 10M
    -t, --tools=aliases        : CSV List of tool aliases to analyze;
                                 defaults to all tools defined in
                                 the meta collection

USAGE

my $g_Directory;
my $g_Tools;
my $g_BatchSize;
my $g_NumBatches;

GetOptions("C|directory=s", \$g_Directory,
           "n|number-of-batches=i", \$g_NumBatches,
           "s|batch-size=i", \$g_BatchSize,
           "t|tools=s", \$g_Tools);



$g_Directory = '.' if (!defined($g_Directory));
$g_Directory .= '/' if ($g_Directory !~ /\/$/);
$g_NumBatches = 0 if (!defined($g_NumBatches));
$g_BatchSize = 10000000 if (!defined($g_BatchSize));

my $g_JobName = shift or die $usage;
die qq(Invalid directory: $g_Directory\n) if (!-d $g_Directory);
die qq(Batch size must be positive\n) if ($g_BatchSize < 1);
die qq(Number of batches must >= 0\n) if ($g_NumBatches < 0);

$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $aseqs = $db->get_collection('aseqs');

if (defined($g_Tools)) {
    $g_Tools = [ split(',', $g_Tools) ];
    &dieIfContainsInvalidTool($g_Tools);
}
else {
    $g_Tools = &getTools() if (!defined($g_Tools));
    die qq(No tools found in database\n) if (!$g_Tools || @$g_Tools == 0);
}

# --------------------------------------------------------------------------------------------------------------------
# Minor setup
$g_Directory .= $g_JobName . '/';
mkdir($g_Directory);

# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# Main
my $fields = {s => 1, t => 1};
my $conditions = &buildConditions($g_Tools);
my $cursor = $aseqs->find($conditions);
$cursor->fields($fields);
if ($g_NumBatches > 0) {
    $cursor->limit(($g_NumBatches + 1) * $g_BatchSize);
}
my %toolCombosIOs = ();
my $i = 0;
my $curBatchNo = -1;
my $targetDirectory;
my $toolComboDir = 'a';
my $t0 = [gettimeofday];
while (my $doc = $cursor->next()) {
    my @undoneTools = sort @{ &findUndoneTools($doc, $g_Tools) };
    if (@undoneTools == 0) {
        print STDERR '-'x60, "\n";
        print STDERR qq(Error!\n);
        print Dumper($doc);
        die qq(Impossible condition! No undone tools, yet query found one\n);
    }

    my $toolCombo = join(',', @undoneTools);

    my $targetBatch = int($i / $g_BatchSize);
    if ($targetBatch != $curBatchNo) {
        foreach my $combo (keys %toolCombosIOs) {
            $toolCombosIOs{$combo}->close();
        }
        %toolCombosIOs = ();

        ++$curBatchNo;
        my $numberOfCompleteBatches = $curBatchNo;
        last if ($g_NumBatches != 0 && $numberOfCompleteBatches == $g_NumBatches);

        $targetDirectory = $g_Directory . $curBatchNo . '/';
        mkdir($targetDirectory);
        $toolComboDir = 'a';
    }

    if (!exists $toolCombosIOs{$toolCombo}) {
        my $dir = $targetDirectory . $toolComboDir . '/';
        mkdir($dir);
        &createToolListFile($dir . 'tools.csv', $toolCombo);
        my $toolComboFile = $dir . 'seqs';
        $toolCombosIOs{$toolCombo} = new IO::File("> $toolComboFile");
        ++$toolComboDir; # Put the next tool combo in a different directory
    }

    my $fh = $toolCombosIOs{$toolCombo};

    my $globalSeqPrefix = 'X';
    #  ^^^^^^^^^^^^^^^^ Some programs (e.g. HMMER2.4) do not correctly handle sequences that do begin with a dash. To
    # circumvent this issue, always prepend each sequence id with an X. When inserting the results back into the database,
    # it will be necessary to remove this prefix.

    print $fh ">", $globalSeqPrefix, $doc->{_id}, "\n", $doc->{s}, "\n";

    ++$i;
    if ($i % 2000 == 0) {
        print STDERR '.';
        if ($i % 100000 == 0) {
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i sequences exported [%.3f seconds, batch: $curBatchNo]\n", $elapsed, 10000 / $elapsed;
            $t0 = [gettimeofday];
        }
    }
}

print STDERR qq(\nDumped $i sequences\n);
if ($i == 0) {
    print STDERR " --> Nothing to do!\n";
    rmdir($g_Directory);
}

# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# Subroutines
sub getTools {
    my $toolCollection = $db->get_collection('tools');
    my $cursor = $toolCollection->find({}, {_id => 1});

    my @tools = ();
    while (my $doc = $cursor->next()) {
        push @tools, $doc->{_id};
    }

    return \@tools;
}

sub dieIfContainsInvalidTool {
    my $userTools = shift or die;
    my $dbTools = &getTools();
    die qq(No tools defined in database\n) if (!$dbTools);
    my %dbTools = ();
    map {$dbTools{$_} = 1} @$dbTools;

    foreach my $userTool (@$userTools) {
        die qq(Invalid tool specified: $userTool\n) if (!$dbTools{$userTool});
    }
}

sub buildConditions {
    my $tools = shift or die;
    my $or = [];
    foreach my $tool (@$tools) {
        push @$or, {'t.' . $tool => {'$exists' => 0}};
    }
    return {'$or' => $or};
}

sub findUndoneTools {
    my $doc = shift or die;
    my $toolsToDo = shift or die;

    my @undone = ();
    foreach my $toolToDo (@$toolsToDo) {
        push @undone, $toolToDo if (!exists($doc->{t}->{$toolToDo}));
    }
    return \@undone;
}

sub createToolListFile {
    my $toolFile = shift or die;
    my $combo = shift or die;

    open (OUT, "> $toolFile") or die qq([$0] Unable to create file $toolFile: $!\n);
    print OUT $combo, "\n";
    close (OUT);
}
