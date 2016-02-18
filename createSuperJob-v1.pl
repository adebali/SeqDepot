#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use IO::File;
use MongoDB;

use FindBin '$Bin';
use lib $Bin;
use Common;


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
                                 Common.pm

USAGE

my $g_Directory;
my $g_Tools;
my $g_BatchSize;
my $g_NumBatches;
my $g_metaFlag;
my $g_colFlag = "aseqs";

GetOptions("C|directory=s", \$g_Directory,
           "n|number-of-batches=i", \$g_NumBatches,
           "s|batch-size=i", \$g_BatchSize,
           "t|tools=s", \$g_Tools,
           "m|metaFlag=i", \$g_metaFlag,
           "col|colFlag=s", \$g_colFlag);



$g_Directory = '.' if (!defined($g_Directory));
$g_Directory .= '/' if ($g_Directory !~ /\/$/);
$g_NumBatches = 0 if (!defined($g_NumBatches));
$g_BatchSize = 10000000 if (!defined($g_BatchSize));
$g_metaFlag = 0 if (!defined($g_metaFlag));

my $g_JobName = shift or die $usage;
die qq(Invalid directory: $g_Directory\n) if (!-d $g_Directory);
die qq(Batch size must be positive\n) if ($g_BatchSize < 1);
die qq(Number of batches must >= 0\n) if ($g_NumBatches < 0);

my %toolStatusPos = ();
my $i = 0;
foreach my $fieldName (@Common::tools) {
    $toolStatusPos{$fieldName} = $i;
    ++$i;
}

my $doneStatusRegex = '.'x$i;

if (defined($g_Tools)) {
    $g_Tools = [ split(',', $g_Tools) ];
    &dieIfContainsInvalidTool($g_Tools);

    my @donePoses = ();
    map { push @donePoses, $toolStatusPos{$_} } @$g_Tools;
    map { substr($doneStatusRegex, $_, 1, 'X') } sort { $b <=> $a } @donePoses;
    my $tmp = $doneStatusRegex;
    $doneStatusRegex = '';
    while ($tmp =~ /(\.*)(X+)/g) {
        my $nDots = length($1);
        my $nXs = length($2);
        my $dots = '';
        if ($nDots) {
            $dots = ($nDots > 1) ? '.{' . $nDots . '}' : '.';
        }
        my $Xs = ($nXs > 1) ? 'X{' . $nXs . '}' : 'X';
        $doneStatusRegex .= $dots . $Xs;
    }
    $doneStatusRegex =~ s/X/[dT]/g;
}
else {
    $g_Tools = @Common::tools;
    $doneStatusRegex = "[dT]{$i}";
}

# --------------------------------------------------------------------------------------------------------------------
# Also tests the connection because it will die if it cannot connect
my $aseqs = &Common::aseqs($g_colFlag);
	

# --------------------------------------------------------------------------------------------------------------------
# Minor setup
$g_Directory .= $g_JobName . '/';
mkdir($g_Directory);

# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# Main
my $globalSeqPrefix = 'X';
#  ^^^^^^^^^^^^^^^^ Some programs (e.g. HMMER2.4) do not correctly handle sequences that do begin with a dash. To
# circumvent this issue, always prepend each sequence id with an X. When inserting the results back into the database,
# it will be necessary to remove this prefix.

my $limit = ($g_NumBatches) ? ($g_NumBatches + 1) * $g_BatchSize : 999999999;

my %toolCombosIOs = ();
my $curBatchNo = -1;
my $targetDirectory;
my $toolComboDir = 'a';

my $fields = {s => 1, _s => 1};

my $cursor;

if ($g_metaFlag){
	#$cursor = $aseqs->find({ _id => 'CFuX83oc9Sj5rtEXC_nMZA' });
	$cursor = $aseqs->find({ _s => {'$not' => qr/^$doneStatusRegex/} });
	#print("metaFlag\n");
	} else {
	#print("No MetaFlag\n");
	#$cursor = $aseqs->find({ _s => {'$not' => qr/^$doneStatusRegex/} });
	#$cursor = $aseqs->find({ _id => 'CFuX83oc9Sj5rtEXC_nMZA' });
	$cursor = $aseqs->find({ _s => {'$not' => qr/^$doneStatusRegex/}, m => {'$ne' => 1 }});
	}
#my $cursor = $aseqs->find({ _id => 'CFuX83oc9Sj5rtEXC_nMZA' });
#my $cursor = $aseqs->find({ _id => '---OggegDeUFPNveg_YZwQ', m => 1 });
$cursor->immortal(1);
#                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# The regex is true only if all fields are done, false if any of the status fields are not done
$cursor->fields($fields);
&Common::startTicker(2000);
while (my $doc = $cursor->next()) {
    my @undoneTools = sort @{ &findUndoneTools($doc, $g_Tools) };
    if (@undoneTools == 0) {
        print STDERR '-'x60, "\n";
        print STDERR qq(Error!\n);
        print Dumper($doc);
        die qq(Impossible condition! No undone tools, yet query found one\n);
    }

    my $toolCombo = join(',', @undoneTools);

    my $targetBatch = int($Common::ticker{i} / $g_BatchSize);
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
    print $fh ">", $globalSeqPrefix, $doc->{_id}, "\n", $doc->{s}, "\n";
    &Common::tick();

    last if ($Common::ticker{i} >= $limit);
}

print STDERR qq(\nDumped $Common::ticker{i} sequences\n);
if ($Common::ticker{i} == 0) {
    print STDERR " --> Nothing to do!\n";
    rmdir($g_Directory);
}

# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# Subroutines
# sub getTools {
#     my $toolCollection = $db->get_collection('tools');
#     my $cursor = $toolCollection->find({}, {_id => 1});

#     my @tools = ();
#     while (my $doc = $cursor->next()) {
#         push @tools, $doc->{_id};
#     }

#     return \@tools;
# }

sub dieIfContainsInvalidTool {
    my $userTools = shift or die;
    my %dbTools = ();
    map {$dbTools{$_} = 1} @Common::tools;

    foreach my $userTool (@$userTools) {
        die qq(Invalid tool specified: $userTool\n) if (!$dbTools{$userTool});
    }
}

sub findUndoneTools {
    my $doc = shift or die;
    my $toolsToDo = shift or die;

    my @undone = ();
    my $status = $doc->{_s};
    foreach my $fieldName (@$toolsToDo) {
        my $pos = $toolStatusPos{$fieldName};
        my $code = substr($status, $pos, 1);
        push @undone, $fieldName if ($code eq '-');
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
