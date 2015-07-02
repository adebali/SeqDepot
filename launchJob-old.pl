#!/usr/bin/perl
#
# Launches a SeqDepot super job.

$| = 1;

use strict;
use warnings;

use Data::Dumper;
use FindBin '$Bin';

my $usage = <<USAGE;
Usage: $0 <job name>

  Looks in the jobs subdirectory for <job name> and if it
    exists, launches that job. This involves:

    1) Split the fasta sequence data into chunks
    2) Create the SGE scripts for each tool
    3) Submit tool scripts to SGE
       a) Each tool outputs a pseudo JSON file with each line
          containing the id [TAB] JSON results for this sequence
    4) Once all the individual tools are completed, another
       script glues together the results into a composite tool
       result file.
    5) Finally, all the split composite batches are glued into
       a single JSON file, gzipped and sent back to the SeqDepot
       machine.

    Note: it is desirable for external scripts to "watch" the
    progress of this job. Originally, this was done by executing
    qstat and grepping for the job name; however, there is often
    significant amounts of prepwork that must be done first before
    any job is submitted (e.g. decompressing files).

    To accommodate these issues, watching will consist of checking
    the job directory for the existence of a specific file which
    will be created by the final SGE script *after* all processing
    has completed.

USAGE

my $jobName = shift or die $usage;
my $jobsPath = $Bin . '/../jobs/';
my $jobPath = $jobsPath . $jobName . '/';
my $jobTarFile = $jobsPath . $jobName . '.tar.gz';
my $doneFile = $jobsPath . $jobName . '-done';    # Create this file once the entire process is complete

my $baseResultDir = $jobName . '-results/';
my $finalResultDir = $jobPath . $baseResultDir;

my $g_SplitSize = 10000; # 20000;

die qq(Job path, $jobPath, already exists!\n) if (-d $jobPath);

die qq(Expected tarball, $jobTarFile, does not exist\n) if (!-e $jobTarFile);

print "Checking MD5\n";
chdir($jobsPath);
&checkMD5($jobTarFile);

print "Extracting tarball...\n";
system('tar', 'zxvf', $jobTarFile, '-C', $jobsPath);

die qq($jobPath does not exist\n) if (!-d $jobPath);

my $batches = &readSubDirectories($jobPath);
foreach my $batch (sort @$batches) {
    my $batchDir = $jobPath . $batch . '/';

    my $commonName = $jobName . '-' . $batch;

    # Loop through each of the tool configruations
    my $toolComboDirs = &readSubDirectories($batchDir);
    foreach my $toolComboDir (@$toolComboDirs) {
	my $toolGroupDir = $batchDir . $toolComboDir . '/';

	my $toolFile = $toolGroupDir . 'tools.csv';
	my $seqFile = $toolGroupDir . 'seqs';

	print "Processing $batch.$toolComboDir\n";

	# 1) Split the sequences into smaller chunks
	print "Splitting sequences into chunks of $g_SplitSize sequences each\n";
	chdir($toolGroupDir); # Move to the directory where we want to store the chunks
	system('/home/lulrich/bin/fasplit', '-n', $g_SplitSize, '-d', $seqFile);

	# 2) Count how many split sequences there are
	my $nChunks = &countSeqFiles($toolGroupDir);

	die qq(After splitting, no chunks were found on the filesystem!\n) if ($nChunks == 0);

	# 3) Read in the tools to be analyzed
	my @tools = @{ &readToolFile($toolFile) };
	die qq(No tools defined for this batch of sequences\n) if (@tools == 0);

	# 4) Create the SGE script for each tool
	my $commonBatchToolName = $commonName . '-' . $toolComboDir;
	foreach my $tool (@tools) {
	    my $scriptFile = &createSGEScript($tool, $toolGroupDir, 'seqs', $nChunks, $commonBatchToolName);
	    system('qsub', $scriptFile);
	}

	# 5) Once the above scripts complete, glue the results together for this tool combination
	my $toolGlueScript = &createGlueToolDataScript($toolGroupDir, 'seqs', $nChunks, \@tools, $commonName);
	system('qsub', '-hold_jid', $commonBatchToolName, $toolGlueScript);
    }

    # Sub-Finally, concatenate all the merged data files
    my $concatScript = &createConcatScript($batch, $batchDir, $toolComboDirs);
    system('qsub', '-hold_jid', $commonName, $concatScript);
}

my $finalScript = &createFinalizeBatchesScript($jobPath, $baseResultDir);
system('qsub', '-hold_jid', 'processBatch', $finalScript);
#                            ^^^^^^^^^^^^ Name of script that combines results of an individual batch

# ------------------------------------------------------------------------------
sub createLockFile {
    my $file = shift or die;
    open (OUT, "> $file") or die qq(Could not create lock file, $file: $!\n);
    close (OUT);
}

sub checkMD5 {
    my $tarFile = shift or die;
    my $md5File = $tarFile . '.md5';

    die qq(MD5 file, $md5File, does not exist!\n) if (!-e $md5File);

    open (IN, "md5sum -c $md5File |") or die qq(Could not md5 tarball\n);
    my $check = <IN>;
    close (IN);

    die qq(MD5 checksums do not match!\n) if ($check !~ /OK/);
}

sub readSubDirectories {
    my $dir = shift or die;
    opendir(my $dh, $dir) or die qq(Unable to read directory, $dir: $!\n);
    my @subdirs = grep { !/^\./ && -d "$dir/$_" } readdir($dh);
    closedir($dh);

    return \@subdirs;
}

sub countSeqFiles {
    my $dir = shift or die;

    my $seqFileCount = 0;

    opendir(DIR, $dir) or die qq(Unable to read directory, $dir: $!\n);
    while (my $x = readdir(DIR)) {
	$seqFileCount++ if ($x =~ /^seqs.\d+$/);
    }
    closedir(DIR);

    return $seqFileCount;
}

sub readToolFile {
    my $file = shift or die;
    open (IN, "< $file") or die qq([$0] Unable to open file '$file': $!\n);
    my $line = <IN>;
    close (IN);

    chomp($line);

    return [ split(',', $line) ];
}

sub createSGEScript {
    my $toolName = shift or die;
    my $toolGroupDir = shift or die;
    my $baseFastaName = shift or die;
    my $chunks = shift or die;
    my $sgeName = shift or die;
    my $sgePath = $toolGroupDir . '/sge/';
    mkdir($sgePath);

    # Remove the terminal slash
    $toolGroupDir =~ s/\/$//;

    my $queue = &queueForTool($toolName);

    my $script = <<HEADER;
#!/bin/bash
#
# Use host environment
#\$ -V
#
#\$ -N $sgeName
#\$ -t 1-$chunks
#\$ -q $queue*
#
#\$ -o /dev/null
#\$ -e /dev/null

set TOOL_NAME=$toolName
set DATA_DIR=$toolGroupDir
set OUT_DIR=\$DATA_DIR/\$TOOL_NAME
set BASEINFILE=$baseFastaName.\$SGE_TASK_ID
set INFILE=\$DATA_DIR/\$BASEINFILE
set OUTFILE=\$OUT_DIR/\$BASEINFILE.$toolName
set ERRFILE=\$OUT_DIR/errors

mkdir \$OUT_DIR
HEADER

    $script .= &sgeCommandsForTool($toolName);

    my $sgeFile = $sgePath . $toolName . '.sh';
    &writeSgeFile($sgeFile, $script);
    return $sgeFile;
}

sub queueForTool {
    my $toolName = shift or die;

    my %queues = (
        pfam26 => 'medium',
        pfam27 => 'medium',
	ecf => 'short',
	agfam1 => 'short',
        das => 'medium',
	segs => 'short',
        coils => 'short'
    );

    return $queues{$toolName} if (exists $queues{$toolName});

    return 'medium';
}

sub sgeCommandsForTool {
    my $toolName = shift or die;

    if ($toolName eq 'agfam1') {
	return &sge_agfam1();
    }
    elsif ($toolName eq 'coils') {
	return &sge_coils();
    }
    elsif ($toolName eq 'das') {
	return &sge_das();
    }
    elsif ($toolName eq 'ecf') {
	return &sge_ecf();
    }
    elsif ($toolName eq 'pfam26') {
	return &sge_pfam26();
    }
    elsif ($toolName eq 'pfam27') {
	return &sge_pfam27();
    }
    elsif ($toolName eq 'segs') {
	return &sge_segs();
    }

    die qq(Missing commands for tool: $toolName\n);
}

sub sge_agfam1 {
    return <<AGFAM1;
set HMMSEARCH=/home/lulrich/tools/hmmer-2.4i/src/hmmpfam
set AGFAM=/lustre/zhulin/lulrich/agfam/agfam.bin
set PARSER=$Bin/h22pseudo_json.pl

\$HMMSEARCH --cpu 0 --cut_ga \$AGFAM \$INFILE | \$PARSER -i \$INFILE -e \$ERRFILE > \$OUTFILE.tmp
$Bin/cullDomainOverlaps.pl -l 3 \$OUTFILE.tmp > \$OUTFILE
rm \$OUTFILE.tmp

AGFAM1
}

sub sge_coils {
    return <<COILS;
set TOOL=/home/lulrich/tools/coils2/coils2
set PARSER=/home/lulrich/lustre/seqdepot/scripts/segsOrCoils2pseudo_json.pl

\$TOOL -f < \$INFILE | \$PARSER -i \$INFILE -e \$ERRFILE > \$OUTFILE
COILS
}

sub sge_das {
    return <<DAS;
set TOOL=/home/lulrich/tools/das/das32

\$TOOL -s -u \$INFILE | $Bin/das2pseudo_json.pl -i \$INFILE -e \$ERRFILE > \$OUTFILE

DAS
}

sub sge_ecf {
    return <<ECF;
set TOOL=$Bin/predictEcf.pl
\$TOOL -e \$ERRFILE \$INFILE > \$OUTFILE

ECF
}

sub sge_pfam26 {
    return <<PFAM26;
set HMMSEARCH=/home/lulrich/tools/hmmer-3.0/src/hmmsearch
set PFAM=/lustre/zhulin/lulrich/pfam/26.0/pfam-a.hmm
set TBLFILE=\$OUT_DIR/\$BASEINFILE.domtbl

set HMMS_SEARCHED=`\$HMMSEARCH --cpu 0 --cut_ga --domtblout \$TBLFILE \$PFAM \$INFILE | grep -c '//'`
# For some reason, there are 13673 families as determined by the grep output rather than 13672 as reported
# by pfam. Perhaps this is due to grep catching more // than expected.
if \$HMMS_SEARCHED != 13673 then
  echo "Failed to complete searching all hmms (\$HMMS_SEARCHED, SGE ID: \$SGE_TASK_ID)" >> \$ERRFILE
else
  $Bin/h3tbl2pseudo_json.pl \$TBLFILE \$INFILE \$ERRFILE > \$OUTFILE
  rm \$TBLFILE
endif
PFAM26
}

sub sge_pfam27 {
    return <<PFAM27;
set HMMSEARCH=/home/lulrich/tools/hmmer-3.1b1/binaries/hmmsearch
set PFAM=/lustre/zhulin/lulrich/pfam/27.0/pfam-a.hmm
set TBLFILE=\$OUT_DIR/\$BASEINFILE.domtbl

set HMMS_SEARCHED=`\$HMMSEARCH --cpu 0 --cut_ga --domtblout \$TBLFILE \$PFAM \$INFILE | grep -c '//'`
$Bin/h3tbl2pseudo_json.pl \$TBLFILE \$INFILE \$ERRFILE > \$OUTFILE
rm \$TBLFILE
PFAM27
}

sub sge_segs {
    return <<SEG;
set TOOL=/home/lulrich/tools/seg/seg
set PARSER=/home/lulrich/lustre/seqdepot/scripts/segsOrCoils2pseudo_json.pl

\$TOOL \$INFILE -x | \$PARSER -i \$INFILE -e \$ERRFILE > \$OUTFILE
SEG
}

sub createGlueToolDataScript {
    my $toolGroupDir = shift or die;
    my $baseFastaName = shift or die;
    my $nChunks = shift;
    my $tools = shift;
    my $commonScriptName = shift;
    my $sgePath = $toolGroupDir . '/sge/';
    mkdir($sgePath);

    my $tool_ssv = join(' ', @$tools);
    my $tool_csv = join(',', @$tools);

    my $script = <<GLUE_SCRIPT;
$!/bin/bash
#\$ -V
#
#\$ -N $commonScriptName
#\$ -q medium*
#
#\$ -o /dev/null
#\$ -e /dev/null

foreach tool ($tool_ssv)
  foreach i (`seq 1 $nChunks`)
    cat $toolGroupDir/\$tool/$baseFastaName.\$i.\$tool >> $toolGroupDir/$baseFastaName.\$tool
  end
end

$Bin/mergeToolData.pl -e $toolGroupDir/glue_errors $toolGroupDir $tool_csv > $toolGroupDir/results.pseudo_json

GLUE_SCRIPT

    my $sgeFile = $sgePath . 'merge.sh';
    &writeSgeFile($sgeFile, $script);
    return $sgeFile;
}

sub createConcatScript {
    my $batchNo = shift;
    die if (!defined($batchNo));
    my $batchDir = shift or die;
    my $toolComboDirs = shift or die;

    my $tool_ssv = join(' ', @$toolComboDirs);

    my $script = <<CONCAT;
$!/bin/bash
#\$ -V
#\$ -N processBatch
#
#\$ -q medium*
#
#\$ -o /dev/null
#\$ -e /dev/null

mkdir $finalResultDir

set OUTFILE=$finalResultDir$batchNo.pseudo_json
set ERRFILE=$finalResultDir$batchNo.errors
set NO_ERRORS=1

foreach x ($tool_ssv)
  if (-e $batchDir/\$x/glue_errors) then
    cat $batchDir/\$x/glue_errors >> \$ERRFILE
    set \$NO_ERRORS=0
    break
  endif

  cat $batchDir/\$x/results.pseudo_json >> \$OUTFILE
end

if (\$NO_ERRORS == 1) then
  gzip \$OUTFILE

  # Do some cleanup
  rm -rf $batchDir
else
  # An error occurred - remove the file
  rm \$OUTFILE
endif

CONCAT

    my $sgeFile = $batchDir . 'concatToolCombos.sh';
    &writeSgeFile($sgeFile, $script);
    return $sgeFile;
}

sub createFinalizeBatchesScript {
    my $jobPath = shift or die;
    my $resultDir = shift or die;

    $resultDir =~ s/\/$//;
    my $sgeFile = $jobPath . 'finalize.sh';

    my $script = <<SGE;
#\$ -V
#
#\$ -q medium*
#
#\$ -o /dev/null
#\$ -e /dev/null
#
#\$ -m e
#\$ -M ulrich.luke\@gmail.com

cd $jobPath
set NERRORS=`ls $resultDir/*.errors | wc -l`
if (\$NERRORS == 0) then
  tar cf $resultDir.tar $resultDir
  md5sum $resultDir.tar > $resultDir.tar.md5
  rm -rf $resultDir
  rm $sgeFile
endif

# Signal that the whole process is done
touch $doneFile;
SGE

    &writeSgeFile($sgeFile, $script);
    return $sgeFile;
}

sub writeSgeFile {
    my $sgeFile = shift or die;
    my $contents = shift or die;

    open (OUT, "> $sgeFile") or die qq([$0] Unable to create file, $sgeFile: $!\n);
    print OUT $contents;
    close (OUT);
}
