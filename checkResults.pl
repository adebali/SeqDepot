#!/usr/bin/perl
#
# Launches a SeqDepot super job.

$| = 1;

use strict;
use warnings;

use FindBin '$Bin';
use JSON;

my $usage = <<USAGE;
Usage: $0 <job name>

  Checks that the results for a job are all valid and formatted
  as expected.

USAGE

my $jobName = shift or die $usage;
my $jobsPath = $Bin . '/../jobs/';
my $jobPath = $jobsPath . $jobName . '/';
my $jobTarFile = $jobsPath . $jobName . '.tar.gz';
my $doneFile = $jobsPath . $jobName . '-done';    # Create this file once the entire process is complete

chdir($jobsPath);

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

	# Count how many split sequences there are
	my $nChunks = &countSeqFiles($toolGroupDir);
	die qq(No chunks were found on the filesystem!\n) if ($nChunks == 0);

	# Read in the tools to be analyzed
	my @tools = @{ &readToolFile($toolFile) };
	die qq(No tools defined for this batch of sequences\n) if (@tools == 0);

        # Read the first and last identifier of each sequence file
	print "Reading id boundaries\n";
	my %idBounds;
	for my $i (1 .. $nChunks) {
	    my $file = "$toolGroupDir/seqs.$i";
	    open (IN, "< $file") or die qq(Unable to open $file: $!\n);
	    my $line = <IN>;
	    die qq(Malformed id line: $line\n) if ($line !~ /^>(\S+)/);
	    $idBounds{$i}->{first} = $1;
	    close (IN);

	    open (IN, "tac $file |") or die qq(Unable to tac file: $!\n);
	    while (<IN>) {
		if (/^>(\S+)/) {
		    $idBounds{$i}->{last} = $1;
		    last;
		}
	    }
	    close (IN);

            die qq(Unable to extract last id from $file\n) if (!$idBounds{$i}->{last});
            print '.';
            print " [$i]\n" if ($i % 50 == 0);
	}
        print " [$nChunks]\n";

	foreach my $tool (@tools) {
	    print "Checking results for tool: $tool\n";
	    my $dir = "$toolGroupDir/$tool";
	    die qq(Missing tool result directory: $dir\n) if (!-d $dir);

	    for my $i (1 .. $nChunks) {
		my $file = "$dir/seqs.$i.$tool";
		die qq(Empty result file: $file\n) if (-s $file == 0);

		open (IN, "< $file") or die qq(Unable to open $file: $!\n);
		my $line = <IN>;
		die qq(Malformed id in first line of result file: $line\n) if ($line !~ /^(\S+)\t\S/);
		close (IN);

		my $firstId = $1;
		die qq(First id in sequence file ($idBounds{$i}->{first}) does not match first id in result file ($firstId)\n) if ($firstId ne $idBounds{$i}->{first});
		
		open (IN, "tail -n 1 $file |") or die qq(Unable to tail file: $!\n);
                $line = <IN>;
		die qq(Malformed id in last line of result file: $line\n) if ($line !~ /^(\S+)\t(.*)/);
		close (IN);

                my $lastId = $1;
                my $json = $2;

		die qq(Last id in sequence file ($idBounds{$i}->{last}) does not match last id in result file ($lastId)\n) if ($lastId ne $idBounds{$i}->{last});
               
                # Check that the JSON is valid
                my $data = from_json($json);

                print '.';
                print " [$i]\n" if ($i % 50 == 0);
	    }

	    print "\n\n";
	}
    }
}

print "\nALL CHECKS PASSED!\n\n";

# ------------------------------------------------------------------------------
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
