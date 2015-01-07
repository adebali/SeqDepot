#!/usr/bin/perl

$| = 1;

use strict;
use warnings;
use Getopt::Long;
use IO::File;
use JSON;

use FindBin '$Bin';
use lib "$Bin/lib";
use Common;

my $usage = <<USAGE;
Usage: $0 [options] <tool base directory> <tool csv string>

  Expects a directory beneath <tool base directory> for each
  tool expressed in <tool csv string> that contains a SeqDepot
  results.pseudo_json file. These are then merged into a single
  pseudo JSON file that is output to STDOUT:

    <base64 id> [TAB] {t.pfam26: [], ...}
    ...

  Available options
  -----------------
    -e, --error-file = string        : file name to write any errors to.

USAGE

my $g_Help;
my $g_ErrFile;

GetOptions("h|help", \$g_Help,
	   "e|error-file=s", \$g_ErrFile);
die $usage if ($g_Help);

my $baseDir = shift or die $usage;
my $toolCsv = shift or die $usage;

$baseDir .= '/' if (substr($baseDir, -1) ne '/');
if (!-e $baseDir || !-d $baseDir) {
    &Common::writeErrorAndDie(qq([$0] $baseDir directory is an invalid directory\n), $g_ErrFile);
}

my @files = @{ &checkAndOpenFiles($baseDir, $toolCsv) };

# Read from each file line by line and join its output via JSON
my $seqsProcessed = 0;
while (1) {
    # Possible errors:
    # - Did not read a line from each file
    # - Ids do not correspond from each file
    # - Invalid JSON data for any given line
    my ($id, $data) = &readAndParseOneLine(\@files);
    last if (!$id);

    print $id, "\t", to_json($data), "\n";

    ++$seqsProcessed;
}

if ($seqsProcessed == 0) {
    &Common::writeErrorAndDie(qq([$0] No seqs processed!), $g_ErrFile);
}

# ------------------------------------------------------------
# ------------------------------------------------------------
sub checkAndOpenFiles {
    my $baseDir = shift or die;
    my $toolCsv = shift or die;

    my @files = ();
    my @tools = split(',', $toolCsv);
    foreach my $tool (@tools) {
	my $toolPseudoJsonFile = $baseDir . 'seqs.' . $tool;
	if (!-e $toolPseudoJsonFile || -s $toolPseudoJsonFile == 0) {
	    &Common::writeErrorAndDie(qq([$0] Results file, $toolPseudoJsonFile, does not exist or is empty\n), $g_ErrFile);
	}
	
	my $io = new IO::File("< $toolPseudoJsonFile");
	if (!$io) {
	    &Common::writeErrorAndDie(qq([$0] Error opening file, $toolPseudoJsonFile: $!\n), $g_ErrFile);
	}
	
	push @files, {tool => $tool, io => $io};
    }

    return \@files;
}

sub readAndParseOneLine {
    my $files = shift or die;

    my $commonId = undef;

    my %data = ();
    foreach my $file (@$files) {
	my $line = $file->{io}->getline();
	next if (!defined($line));
	chomp($line);
	
	my $tool = $file->{tool};

	my ($id, $json) = split(/\t/, $line, 2);
	if ($id !~ /\S+/) {
	    &Common::writeErrorAndDie(qq([$0] Unable to read id for tool, $tool. Line: $line\n), $g_ErrFile);
	}
	$commonId = $id if (!$commonId);
	if ($id ne $commonId) {
	    &Common::writeErrorAndDie(qq([$0] Id mismatch for tool, $tool. Common id: $commonId. Mismatched id: $id\n), $g_ErrFile);
	}
	
	# Prefix each name with t to be compatible with the SeqDepot schema
	my $result = eval { from_json($json); };
	if (!$result) {
	    &Common::writeErrorAndDie(qq([$0] Invalid JSON for $id: $json\n), $g_ErrFile);
	}
	$data{'t.' . $tool} = $result;
    }

    return ($commonId, \%data);
}
