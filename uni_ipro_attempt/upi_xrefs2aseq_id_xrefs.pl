#!/usr/bin/perl

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 <uniparc fasta file> <uniparc xref pjson>

USAGE

my $g_File = shift or die $usage;
my $g_IdMap = shift or die $usage;
my $fh = &Common::openFileOrGzFile($g_File);
my $idFh = &Common::openFileOrGzFile($g_IdMap);

my $upisWithoutXrefs = 0;
my $xrefsWithoutUpi = 0;

my $curXref;
my $doneIdFile = 0;

&Common::startTicker(10000);
while (my $seq = &Common::readFastaSequence($fh)) {
    # Extract the UPI
    die qq(Unable to extract UPI from: $$seq[0]\n) if ($$seq[0] !~ /(UPI\w+)/);

    my $upi = $1;
    if (!$curXref) {
        $curXref = [ &readIds() ];
    }
    last if ($doneIdFile);

    # print ">> $upi and $curXref->[0]\n";

    # Case 1) Gap in the ID mapping set
    while ($upi gt $curXref->[0]) {
        # print "---> BLARGHES! $upi vs $curXref->[0]\n";

        $curXref = [ &readIds() ];
        goto END if ($doneIdFile);
        ++$xrefsWithoutUpi;
        # <STDIN>;
    }

    # Case 2) the UPIs is less than or equal to the id upi
    if ($upi eq $curXref->[0]) {
        # If they are equal, add in the xrefs and remove the current xref data
        print &Common::aseqIdFromSequence($$seq[1]), "\t", $curXref->[1], "\n";
        undef $curXref;
    }
    else {
        # print "---> BLARGHES! $curXref->[0] is ahead of $upi\n";
        # upi is less than the id xref - keep the same xref for the next iteration
        ++$upisWithoutXrefs;
        # <STDIN>;
    }
    &Common::tick();
}

END:

$fh->close();
$idFh->close();

print STDERR qq(\n\n);
print STDERR qq(UPIs without xrefs: $upisWithoutXrefs\n);
print STDERR qq(Xrefs without UPIs: $xrefsWithoutUpi\n);

sub readIds {
    my $idLine = <$idFh>;
    if ($idLine) {
        chomp($idLine);
        return split(/\t/, $idLine, 2);
    }
    else {
        $doneIdFile = 1;
    }
}
