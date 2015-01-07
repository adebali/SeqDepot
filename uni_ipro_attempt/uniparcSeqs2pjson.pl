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

&Common::startTicker();
while (my $seq = &Common::readFastaSequence($fh)) {
    # Extract the UPI
    die qq(Unable to extract UPI from: $$seq[0]\n) if ($$seq[0] !~ /(UPI\d+)/);

    my $upi = $1;
    if (!$curXref) {
        $curXref = [ &readIds() ];
    }

    # Case 1) Gap in the ID mapping set
    while ($upi gt $curXref->[0]) {
        $curXref = [ &readIds() ];
        ++$xrefsWithoutUpi;
    }

    # Case 2) the UPIs is less than or equal to the id upi
    my $data = &Common::baseStructure($$seq[1]);
    $data->{_s} = &Common::emptyStatus();
    if ($upi eq $curXref->[0]) {
        # If they are equal, add in the xrefs and remove the current xref data
        $data->{x} = $curXref->[1]->{x};
        undef $curXref;
    }
    else {
        # upi is less than the id xref - keep the same xref for the next iteration
        ++$upisWithoutXrefs;
    }
    &Common::printPJSON($data);
    &Common::tick();
}

$fh->close();
$idFh->close();

print STDERR qq(\n\n);

sub readIds {
    my $idLine = <$idFh>;
    chomp($idLine);
    my ($upi, $json) = split(/\t/, $idLine, 2);
    return ($upi, from_json($json));
}
