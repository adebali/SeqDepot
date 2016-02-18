#!/usr/bin/perl

use strict;
use warnings;

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 <fasta file> <cross Reference field>

  Note: <fasta file> may be gzipped (but should have a .gz
        extension.

USAGE

my $file = shift or die $usage;
my $fh = &Common::openFileOrGzFile($file);

my $crossRef = shift or die $usage;

&Common::startTicker();
while (my $seq = &Common::readFastaSequence($fh)) {
    my $prot_seq = $$seq[1];
    if (index($prot_seq, '@') != -1) {
        print STDERR qq([Warn] sequence contains invalid characters $$seq[0] - $prot_seq\n);
        next;
    }

    my $l = length($prot_seq);
    next if ($l < $Common::g_MinLen);

    my $data = &Common::baseStructure($prot_seq);
    # >gi|123234|...
    # ABCACBC
    #
    # parseGis(...) --> [123234, ...]
    if ($crossRef eq "gi"){
    $data->{x}->{gi} = &parseGis($$seq[0]);
    }
    elsif ($crossRef eq "pdb"){
    $data->{x}->{pdb} = &parsePDB_IDs($$seq[0]);
    }
    elsif ($crossRef eq "uni"){
    $data->{x}->{uni} = &parseUniProtIDs($$seq[0]);
    }

    &Common::printPJSON($data);
    &Common::tick();
}

sub parseGis {
    my $header = shift;

    my @gis = ();
    chomp($header);
    my @deflines = split(/\cA/, $header);
    foreach my $defline (@deflines) {
        if ($defline =~ /gi\|(\d+)/) {
            push @gis, int($1);
        }
    }

    return \@gis;
}

sub parsePDB_IDs {
    my $header = shift;
    my @PDB_IDs = ();
    chomp($header);
    my @deflines = split(/ /, $header);
    my $PDB_ID = $deflines[0];
    push @PDB_IDs, $PDB_ID;
    return \@PDB_IDs;
}
sub parseUniProtIDs {
    my $header = shift;
    my @IDs = ();
    chomp($header);
    my @deflines = split(/\|/, $header);
    my $uni_ID = $deflines[1];
    push @IDs, $uni_ID;
    return \@IDs;
}
