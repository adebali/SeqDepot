#!/usr/bin/perl

use strict;
use warnings;
use File::Temp 'tempfile';

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 [<pdb seqres file>]

USAGE

# ------------------------------------------------------------
# Download the PDB database
my $seqres_file = shift;
if ($seqres_file && !-e $seqres_file) {
    die qq(Invalid file '$seqres_file'\n);
}

if (!$seqres_file) {
    my $g_SeqresDbUrl = 'ftp://ftp.wwpdb.org/pub/pdb/derived_data/pdb_seqres.txt.gz';
    my $fh;
    ($fh, $seqres_file) = tempfile('seqdepot_pdb_seqres_XXXXXX', SUFFIX => '.gz', DIR => '/tmp', UNLINK => 0);
    $fh->close();
    system(qq(wget -O $seqres_file $g_SeqresDbUrl));
    die qq([$0] Unable to download $g_SeqresDbUrl\n) if ($? == -1);
}

my %pdbs = ();

my $fh = &Common::openFileOrGzFile($seqres_file);
&Common::startTicker();
while (my $seq = &Common::readFastaSequence($fh)) {
    next if ($seq->[0] !~ /mol:protein/);
    if ($seq->[0] !~ /^(\w+)_(\S+)/)
    {
        print STDERR "Unable to parse id and chain from header: $seq->[0]\n";
        next;
    }
    my ($pdb_id, $chain) = ($1, $2);
    my $prot_seq = $$seq[1];
    if (index($prot_seq, '@') != -1) {
        print STDERR qq([Warn] sequence contains invalid characters $$seq[0] - $prot_seq\n);
        next;
    }

    my $l = length($prot_seq);
    next if ($l < $Common::g_MinLen);

    my $data = &Common::baseStructure($prot_seq);
    $data->{x}->{pdb} = [$pdb_id];

    my $aseqId = $data->{_id};
    if (!$pdbs{$aseqId}) {
        $pdbs{$aseqId} = $data;
    }
    else {
        my $ref = $pdbs{$aseqId};
        my $newId = 1;
        foreach my $oldId (@{$ref->{x}->{pdb}}) {
            if ($oldId eq $pdb_id) {
                $newId = 0;
                last;
            }
        }

        if ($newId) {
            push @{ $ref->{x}->{pdb} }, $pdb_id;
        }
    }
    &Common::tick();
}

foreach my $aseqId (keys %pdbs) {
    &Common::printPJSON($pdbs{$aseqId});
}