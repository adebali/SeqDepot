#!/usr/bin/perl
#
# Program: load_pdb.pl
# Author: Luke Ulrich
# Date: 21 April 2010
# Description: Download and integrate the protein entries in the PDB database into SeqDepot.
#
#              Similar to the UniRef XML loader, we simply reload all the data each time this script is run
#              rather than check for differences.

$| = 1;

use strict;
use warnings;

use Digest::MD5 'md5_base64';
use File::Temp 'tempfile';
use LWP::UserAgent;

use Time::HiRes qw(gettimeofday tv_interval);
use MongoDB;

my $g_MinLen = 30;

# my $g_EntryTypeUrl = 'ftp://ftp.wwpdb.org/pub/pdb/derived_data/pdb_entry_type.txt';

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

my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $collection = $db->get_collection('aseqs');

# ------------
# Read the fasta seqres file
my $inserted = 0;
my $updated = 0;
my $i = 0;

my $cmd = qq(zcat $seqres_file |);
if ($seqres_file !~ /\.gz$/) {
    $cmd = $seqres_file;
}

my $t0 = [gettimeofday];
open (IN, "$cmd") or die qq([$0] Unable to open file '$seqres_file': $!\n);
while (my $seq = &readFastaSequence(*IN)) {
    # Only consider protein sequences
    next if ($seq->[0] !~ /mol:protein/);

    if ($seq->[0] !~ /^(\w+)_(\S+)/)
    {
        print STDERR "Unable to parse id and chain from header: $seq->[0]\n";
        next;
    }
    my ($pdb_id, $chain) = ($1, $2);

    # Only thing remaining is the sequence
    my $prot_seq = $$seq[1];
    $prot_seq =~ s/\s+//g;
    $prot_seq =~ s/\W|\d/@/g;

    if (index($prot_seq, '@') != -1) {
        print STDERR qq([Warn] sequence contains invalid characters $pdb_id - $prot_seq\n);
        next;
    }

    my $l = length($prot_seq);
    next if ($l < $g_MinLen);

    # Data as though we are inserting a fresh record
    my %data = (
        _id => &digest($prot_seq),
        s => $prot_seq,
        l => $l,
        t => {},
        x => {
            pdb => [$pdb_id]
        }
    );

    # Do the lookup
    my $object = $collection->find_one({_id => $data{_id}}, {_id => 1, 'x.pdb' => 1});
    if (!$object) {
        ++$inserted;
        $collection->insert(\%data, {safe => 1});
    }
    else {
        my $update = undef;
        if (!$object->{x}->{pdb}) {
            $update = {
                '$set' => {
                    'x.pdb' => $data{x}->{pdb}
                }
            };
        }
        else {
            my $newIds = &findNewIds($data{x}->{pdb}, $object->{x}->{pdb});
            if (@$newIds) {
                $update = {
                    '$pushAll' => {
                        'x.pdb' => $newIds
                    }
                };
            }
        }

        if ($update) {
            ++$updated;
            $collection->update({_id => $data{_id}}, $update, {safe => 1});
        }
    }

    ++$i;
    if ($i % 200 == 0) {
        print STDERR '.';
        if ($i % 10000 == 0) {
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i sequences [%.3f seconds, $inserted new, $updated updated %.1f / s]\n", $elapsed, 10000 / $elapsed;
            $t0 = [gettimeofday];
            $inserted = 0;
            $updated = 0;
        }
    }

    last if ($seq->[2]);
}
close (IN);

print STDERR "\nRecords: $i protein PDB sequences\n";

# unlink($seqres_file);

my $__buffer = '';
sub readFastaSequence
{
    my $fh = shift or return;
    my $keep_extra = shift;

    while (my $line = <$fh>)
    {
        $line =~ tr/\r//d;
        $__buffer .= $line;

        if (length($__buffer) &&
            $__buffer =~ s/^>([^\n]+)\n(.*?\n)(?=>)//ms)
        {
            my $header = $1;
            my $sequence = $2;
            $header =~ s/^\s*//;
            $header =~ s/\s*$//;

            $sequence =~ s/\s+//g;
            $sequence =~ s/\W|\d/X/g;

            return [ $header, $sequence ];
        }
    }

    # We only get here if the finished reading last bit from the filehandle; thus, all sequence
    # data following the last caret belongs to the last sequence
    if ($__buffer && length($__buffer) &&
        $__buffer =~ s/^>([^\n]+)\n(.*)//ms)
    {
        my $header = $1;
        my $sequence = $2;
        $header =~ s/^\s*//;
        $header =~ s/\s*$//;

        $sequence =~ s/\s+//g;
        $sequence =~ s/\W|\d/X/g;

        return [ $header, $sequence, 'done' ];
    }

    return;
}

sub findNewIds {
    my $fileIds = shift or die;
    my $dbIds = shift or die;

    my %hash = ();
    map {$hash{$_} = 1} @$dbIds;

    my $newIds = [];
    foreach my $id (@$fileIds) {
        next if ($hash{$id});

        push @$newIds, $id;
    }

    return $newIds;
}

sub digest {
    my $sequence = shift or die;

    my $base64 = md5_base64($sequence);
    $base64 =~ tr/+\//-_/;

    return $base64;
}
