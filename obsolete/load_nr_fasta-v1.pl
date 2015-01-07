#!/usr/bin/perl
#
# Program: load_nr_fasta.pl
# Author: Luke Ulrich
# Date: 8 Apr 2010
# Description: Load NR fasta data into the SeqDepot, build cross-references to GI numbers, and store the associated
#              descriptions.
#
# Ignores any sequence with any non-word character or digits.

$| = 1;

use strict;
use warnings;
use Data::Dumper;
use File::Temp 'tempfile';
use Time::HiRes qw(gettimeofday tv_interval);

use FindBin '$Bin';
use lib $Bin;
use Common;

use lib '/binf/lib/perl/modules';
use BinfUtil;

my $g_MinLen = 30;

my $nr_file = shift;
if ($nr_file && !-e $nr_file) {
    die qq(Invalid file '$nr_file'\n);
}

if (!$nr_file) {
    my $g_NrUrl = 'ftp://ftp.ncbi.nih.gov/blast/db/FASTA/nr.gz';
    my $fh;
    ($fh, $nr_file) = tempfile('seqdepot_nr_XXXXXX', SUFFIX => '.gz', DIR => '/tmp');
    $fh->close();
    system(qq(wget -O $nr_file $g_NrUrl));
    die qq([$0] Unable to download $g_NrUrl\n) if ($? == -1);
}

use MongoDB;
$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $collection = $db->get_collection('aseqs');

my $cmd = qq(zcat $nr_file |);
if ($nr_file !~ /\.gz$/) {
    $cmd = $nr_file;
}
open (IN, "$cmd") or die qq([$0] Unable to open file '$nr_file': $!\n);
my $n_gis = 0;
my $new = 0;
my $i = 0;
my $updated = 0;

my $newStatus = &Common::emptyStatus();

my $t0 = [gettimeofday];
# while (my $seq = &readFastaSequence(*IN)) {
while (my $seq = &fastReadFastaSequence(*IN)) {
    # print Dumper($seq);
    # <STDIN>;
    # next;

    # Parse out the gi and descriptions
    my @gis = ();
    chomp($$seq[0]);
    my @deflines = split(/\cA/, $$seq[0]);
    foreach my $defline (@deflines) {
        # if ($defline =~ m/gi\|(\d+)\|[^|]*\|[^|]*\|(?:\S+)? +(?:(.*)\[(.*?)\]|(.*))/) {
        if ($defline =~ /gi\|(\d+)/) {
    	    ++$n_gis;
    	    push @gis, int($1);
        }
    }

    my $prot_seq = $$seq[1];
    if (index($prot_seq, '@') != -1) {
        print STDERR qq([Warn] sequence contains invalid characters $$seq[0] - $prot_seq\n);
        next;
    }

    my $l = length($prot_seq);
    next if ($l < $g_MinLen);

    my $aseqId = &Common::aseqIdFromSequence($prot_seq);
    my $object = $collection->find_one({_id => $aseqId}, {_id => 1}); # , 'x.gi' => 1});
    if (0) {

    # my $object = $collection->find_one({_id => $aseqId});
    # my $cursor = $collection->find({_id => $aseqId}, {_id => 1, 'x.gi' => 1});
    # $cursor->hint({_id => 1});
    # my $object = $cursor->next();
    if (!$object) {
        ++$new;

        # Data as though we are inserting a fresh record
        my %data = (
            _id => $aseqId,
            s => $prot_seq,
            l => $l,
            t => {},
            x => {
                gi => \@gis
            },
            _s => $newStatus
        );
        $data{$Common::paddingKey} = $Common::padding{nr}->{buffer};
        # $collection->insert(\%data, {safe => 1});
        $collection->insert(\%data);
    }
    else {
        my $update = undef;
        my $newIds = [];
        if ($object->{x}->{gi}) {
            $newIds = &findNewIds(\@gis, $object->{x}->{gi});
        }

        if (@$newIds > 0 || !$object->{x}->{gi}) {
            # Always use $set instead of push/pull, etc.
            $update = {
                '$set' => {
                    'x.gi' => \@gis
                }
            };
            # $collection->update({_id => $aseqId}, $update, {safe => 1});
            $collection->update({_id => $aseqId}, $update);
            ++$updated;
        }
    }
    }

    ++$i;
    if ($i % 200 == 0) {
    	print STDERR '.';
    	if ($i % 10000 == 0) {
    	    my $elapsed = tv_interval($t0, [gettimeofday]);
    	    printf STDERR " $i sequences, $n_gis gis [%.3f seconds, $new new, $updated updated <> %.1f / s]\n", $elapsed, 10000 / $elapsed;
    	    $t0 = [gettimeofday];

            $n_gis = 0;
            $new = 0;
            $updated = 0;
    	}
    }

    last if ($seq->[2]);
}
print STDERR "\nRecords: $i\n";

# unlink($nr_file);


# ------------------------------------------------------------
# ------------------------------------------------------------
my $next;
sub fastReadFastaSequence {
    my $fh = shift or die;

    local $/ = "\n>";
    my $buffer = <$fh>;

    if ($next) {
        my $x = index($next, "\n");
        my $header = substr($next, 0, $x);
        my $sequence;
        if ($buffer) {
            $sequence = substr($next, $x + 1, -2);
        }
        else {
            $sequence = substr($next, $x + 1);
        }

        $sequence =~ tr/\n\r\f\t //d;
        $sequence =~ s/\W|\d/@/g;

        $next = $buffer;
        return [$header, $sequence];
    }
    elsif ($buffer) {
        my $x = index($buffer, "\n");
        my $header = substr($buffer, 1, $x - 1);
        my $sequence = substr($buffer, $x + 1);
        $next = <$fh>;
        if ($next) {
            chop($sequence);
            chop($sequence);
        }
        $sequence =~ tr/\n\r\f\t //d;
        $sequence =~ s/\W|\d/@/g;
        return [$header, $sequence];
    }
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

