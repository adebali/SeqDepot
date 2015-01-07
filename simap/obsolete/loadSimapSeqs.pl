#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

use Digest::MD5 'md5_base64';
use Time::HiRes qw(gettimeofday tv_interval);
use MIME::Base64;

# my $g_MinLen = 30;

my $usage = <<USAGE;
Usage: $0 <sequences.gz>

USAGE

my $g_File = shift || die $usage;

my @tools = qw(agfam1 coils das ecf gene3d hamap panther patternScan pfam pir prints profileScan seg signalp smart superfamily targetp tigrfam tmhmm);
my $status = 'd'x(scalar(@tools));
substr($status, 0, 1, '-');
substr($status, 2, 2, '--');

use MongoDB;
$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $collection = $db->get_collection('aseqs');

my $cmd = ($g_File =~ /\.gz$/) ? qq(zcat $g_File |) : "< $g_File";
open (IN, "$cmd") or die qq([$0] Unable to open file '$g_File': $!\n);

my $i = 0;
my $updated = 0;
my $new = 0;
my $t0 = [gettimeofday];
while (my $line = <IN>) {
    chomp($line);
    my ($md5hex, $prot_seq) = split(/\t/, $line);

    $prot_seq = uc($prot_seq);
    $prot_seq =~ s/\s+//g;
    $prot_seq =~ s/\W|\d/@/g;

    if (index($prot_seq, '@') != -1) {
        print STDERR qq([Warn] sequence contains invalid characters $md5hex - $prot_seq\n);
        next;
    }

    my $l = length($prot_seq);

    my %data = (
        _id => &digest($prot_seq),
        s => $prot_seq,
        l => $l,
        t => {},
        x => {},
        _s => $status
    );

    die qq(Digest does not equal expected aseq_id from md5 hex\n) if ($data{_id} ne &md5hex_toAseqID($md5hex));

    my $object = $collection->find_one({_id => $data{_id}}, {_id => 1, _s => 1});
    if ($object) {
        if ($object->{_s} ne $status) {
            my $newStatus = &mergeStatus($status, $object->{_s});
            $collection->update({_id => $data{_id}}, {'$set' => {'_s' => $newStatus}}, {safe => 1});
            ++$updated;
        }
    }
    else {
        ++$new;
        $collection->insert(\%data, {safe => 1});
    }

    ++$i;
    if ($i % 200 == 0) {
        print STDERR '.';
        if ($i % 10000 == 0) {
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i sequences [%.3f seconds, $new new, $updated updated <> %.1f / s]\n", $elapsed, 10000 / $elapsed;
            $t0 = [gettimeofday];

            $new = 0;
            $updated = 0;
        }
    }
}
close (IN);


sub digest {
    my $sequence = shift or die;

    my $base64 = md5_base64($sequence);
    $base64 =~ tr/+\//-_/;

    return $base64;
}

sub md5hex_toAseqID {
    my $md5hex = shift or die;
    my $x = encode_base64(pack('H*', $md5hex));
    chomp($x);
    $x =~ s/=+$//;
    $x =~ tr/+\//-_/;
    return $x;
}

sub mergeStatus {
    my $a = shift or die;
    my $b = shift or die;

    my $l = length($a);

    die qq(Status lengths differ!\n) if ($l != length($b));

    my $newStatus = '';
    for (my $i=0; $i < $l; ++$i) {
        if (substr($a, $i, 1) eq '1' ||
            substr($b, $i, 1) eq '1') {
            $newStatus .= '1';
        }
        else {
            $newStatus .= '-';
        }
    }

    return $newStatus;
}