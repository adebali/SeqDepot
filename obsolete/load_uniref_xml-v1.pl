#!/usr/bin/perl
#
# Program: load_nr_fasta.pl
# Author: Luke Ulrich
# Date: 20 April 2010
# Description: Load UniRef 100 XML data into the SeqDepot, build cross-references to identifiers, and store the associated
#              descriptions.
#
# Ignores any sequence with any non-word character or digits.
#
# Only cross-references UniProtDB ids and accessions

$| = 1;

use strict;
use warnings;
use Digest::MD5 'md5_base64';
use IO::File;
use File::Temp 'tempfile';
use Time::HiRes qw(gettimeofday tv_interval);
use XML::Parser;

my $usage = <<USAGE;
Usage: $0 <UniRef100 xml file>

USAGE

my $g_MinLen = 30;

my $g_UniRefXmlFile = shift;
if ($g_UniRefXmlFile && !-e $g_UniRefXmlFile) {
    die qq(Invalid file '$g_UniRefXmlFile'\n);
}

if (!$g_UniRefXmlFile) {
    my $fh;
    my $url = 'ftp://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref100/uniref100.xml.gz';
    ($fh, $g_UniRefXmlFile) = tempfile('seqdepot_uniref_XXXXXX', SUFFIX => '.gz', DIR => '/tmp');
    $fh->close();
    system(qq(wget -O $g_UniRefXmlFile $url));
    die qq([$0] Unable to download $url\n) if ($? == -1);
}

use MongoDB;
$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $collection = $db->get_collection('aseqs');

# ------------------------------------------------------------
# Begin parsing XML file
my $char_data;
my @elstack;
my $data = undef;
my $t0;
my $new = 0;
my $i = 0;
my $updated = 0;

sub onStartTag {
    my ($expat, $element, %attr) = @_;

    push @elstack, $element;
    my $parent = (@elstack > 1) ? $elstack[-2] : '';

    $char_data = undef;

    # ------------------------------------------------------------
    # ------------------------------------------------------------
    # Entry details
    if ($element eq 'entry') {
        $attr{id} =~ s/^UniRef100_//;
    	$data = {
    	    x => {
                uni => [$attr{id}]
            }
    	};
    }
    elsif ($element eq 'dbReference') {
    	my $type = lc($attr{type});
    	$type =~ tr/ /_/;
    	if ($type eq 'uniprotkb_id') {
            push @{ $data->{x}->{uni} }, $attr{id};
    	}
    }
    elsif ($element eq 'property') {
    	my ($type, $value) = (lc($attr{type}), $attr{value});
    	$type =~ tr/ /_/;

    	if ($parent ne 'entry') {
    	    if ($type eq 'uniprotkb_accession') {
                push @{ $data->{x}->{uni} }, $attr{value};
    	    }
    	}
    }
}

sub onCharData {
    my ($expat, $data) = @_;

    $char_data .= $data;
}

sub onEndTag {
    my ($expat, $element) = @_;
    my $parent = (@elstack > 1) ? $elstack[-2] : '';

    if ($element eq 'sequence') {
    	$char_data =~ s/\s+//g;
    	$char_data =~ s/\W|\d/@/g;

	    $data->{s} = $char_data;
    }
    elsif ($element eq 'entry') {
        if (index($data->{s}, '@') != -1) {
            my @ids = join(', ', @{ $data->{x}->{uni} });
            print STDERR qq([Warn] sequence contains invalid characters @ids - $data->{s}\n);
        }
        else {
            &processEntry($data);
        }
    }

    pop @elstack;
}

my $parser = new XML::Parser();
$parser->setHandlers(
    Start => \&onStartTag,
	Char => \&onCharData,
	End => \&onEndTag);

$t0 = [gettimeofday];
my $file = ($g_UniRefXmlFile =~ /\.gz$/) ? qq(zcat $g_UniRefXmlFile |) : $g_UniRefXmlFile;
my $io = new IO::File();
$io->open($file) or die qq([$0] Unable to open file '$!'\n);
$parser->parse($io);
$io->close();

sub processEntry {
    my $data = shift or die;

    my $l = length($data->{s});
    return if ($l < $g_MinLen);

    $data->{_id} = &digest($data->{s});
    $data->{l} = $l;
    $data->{t} = {};
    $data->{x}->{uni} = &unique($data->{x}->{uni});

    my $object = $collection->find_one({_id => $data->{_id}}, {_id => 1, 'x.uni' => 1});
    if (!$object) {
        ++$new;
        $collection->insert($data, {safe => 1});
    }
    else {
        my $update = undef;
        my $newIds = [];
        if ($object->{x}->{uni}) {
            $newIds = &findNewIds($data->{x}->{uni}, $object->{x}->{uni});
        }

        if (@$newIds || !$object->{x}->{uni}) {
            # Always use $set instead of push/pull, etc.
            $update = {
                '$set' => {
                    'x.uni' => $data->{x}->{uni}
                }
            };
        }

        if ($update) {
            ++$updated;
            $collection->update({_id => $data->{_id}}, $update, {safe => 1});
        }
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

sub unique {
    my $array = shift or die;

    my %hash = ();
    foreach my $val (@$array) {
        $hash{$val} = 1;
    }

    return [ keys %hash ];
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
