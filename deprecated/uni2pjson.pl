#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use XML::Parser;
use File::Temp 'tempfile';

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 [<uniref xml file>]

USAGE

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

# ------------------------------------------------------------
# Begin parsing XML file
my $char_data;
my @elstack;
my $data = undef;

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


my $fh = &Common::openFileOrGzFile($g_UniRefXmlFile);
&Common::startTicker();
$parser->parse($fh);
$fh->close();

sub processEntry {
    my $entry = shift or die;

    my $l = length($entry->{s});
    return if ($l < $Common::g_MinLen);

    my $data = &Common::baseStructure($entry->{s});
    $data->{x}->{uni} = &unique($entry->{x}->{uni});
    &Common::printPJSON($data);
    &Common::tick();
}

sub unique {
    my $array = shift or die;

    my %hash = ();
    foreach my $val (@$array) {
        $hash{$val} = 1;
    }

    return [ keys %hash ];
}
