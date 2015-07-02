#!/usr/bin/perl

use strict;
use warnings;

use FindBin '$Bin';
use lib $Bin;
use Common;
use File::Temp 'tempfile';

my $usage = <<USAGE;
Usage: $0 [<nr file>]

USAGE

my $nr_file = shift;
if ($nr_file && !-e $nr_file) {
    die qq(Invalid file '$nr_file'\n);
}

#if (!$nr_file) {
#    my $g_NrUrl = 'ftp://ftp.ncbi.nih.gov/blast/db/FASTA/nr.gz';
#    my $fh;
#    ($fh, $nr_file) = tempfile('seqdepot_nr_XXXXXX', SUFFIX => '.gz', DIR => '/tmp');
#    $fh->close();
#    system(qq(wget -O $nr_file $g_NrUrl));
#    die qq([$0] Unable to download $g_NrUrl\n) if ($? == -1);
#}

my $fh = &Common::openFileOrGzFile($nr_file);

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
    #$data->{x}->{ENS} = &parseGis($$seq[0]);
    if($data->{x}->{ENS})
    	{$data->{x}->{ENS}.push(&parseGis($$seq[0]));
	}
    else{
	$data->{x}->{ENS} = &parseGis($$seq[0]);
	}
    &Common::printPJSON($data);
    &Common::tick();
}

sub parseGis {
    my $header = shift;

    my @gis = ();
    chomp($header);
    my @def = split(/ /, $header);
    push @gis, $def[0];
    return \@gis;
}
