#!/usr/bin/perl
#
# Program: load_nr_fasta.pl
# Author: Luke Ulrich
# Date: 8 Apr 2010
# Description: Load NR fasta data into the SeqDepot, build cross-references to GI numbers, and store the associated
#              descriptions.
#
# Replaces any non-word character or digit from the sequence with an X

$| = 1;

use strict;
use warnings;
use File::Temp 'tempfile';
use LWP::Simple;
use Time::HiRes qw(gettimeofday tv_interval);

use lib '../lib';
use Aseq;
use Db;

my $nr_file = shift;
if ($nr_file && !-e $nr_file)
{
    die qq(Invalid file '$nr_file'\n);
}

my $db = new Db;
my $dbh = $db->getHandle();
if (!$dbh)
{
    die "[$0] " . $db->error(), "\n";
}

my $g_Aseq = new Aseq;

if (!$nr_file)
{
    my $g_NrUrl = 'ftp://ftp.ncbi.nih.gov/blast/db/FASTA/nr.gz';
    my $fh;
    ($fh, $nr_file) = tempfile('seqdepot_nr_XXXXXX', SUFFIX => '.gz', DIR => '/tmp');
    $fh->close();
    system(qq(wget -O $nr_file $g_NrUrl));
    die qq([$0] Unable to download $g_NrUrl\n) if ($? == -1);
}

my $i_gi = $dbh->prepare(q(INSERT INTO gis (gi, aseq_id) VALUES (?, ?)));
my $i_gi_data = $dbh->prepare(q(INSERT INTO gis_data (gi, organism, description) VALUES (?, ?, ?)));
my $s_gi = $dbh->prepare(q(SELECT gi FROM gis WHERE gi = ?));

my @new_gis = ();

my $gi_sql_ = "INSERT INTO gis (gi, aseq_id) VALUES ";
my $gi_data_sql_ = "INSERT INTO gis_data (gi, organism, description) VALUES ";

my $cmd = qq(zcat $nr_file |);
if ($nr_file !~ /\.gz$/)
{
    $cmd = $nr_file;
}
open (IN, "$cmd") or die qq([$0] Unable to open file '$nr_file': $!\n);
my $n_gis = 0;
my $i = 0;
my $t0 = [gettimeofday];
while (my $seq = &readFastaSequence(*IN))
{
    # Parse out the gi and descriptions
    my @gis = ();
    chomp($$seq[0]);
    my @deflines = split(/\cA/, $$seq[0]);
    foreach my $defline (@deflines)
    {
	if ($defline =~ m/gi\|(\d+)\|[^|]*\|[^|]*\|(?:\S+)? +(?:(.*)\[(.*?)\]|(.*))/)
	{
	    ++$n_gis;

	    $s_gi->execute($1);
	    next if ($s_gi->fetchrow_array());

	    push @gis, {
		gi => $1,
		organism => $3,
		description => defined($2) ? $2 : $4
	    };
	    $gis[-1]->{description} =~ s/\s+$//;
	}
    }

    if (@gis)
    {
	# Now fetch the aseq_id
	my $aseq_id = $g_Aseq->getAseqId($$seq[1], 1);
	if (!$aseq_id)
	{
	    die qq([$0] Unable to fetch aseq_id for $$seq[0] :: $$seq[1]\n);
	}

	map { $_->{aseq_id} = $aseq_id } @gis;
	
	push @new_gis, @gis;
    }

    &saveData(\@new_gis) if (@new_gis > 10000);

    ++$i;
    if ($i % 200 == 0)
    {
	print STDERR '.';
	if ($i % 10000 == 0)
	{
	    my $elapsed = tv_interval($t0, [gettimeofday]);
	    printf STDERR " $i sequences, $n_gis gis [%.3f seconds]\n", $elapsed;
	    $t0 = [gettimeofday];
	}
    }

    last if ($seq->[2]);
}
&saveData(\@new_gis);
print STDERR "\nRecords: ", $i, ", Deflines: ", $n_gis, "\n";

unlink($nr_file);


# ------------------------------------------------------------
# ------------------------------------------------------------
# Leaves out the greater than character of the header
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

sub saveData
{
    my $gis = shift or return;

    my $z = @$gis;
    return if (!$z);

    # Now insert all the gis
    $dbh->do(q(LOCK TABLES gis WRITE, gis_data WRITE));

    foreach my $gi (@$gis)
    {
	$i_gi->execute($gi->{gi}, $gi->{aseq_id});
	$i_gi_data->execute($gi->{gi}, $gi->{organism}, $gi->{description});
    }
    $dbh->do(q(UNLOCK TABLES));

    @$gis = ();
}
