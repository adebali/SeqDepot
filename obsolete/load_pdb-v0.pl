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

use File::Temp 'tempfile';
use LWP::UserAgent;

use Time::HiRes qw(gettimeofday tv_interval);

use lib '../lib';
use Aseq;
use Db;

my $db = new Db;
my $dbh = $db->getHandle();
if (!$dbh)
{
    die "[$0] " . $db->error(), "\n";
}

my $g_Aseq = new Aseq;

my $g_SeqresDbUrl = 'ftp://ftp.wwpdb.org/pub/pdb/derived_data/pdb_seqres.txt.gz';
my $g_EntryTypeUrl = 'ftp://ftp.wwpdb.org/pub/pdb/derived_data/pdb_entry_type.txt';

# ------------------------------------------------------------
# Download the PDB database
my $seqres_file;
my $entry_file;
my $fh;

($fh, $seqres_file) = tempfile('seqdepot_pdb_seqres_XXXXXX', SUFFIX => '.gz', DIR => '/tmp');
$fh->close();
system(qq(wget -O $seqres_file $g_SeqresDbUrl));
die qq([$0] Unable to download $g_SeqresDbUrl\n) if ($? == -1);

($fh, $entry_file) = tempfile('seqdepot_pdb_entry_XXXXXX', DIR => '/tmp');
$fh->close();
system(qq(wget -O $entry_file $g_EntryTypeUrl));
die qq([$0] Unable to download $g_EntryTypeUrl\n) if ($? == -1);

# Decompress the seqres file
system(qq(gunzip $seqres_file));
$seqres_file =~ s/\.gz$//;

# ------------------------------------------------------------
# Main program
my $i_pdb = $dbh->prepare(q(INSERT INTO pdb_new (aseq_id, pdb_id, chain, method, description) VALUES (?, ?, ?, ?, ?)));

&create_new_tables($dbh);

# ------------
# Parse step A
my %pdb_id2method = ();
open (IN, "< $entry_file") or die qq([$0] Unable to open entry file '$entry_file': $!\n);
while (<IN>)
{
    chomp;
    my ($pdb_id, $dummy, $method) = split(/\s+/, $_);

    $pdb_id2method{$pdb_id} = $method;
}
close (IN);

# ------------
# Parse step B :: read the fasta seqres file and populate the pdb table
my $i = 0;
my @pdbs = ();
my $t0 = [gettimeofday];
open (IN, "< $seqres_file") or die qq([$0] Unable to open seqres file '$seqres_file': $!\n);
while (my $seq = &readFastaSequence(*IN))
{
    # Only consider protein sequences
    next if ($seq->[0] !~ /mol:protein/);

    if ($seq->[0] !~ /^(\w+)_(\S+)/)
    {
	print STDERR "Unable to parse id and chain from header: $seq->[0]\n";
	next;
    }
    my ($pdb_id, $chain) = ($1, $2);

    my $description = '';
    $description = $1 if ($seq->[0] =~ /.*  (.*\S)/);
    $description =~ s/^\s+//g;

    my $method = $pdb_id2method{$pdb_id};
    if (!$method)
    {
	print STDERR "Unable to find method for PDB_ID: $pdb_id\n";
	next;
    }

    # Only thing remaining is the sequence
    my $aseq_id = $g_Aseq->getAseqId($$seq[1], 1);
    if (!$aseq_id)
    {
	print STDERR "Unable to create aseq_id for PDB_ID: $pdb_id\n";
	next;
    }

    push @pdbs, {
	aseq_id => $aseq_id,
	pdb_id => $pdb_id,
	chain => $chain,
	method => $method,
	description => lc($description)
    };

    ++$i;
    if ($i % 200 == 0)
    {
	print STDERR '.';
	if ($i % 10000 == 0)
	{
            my $elapsed = tv_interval($t0, [gettimeofday]);
            printf STDERR " $i sequences [%.3f seconds]\n", $elapsed;
            $t0 = [gettimeofday];
	}
    }

    &saveData($dbh, \@pdbs) if (@pdbs > 10000);

    last if ($seq->[2]);
}
close (IN);

&saveData($dbh, \@pdbs) if (@pdbs);
print STDERR "\nRecords: $i protein PDB sequences\n";

&rename_new_tables($dbh);
&drop_old_tables($dbh);

unlink($seqres_file);
unlink($entry_file);


sub saveData
{
    my $dbh = shift or return;
    my $pdbs = shift or return;

    foreach my $pdb (@$pdbs)
    {
	$i_pdb->execute(@{ $pdb }{qw(aseq_id pdb_id chain method description)});
    }

    @$pdbs = ();
}

sub downloadFile
{
    my $url = shift or return;
    my $saveFile = shift or return;

    print STDERR qq(Saving $url -> $saveFile...);

    my $lwp = new LWP::UserAgent(timeout => 30);
    die if (!$lwp);

    my $tries = 0;
    my $delay = 15;   # seconds

    do
    {
	my $response = $lwp->get($url, ':content_file' => $saveFile);
	if ($response->is_success())
	{
	    print STDERR qq(done\n);
	    return 1;
	}

	print STDERR qq(\nAn error occurred while downloading $url -> $saveFile. Trying again in $delay seconds\n);
	sleep($delay);
	++$tries;
    } while ($tries < 3);

    return 0;
}

sub create_new_tables
{
    my $dbh = shift or return;

    $dbh->do(q(drop table if exists pdb_new));
    my $sql = <<SQL;
create table pdb_new (
       aseq_id integer not null comment 'Unenforced foreign key to aseqs_id_map(id)',
       pdb_id char(4) not null comment 'RCSB sequence identifier',
       chain char(2) not null,
       method char(16) comment 'Method for determining the protein structure',
       description varchar(255),

       index(aseq_id),
       index(pdb_id)
) engine=myisam
comment 'Protein data bank (structures)'
SQL
    $dbh->do($sql);
}

sub rename_new_tables
{
    my $dbh = shift or return;
    
    my $sql = <<SQL;
rename table
    pdb to pdb_old,
    pdb_new to pdb;
SQL

    $dbh->do($sql);
}

sub drop_old_tables
{
    my $dbh = shift or return;

    $dbh->do(q(DROP TABLE if exists pdb_old));
}

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
