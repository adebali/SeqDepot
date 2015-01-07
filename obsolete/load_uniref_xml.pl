#!/usr/bin/perl
#
# Program: load_nr_fasta.pl
# Author: Luke Ulrich
# Date: 20 April 2010
# Description: Load UniRef 100 XML data into the SeqDepot, build cross-references to identifiers, and store the associated
#              descriptions.
#
# Replaces any non-word character or digit from the sequence with an X

$| = 1;

use strict;
use warnings;
use IO::File;
use File::Temp 'tempfile';
use Time::HiRes qw(gettimeofday tv_interval);
use XML::Parser;

use lib '../lib';
use Aseq;
use Db;

my $usage = <<USAGE;
Usage: $0 <UniRef100 xml file>

USAGE

my $g_UniRefXmlFile = shift;
if ($g_UniRefXmlFile && !-e $g_UniRefXmlFile)
{
    die qq(Invalid file '$g_UniRefXmlFile'\n);
}

my $db = new Db;
my $dbh = $db->getHandle();
if (!$dbh)
{
    die "[$0] " . $db->error(), "\n";
}

my $g_Aseq = new Aseq;
$dbh->{AutoCommit} = 0;

if (!$g_UniRefXmlFile)
{
    my $fh;
    my $url = 'ftp://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref100/uniref100.xml.gz';
    ($fh, $g_UniRefXmlFile) = tempfile('seqdepot_uniref_XXXXXX', SUFFIX => '.gz', DIR => '/tmp');
    $fh->close();
    system(qq(wget -O $g_UniRefXmlFile $url));
    die qq([$0] Unable to download $url\n) if ($? == -1);

}

&create_new_tables($dbh);

my $i_uniref = $dbh->prepare(q(INSERT INTO uniref_new (entry_id, aseq_id) VALUES (?, ?)));
my $i_uniref_data = $dbh->prepare(q(INSERT INTO uniref_data_new (uniref_id, common_taxon_id, updated, common_taxon, name) VALUES (?, ?, ?, ?, ?)));
my $i_uniref_annotation = $dbh->prepare(q(INSERT INTO uniref_annotations_new (seed, ncbi_taxonomy_id, organism, protein_name) VALUES (?, ?, ?, ?)));
my $i_uniref_xref = $dbh->prepare(q(INSERT INTO uniref_xrefs_new (uniref_id, uniref_annotation_id, xref_id, xref_type) VALUES (?, ?, ?, ?)));

$dbh->commit();

# ------------------------------------------------------------
# Begin parsing XML file
my $char_data;
my @elstack;
my $entry = undef;
my @entries = ();
my $t0;
my $n = 0;

sub onStartTag
{
    my ($expat, $element, %attr) = @_;
    
    push @elstack, $element;
    my $parent = (@elstack > 1) ? $elstack[-2] : '';

    $char_data = undef;

    # ------------------------------------------------------------
    # ------------------------------------------------------------
    # Entry details
    if ($element eq 'entry')
    {
	$entry = {
	    entry_id => $attr{id},
	    updated => $attr{updated}
	};
    }
    elsif ($element eq 'dbReference')
    {
	my $type = lc($attr{type});
	$type =~ tr/ /_/;
	if ($type eq 'uniparc_id' ||
	    $type eq 'uniprotkb_id')
	{
	    push @{ $entry->{annotations} }, {
		seed => 0,
		xrefs => [ {
		    xref_id => $attr{id},
		    xref_type => $type
			   } ]
	    };
	}
    }
    elsif ($element eq 'property')
    {
	my ($type, $value) = (lc($attr{type}), $attr{value});
	$type =~ tr/ /_/;
	
	if ($parent eq 'entry')
	{
	    if ($type eq 'common_taxon' ||
		$type eq 'common_taxon_id')
	    {
		$entry->{$type} = $value;
	    }
	}
	else
	{
	    if ($type eq 'uniprotkb_accession' ||
		$type eq 'uniparc_id')
	    {
		# Assert!
		die if (!$entry->{annotations}->[-1]);

		push @{ $entry->{annotations}->[-1]->{xrefs} }, {
		    xref_id => $attr{value},
		    xref_type => $type
		};
	    }
	    elsif ($type eq 'ncbi_taxonomy')
	    {
		# Assert!
		die if (!$entry->{annotations}->[-1]);

		$entry->{annotations}->[-1]->{ncbi_taxonomy_id} = $value;
	    }
	    elsif ($type eq 'source_organism')
	    {
		# Assert!
		die if (!$entry->{annotations}->[-1]);

		$entry->{annotations}->[-1]->{organism} = $value;
	    }
	    elsif ($type eq 'protein_name')
	    {
		# Assert!
		die if (!$entry->{annotations}->[-1]);

		$entry->{annotations}->[-1]->{protein_name} = $value;
	    }
	    elsif ($type eq 'isseed')
	    {
		# Assert!
		die if (!$entry->{annotations}->[-1]);

		$entry->{annotations}->[-1]->{seed} = ($value eq 'true') ? 1 : 0;
	    }
	}
    }
    elsif ($element eq 'UniRef100')
    {
#	$self->{_version} = $attr{version};
    }
}

sub onCharData
{
    my ($expat, $data) = @_;

    $char_data .= $data;
}

sub onEndTag
{
    my ($expat, $element) = @_;
    my $parent = (@elstack > 1) ? $elstack[-2] : '';

    if ($element eq 'name' && $parent eq 'entry')
    {
	$entry->{name} = $char_data;
    }
    elsif ($element eq 'sequence')
    {
	$char_data =~ s/\s+//g;
	$char_data =~ s/\W|\d/X/g;

	$entry->{sequence} = $char_data;
    }
    elsif ($element eq 'entry')
    {
	push @entries, $entry;

	++$n;
	if (@entries == 1000)
	{
	    &processEntries($dbh, \@entries);
	}

	if ($n % 1000 == 0)
	{
	    print STDERR '.';
	    if ($n % 50000 == 0)
	    {
		my $elapsed = tv_interval($t0, [gettimeofday]);
		printf STDERR " $n sequences [%.3f seconds]\n", $elapsed;
		$t0 = [gettimeofday];
	    }
	}
    }
    elsif ($element eq 'UniRef100')
    {
	&processEntries($dbh, \@entries);

	my $elapsed = tv_interval($t0, [gettimeofday]);
	printf STDERR " $n sequences [%.3f seconds]\n", $elapsed;
    }
    
    pop @elstack;
}

my $parser = new XML::Parser();
$parser->setHandlers(Start => \&onStartTag,
		     Char => \&onCharData,
		     End => \&onEndTag);

$t0 = [gettimeofday];

my $file = qq(zcat $g_UniRefXmlFile |);
if ($g_UniRefXmlFile !~ /\.gz$/)
{
    $file = $g_UniRefXmlFile;
}
my $io = new IO::File();
$io->open($file) or die qq([$0] Unable to open file '$!'\n);
$parser->parse($io);
$io->close();

&rename_new_tables($dbh);
&drop_old_tables($dbh);


sub create_new_tables
{
    my $dbh = shift;

    $dbh->do(q(drop table if exists uniref_annotations_new, uniref_xrefs_new, uniref_data_new, uniref_new));
    my $sql = <<SQL;
create table uniref_new (
       id integer unsigned not null auto_increment comment 'Primary key; sequence generated integer',
       entry_id char(13) not null comment 'UniRef100 entry identifier',
       aseq_id integer unsigned not null comment 'Unenforced foreign key to aseqs(id)',

       primary key(id),
       unique(entry_id),
       index(aseq_id)
) engine=innodb
comment 'UniRef100 annotation and cross-references'
SQL
    $dbh->do($sql);
    

    $sql = <<SQL;
create table uniref_data_new (
       uniref_id integer unsigned not null comment 'Primary key; foreign key to uniref(id)',
       common_taxon_id integer unsigned comment 'Common NCBI taxonomy identifier',
       updated char(10) comment 'Date entry was updated',
       common_taxon varchar(128) comment 'Common organism/source',
       name varchar(255) comment 'Annotated name',

       primary key(uniref_id),
       foreign key(uniref_id) references uniref_new(id) on update cascade on delete cascade
) engine=innodb
comment '[Denormalized 1:1] basic uniref annotation data'
SQL
    $dbh->do($sql);

    $sql = <<SQL;
create table uniref_annotations_new (
       id integer unsigned not null auto_increment comment 'Primary key; sequence generated integer',
       seed boolean not null default false comment 'Is this entry the seed record',
       ncbi_taxonomy_id integer unsigned,
       organism varchar(128) comment 'Source organism for this protein',
       protein_name varchar(255) comment 'Description name',

       primary key(id)
) engine=innodb
comment '[Denormalized 1:1] UniRef annotation for a specific xref'
SQL
    $dbh->do($sql);

    $sql = <<SQL;
create table uniref_xrefs_new (
       uniref_id integer unsigned not null comment 'Foreign key to uniref(id)',
       uniref_annotation_id integer unsigned comment 'Foreign key to uniref_annotations(id)',
       xref_id char(13) not null comment 'The actual value of this field',
       xref_type enum('uniprotkb_id', 'uniprotkb_accession', 'uniparc_id'),

       index(uniref_id),
       foreign key(uniref_id) references uniref_new(id) on update cascade on delete cascade,
       index(xref_id)
) engine=innodb
comment 'List of all UniProt related identifiers'
SQL
    $dbh->do($sql);

    $dbh->commit();
}

sub rename_new_tables
{
    my $dbh = shift or return;

    my $sql = <<SQL;
rename table
    uniref_xrefs to uniref_xrefs_old,
    uniref_annotations to uniref_annotations_old,
    uniref_data to uniref_data_old,
    uniref to uniref_old,
    uniref_new to uniref,
    uniref_data_new to uniref_data,
    uniref_annotations_new to uniref_annotations,
    uniref_xrefs_new to uniref_xrefs;
SQL

    $dbh->do($sql);
    $dbh->commit();
}

sub drop_old_tables
{
    my $dbh = shift or return;

    $dbh->do(q(drop table uniref_xrefs_old, uniref_annotations_old, uniref_data_old, uniref_old));
    $dbh->commit();
}

sub processEntries
{
    my $dbh = shift or return;
    my $entries = shift or return;

    foreach my $entry (@$entries)
    {
	die if (!$entry->{sequence});

	$entry->{entry_id} =~ s/^UniRef100_//;
	if (length($entry->{entry_id}) > 13)
	{
	    print "Entry id too long: $entry->{entry_id}\n";
	    next;
	}

	my $aseq_id = $g_Aseq->getAseqId($entry->{sequence}, 1);
	if (!$aseq_id)
	{
	    print "Unable to fetch aseq_id for entry: $entry->{entry_id}\n";
	    next;
	}

	$i_uniref->execute($entry->{entry_id}, $aseq_id);
	my $uniref_id = $dbh->last_insert_id(undef, undef, 'uniref_new', 'id');

	if (length($entry->{updated}) != 10)
	{
	    print "Entry updated bad length: $entry->{entry_id} - $entry->{updated}\n";
	    next;
	}

	$entry->{common_taxon} = substr($entry->{common_taxon}, 0, 128) if ($entry->{common_taxon} && length($entry->{common_taxon}) > 128);
	$entry->{name} = substr($entry->{name}, 0, 255) if ($entry->{name} && length($entry->{name}) > 255);
	$i_uniref_data->execute($uniref_id, @{$entry}{qw(common_taxon_id updated common_taxon name)});

	foreach my $x (@{ $entry->{annotations} })
	{
	    $x->{organism} = substr($x->{organism}, 0, 128) if ($x->{organism} && length($x->{organism}) > 128);
	    $x->{protein_name} = substr($x->{protein_name}, 0, 128) if ($x->{protein_name} && length($x->{protein_name}) > 255);
	    $i_uniref_annotation->execute(@{$x}{qw(seed ncbi_taxonomy_id organism protein_name)});
	    my $annotation_id = $dbh->last_insert_id(undef, undef, 'uniref_annotations_new', 'id');

	    foreach my $xref (@{ $x->{xrefs} })
	    {
		if (length($xref->{xref_id}) > 13)
		{
		    print "Warning: entry ($entry->{entry_id}) has too long an xref_id: $xref->{xref_id}\n";
		    next;
		}

		$i_uniref_xref->execute($uniref_id, $annotation_id, $xref->{xref_id}, $xref->{xref_type});
	    }
	}
    }
    $dbh->commit();

    # Clear out the array
    @$entries = ();
}
