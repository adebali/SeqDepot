#!/usr/bin/perl
#
# Start and stop columns are included with every feature and automatically converted to
# integers. All data is sorted by the start value, then name (if included)

# SIMAP Columns:
# - md5 (128bit MD5 hash of the sequence in upper case letters)
# * CRC64 (CRC64 hash of the sequence in upper case letters)
# - length of the protein sequence
# - name of the database
# - name of the feature
# - description of the feature
# - begin (begin of the hit on the protein sequence)
# - end (end of the hit on the protein sequence)
# - e-Value (e-Value, if available)
# * true positive flag
# * date
# ? name of the InterPro assignment (if assigned)
# ? description of the InterPro assignment (if assigned)
#
# * Useless values. The true positive flag is always T or ?; don't care about either the date or CRC64


use strict;
use warnings;
use Getopt::Long;
use Data::Dumper;
use JSON;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use LWP::Simple;

use FindBin '$Bin';
use lib "$Bin/..";
use Common;

my $usage = <<USAGE;
Usage: $0 [options] <feature file>

  Options
  -------
   -t, --type=s          : If piping data to this script,
                           then it is required to set the
                           data type via this flag.
                           Precedence is given to the file
                           if it is passed in.

    Converts feature file into its JSON equivalent version,
    applying any relevant data massaging.

USAGE

my $toolType;
GetOptions("t|type=s", \$toolType);

my $g_File = shift;

die $usage if (!$g_File && !$toolType);

# -----------------------------------
# Columns as provided by the SIMAP data files
# my $md5Col = 0;
# my $nameCol = 4;
# my $descCol = 5;
# my $startCol = 6;
# my $stopCol = 7;
# my $evalueCol = 8;      # Or score

my $startCol = $Common::simapColumns{start};
my $stopCol = $Common::simapColumns{stop};
my $evalueCol = $Common::simapColumns{evalue};
my $sortColumn = $startCol;

if ($g_File) {
    die qq(Invalid feature file\n) if ($g_File !~ /features_(\w+)\.gz$/);
    $toolType = $1;
}
my $config = \%Common::simapConfig;
die qq(No configuration defined for this feature set\n) if (!exists $config->{$toolType});
my $toolConfig = $config->{$toolType};
my @columns = @{$toolConfig->{columns}};
$sortColumn = $toolConfig->{sortColumn} if (defined($toolConfig->{sortColumn}));
$sortColumn = &mapSortColumnToUserColumn($sortColumn, \@columns);

# --------------------------------------------------------------------------------------------------------------------
# Special tweak for smart
&tweakGene3D() if ($toolType eq 'Gene3D');
&tweakSmart() if ($toolType eq 'HMMSmart');

# --------------------------------------------------------------------------------------------------------------------
# Main data parsing
my @rows = ();
my $lastMd5 = '';

my $fh = ($g_File) ? &Common::openFileOrGzFile($g_File) : IO::Handle->new_from_fd(fileno(STDIN), 'r');

&Common::startTicker();
while (my $line = <$fh>) {
    chomp($line);
    my @values = split(/\t/, $line);
    if ($toolConfig->{preprocess}) {
        &{$toolConfig->{preprocess}}(\@values);
    }

    $values[$startCol] = int($values[$startCol]);
    $values[$stopCol] = int($values[$stopCol]);
    $values[$evalueCol] += 0;

    my $md5hex = $values[$Common::simapColumns{md5}];
    my @row = @values[@columns];

    if (length($lastMd5) > 0 && $md5hex ne $lastMd5) {
        &dumpData($lastMd5, \@rows);
        @rows = ();
    }

    push @rows, \@row;
    $lastMd5 = $md5hex;
}

if (length($lastMd5) > 0 && @rows) {
    &dumpData($lastMd5, \@rows);
}
print STDERR "\n\n";

sub dumpData {
    my $md5hex = shift;
    my $rows = shift;

    if (defined($sortColumn)) {
        @$rows = sort { $a->[$sortColumn] <=> $b->[$sortColumn] } @$rows;
    }

    my $aseqId = &Common::md5hex_toAseqID($md5hex);
    my $data = (defined($toolConfig->{reshape})) ? &{$toolConfig->{reshape}}($rows) : $rows;
    $data = {
        t => {
            $toolConfig->{field} => $data
        }
    };
    print $aseqId, "\t", to_json($data), "\n";
    &Common::tick();
}

sub mapSortColumnToUserColumn {
    my $sortColumn = shift;
    die qq(Sort column may not be the first column!) if ($sortColumn == 0);
    my $userColumns = shift;

    my $i = 0;
    foreach my $column (@$userColumns) {
        if ($column == $sortColumn) {
            return $i;
        }
        ++$i;
    }

    die qq(Sort column is not one of the specified command line columns\n);
}

sub tweakSmart {
    my $smartDefinitionUrl = 'http://smart.embl-heidelberg.de/smart/descriptions.pl';

    print STDERR qq(** Downloading SMART definitions...);
    my $smartDefinitions = get($smartDefinitionUrl);
    die qq(ERROR!!!\n) if (!$smartDefinitions);
    print STDERR qq(done\n);

    # my $smartDefinitions = '';
    # open (X, "< smartdefs") or die $!;
    # local $/ = undef;
    # $smartDefinitions = <X>;
    # close (X);
    # $/ = "\n";

    my %smart = ();
    while ($smartDefinitions =~ /\n(\S+)\s+(SM\d{5})/g) {
        $smart{$2} = $1;
    }

    my $descCol = $Common::simapColumns{desc};

    $config->{HMMSmart}->{preprocess} = sub {
        my $data = shift or die;

        my $accession = $data->[$Common::simapColumns{name}];

        if (!$smart{$accession}) {
            if ($accession eq 'SM00611') {
                $data->[$descCol] = 'SEC63';
            }
            else {
                warn qq(Missing smart accession for: $data->[$Common::simapColumns{name}]\n) ;
                $data->[$descCol] = '';
            }
        }
        else {
            $data->[$descCol] = $smart{$accession};
        }
    };
}

sub tweakGene3D {
    my $cathDefinitionsUrl = 'ftp://ftp.biochem.ucl.ac.uk/pub/gene3d_data/CURRENT_RELEASE/model_to_family_map.csv.gz';

    print STDERR qq(** Downloading CATH definitions...);
    my $cathDefinitionsGz = get($cathDefinitionsUrl);
    die qq(ERROR!!!\n) if (!$cathDefinitionsGz);
    print STDERR qq(done\n);

    # my $cathDefinitionsGz = '';
    # open (X, "< model_to_family_map.csv.gz") or die $!;
    # local $/ = undef;
    # $cathDefinitionsGz = <X>;
    # close (X);
    # $/ = "\n";

    my $gunzipper = new IO::Uncompress::Gunzip(\$cathDefinitionsGz) or die qq([$0] Unable to create gunzip object\n);

    my %cath = ();
    while (my $line = <$gunzipper>) {
        next if ($line !~ /^.*?,(\d+\.\d+\.\d+\.\d+),(.*)/);

        my $id = $1;
        my $desc = $2;
        $desc =~ s/\s+$//;
        $desc = '' if ($desc eq 'null');
        $cath{$id} = $desc;
    }

    my $nameCol = $Common::simapColumns{name};

    $config->{Gene3D}->{preprocess} = sub {
        my $data = shift or die;

        $data->[$nameCol] =~ s/^G3DSA://;

        if (!exists $cath{$data->[$nameCol]}) {
            $data->[$nameCol] = '';
        }
        else {
            $data->[$Common::simapColumns{desc}] = $cath{$data->[$nameCol]};
        }
    };
}
