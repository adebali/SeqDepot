package Common;

use strict;
use warnings;
use Carp 'confess';
use Digest::MD5 'md5_base64';
use IO::Pipe;
use JSON;
use MIME::Base64;
use MongoDB;
use Time::HiRes qw(gettimeofday tv_interval);

our $client;
our $db;

our $g_MinLen = 30;

## Add a tool
our @tools = qw(agfam1 coils das ecf gene3d hamap panther patscan pfam26 pir prints proscan segs signalp smart superfam targetp tigrfam tmhmm pfam27 tigrfam14 pfam28 tigrfam15);
our %toolStatusPos = ();
my $i = 0;
foreach my $fieldName (@tools) {
    $toolStatusPos{$fieldName} = $i;
    ++$i;
}

our %padding = (
    simap => {
        amount => 512
    },
    nr => {
        amount => 1024
    },
    basic => {
        amount => 256
    }
);
foreach my $key (keys %padding) {
    $padding{$key}->{buffer} = '-'x($padding{$key}->{amount});
}
our $paddingKey = '__padding__';

our %simapColumns = (
    md5 => 0,
    name => 4,
    desc => 5,
    start => 6,
    stop => 7,
    evalue => 8
);

our %simapConfig = (
    Coil => {
        field => 'coils',
        columns => [@simapColumns{qw(start stop)}],
        preprocess => sub {
            my $data = shift or die;
            --$data->[$simapColumns{stop}];
        }
    },

    Gene3D => {
        field => 'gene3d',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
        #                                 ^^^^ Dummy column that will get replaced with the Gene3D description later
    },

    HAMAP => {
        field => 'hamap',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
    },

    HMMPanther => {
        field => 'panther',
        columns => [@simapColumns{qw(name start stop evalue)}]
    },

    PatternScan => {
        field => 'patscan',
        columns => [@simapColumns{qw(name desc start stop)}]
    },

    HMMPfam => {
        field => 'pfam',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
    },

    HMMPIR => {
        field => 'pir',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
    },

    FPrintScan => {
        field => 'prints',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
    },

    ProfileScan => {
        field => 'proscan',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
    },

    Seg => {
        field => 'segs',
        columns => [@simapColumns{qw(start stop)}],
    },

    SignalP => {
        field => 'signalp',
        columns => [@simapColumns{qw(desc stop)}],
        #                            ^^^^ = gram- / gram+ / euk
        preprocess => sub {
            my $data = shift or die;
            my $descCol = $simapColumns{desc};
            if ($data->[$descCol] eq 'gram-') {
                $data->[$descCol] = 'gn';
            }
            elsif ($data->[$descCol] eq 'gram+') {
                $data->[$descCol] = 'gp';
            }
            else {
                $data->[$descCol] = 'e';
            }
        },
        reshape => sub {
            my $rows = shift or die;
            my %obj = ();
            foreach my $row (@$rows) {
                my $type = $row->[0];
                my $stop = $row->[1];
                $obj{$type} = $stop;
            }

            return \%obj;
        },
        sortColumn => $simapColumns{stop}
    },

    HMMSmart => {
        field => 'smart',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
        #                                 ^^^^ Dummy column that will get replaced with the smart domain name
    },

    superfamily => {
        field => 'superfam',
        columns => [@simapColumns{qw(name desc start stop evalue)}]
    },

    TargetP => {
        field => 'targetp',
        columns => [@simapColumns{qw(desc stop)}],
        preprocess => sub {
            my $data = shift or die;
            my $descCol = $simapColumns{desc};
            $data->[$descCol] = ($data->[$descCol] eq 'Plant') ? 'p' : 'np';
            #                                                           ^^ = Non-plant
        },
        reshape => sub {
            my $rows = shift or die;
            my %obj = ();
            foreach my $row (@$rows) {
                my $type = $row->[0];
                my $stop = $row->[1];
                $obj{$type} = $stop;
            }

            return \%obj;
        },
        sortColumn => $simapColumns{stop}
    },
    HMMTigr => {
        field => 'tigrfam',
        columns => [@simapColumns{qw(name desc start stop evalue)}],
        preprocess => sub {
            my $data = shift or die;
            my $descCol = $simapColumns{desc};
            my $colonPos = index($data->[$descCol], ':');
            die qq(Missing colon in description: $data->[$descCol]\n) if ($colonPos == -1);
            $data->[$descCol] = substr($data->[$descCol], 0, $colonPos);
        }
    },
    TMHMM => {
        field => 'tmhmm',
        columns => [@simapColumns{qw(start stop)}],
    }
);


sub client {
    if (!$client) {
	$client = new MongoDB::MongoClient(host => 'localhost', port => 27017, query_timeout => 9999999);
	#For god:
	#$client = new MongoDB::MongoClient(host => 'localhost', port => 27020, query_timeout => 9999999);
    }
    return $client;
}

sub database {
    if (!$db) {
	$db = &client()->get_database('seqdepot');
    }
    return $db;
}

sub collection {
    my $name = shift or die;
    return &database()->get_collection($name);
}

sub aseqs{
    #return &database()->get_collection('aseqs');
    my $collectionName = 'aseqs';
    if ($_[0]){
    	$collectionName = $_[0];
	}
    return &database()->get_collection($collectionName);
}

sub isValidTool {
    my $alias = shift or return 0;

    return exists $toolStatusPos{$alias};
}

sub toolStatusPos {
    my $alias = shift or return -1;

    return $toolStatusPos{$alias};
}

sub mergeStatuses {
    my $first = shift or die;
    my $second = shift or die;

    my $first_l = length($first);
    my $second_l = length($second);

    confess if ($first_l != $second_l);

    my $result = '';
    for (my $i=0; $i< $first_l; ++$i) {
        my $a = substr($first, $i, 1);
        my $b = substr($second, $i, 1);

        if ($a eq 'T' || $b eq 'T') {
            $result .= 'T';
        }
        elsif ($a eq 'd' || $b eq 'd') {
            $result .= 'd';
        }
        elsif ($a eq '-') {
            $result .= $b;
        }
        else {
            $result .= $a;
        }
    }

    return $result;
}

sub emptyStatus {
    return '-'x(scalar(@tools));
}

sub baseStructure {
    my $sequence = shift or confess('Invalid sequence');

    return {
        _id => &aseqIdFromSequence($sequence),
        s => $sequence,
        l => length($sequence),
        x => {},
        t => {},
	m => 0,
        _s => &emptyStatus()
    }
}

sub printPJSON {
    my $data = shift or die;
    print $data->{_id}, "\t", to_json($data), "\n";
}

sub aseqIdFromSequence {
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

our $__buffer;
sub readFastaSequence {
    my $fh = shift or die;

    local $/ = "\n>";
    my $buffer = <$fh>;

    if ($__buffer) {
        my $x = index($__buffer, "\n");
        my $header = substr($__buffer, 0, $x);
        my $sequence;
        if ($buffer) {
            $sequence = substr($__buffer, $x + 1, -2);
        }
        else {
            $sequence = substr($__buffer, $x + 1);
        }

        $sequence =~ tr/\n\r\f\t //d;
        $sequence =~ s/\W|\d/@/g;

        $__buffer = $buffer;
        return [$header, $sequence];
    }
    elsif ($buffer) {
        my $x = index($buffer, "\n");
        my $header = substr($buffer, 1, $x - 1);
        my $sequence = substr($buffer, $x + 1);
        $__buffer = <$fh>;
        if ($__buffer) {
            chop($sequence);
            chop($sequence);
        }
        $sequence =~ tr/\n\r\f\t //d;
        $sequence =~ s/\W|\d/@/g;
        return [$header, $sequence];
    }
}

our %ticker = (
    i => 0,
    t0 => 0,
    new => 0,
    updated => 0,

    tickCount => 200,
    ticksPerLine => 50,

    callback => undef
);

sub startTicker {
    my $tickCount = shift;
    $ticker{tickCount} = $tickCount if ($tickCount && $tickCount > 0);

    $ticker{i} = 0;
    $ticker{t0} = [gettimeofday];
    $ticker{tickCountPerLine} = $ticker{tickCount} * $ticker{ticksPerLine};
}

sub tick {
    my $i = ++$ticker{i};
    if ($i % $ticker{tickCount} == 0) {
        print STDERR '.';
        if ($i % $ticker{tickCountPerLine} == 0) {
            my $elapsed = tv_interval($ticker{t0}, [gettimeofday]);
            if ($ticker{new} || $ticker{updated}) {
                printf STDERR " $i sequences [%.3f seconds $ticker{new} new, $ticker{updated} updated <> %.1f / s]\n", $elapsed, $ticker{tickCountPerLine} / $elapsed;
            }
            else {
                printf STDERR " $i sequences [%.3f seconds <> %.1f / s]\n", $elapsed, $ticker{tickCountPerLine} / $elapsed;
            }
            $ticker{t0} = [gettimeofday];
            $ticker{new} = 0;
            $ticker{updated} = 0;

            $ticker{callback}() if ($ticker{callback});
        }
    }
}

sub removePadding {
    my $collection = shift or confess;
    my $aseqId = shift;

    $collection->update({_id => $aseqId}, {'$unset' => {$paddingKey => 1}});
}

sub openFileOrGzFile {
    my $file = shift or confess;

    my $handle = undef;
    if ($file !~ /\.gz$/) {
        $handle = new IO::File($file, 'r');
        confess qq([$0] Unable to open file '$file': $!\n) if (!$handle);
    }
    else {
        $handle = new IO::Pipe();
        $handle->reader('zcat', $file);
        confess qq([$0] Unable to zcat file '$file': $!\n) if (!$handle);
    }

    return $handle;
}

sub unique {
    my $array = shift or confess;

    my %hash = ();
    foreach my $val (@$array) {
        $hash{$val} = 1;
    }

    return [ keys %hash ];
}

1;
