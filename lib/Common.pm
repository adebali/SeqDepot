package Common;

use strict;
use warnings;
use Carp;
use JSON;

my $lastError;

sub readFaaLastId {
    my $fastaFile = shift or croak qq(Missing file name\n);

    if (!-e $fastaFile) {
	$lastError = qq(Fasta file does not exist, $fastaFile\n);
	return undef;
    }

    my $lastId;

    my $pid = open (IN, "tac $fastaFile |");
    if (!defined($pid)) {
	$lastError = qq(Unable to start pipe file: $!\n);
	return undef;
    }
    while (<IN>) {
	if (/^>(\S+)/) {
	    $lastId = $1;
	    last;
	}
    }
    close (IN);

    if (!defined($lastId)) {
	$lastError = qq(No id was found in fasta file, $fastaFile\n);
    }

    return $lastId;
}

sub lastError {
    return $lastError;
}

sub writeError {
    my $error = shift or croak qq(Missing error\n);
    my $file = shift or croak qq(Missing error file\n);

    open (OUT, ">> $file") or croak qq(Unable to write to error file, $file: $!\n);
    print OUT $error;
    close (OUT);
}

sub writeErrorAndDie {
    my $error = shift;
    my $file = shift;
    if ($file) {
	&writeError($error, $file);
    }
    die $error;
}

sub printJson {
    my $id = shift or croak qq(Missing id\n);
    my $obj = shift or croak qq(Invalid object to convert to JSON\n);

    print $id, "\t", to_json($obj), "\n";
}

sub getLastIdOrDieWithError {
    my $file = shift;
    my $errFile = shift;
    if ($file) {
	my $lastId = &readFaaLastId($file);
	if (!$lastId) {
	    &writeErrorAndDie($lastError, $errFile);
	}
	return $lastId;
    }
}

sub writeErrorIfInvalidLastId {
    my $expectedLastId = shift;
    my $lastId = shift;
    my $errFile = shift;
    if ($expectedLastId && (!$lastId || $expectedLastId ne $lastId)) {
	&writeErrorAndDie(qq(Last id parsed is not $expectedLastId\n), $errFile);
    }
}

sub readFastaIds {
    my $file = shift or die qq(Missing file argument\n);
    my $errFile = shift;

    my @ids = ();

    if (!open (IN, "< $file")) {
	&writeErrorAndDie("Unable to open file, $file: $!\n", $errFile);
    }
    while (<IN>) {
	next if (!/^>(\S+)/);

	push @ids, $1;
    }
    close (IN);

    return \@ids;
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

1;
