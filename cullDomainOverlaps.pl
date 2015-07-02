#!/usr/bin/perl

$| = 1;

use strict;
use warnings;
use Getopt::Long;
use Data::Dumper;
use List::Util 'max', 'min';
use JSON;

use FindBin '$Bin';
use lib "$Bin/lib";
use Common;

my $usage = <<USAGE;
Usage: $0 [options] <h2 pseudo json>

    Requires output from h22_pseudo_json.pl with each line
    consisting of an identifier [TAB] json

    The Json should be an array of domain hits structued as follows:

      0          1      2     3          4         5        6          7     8
    [ domain_id, start, stop, ali_extent hmm_start hmm_stop hmm_extent score evalue ];

    No subname should be present.

  Options
  -------

    -h, --help
    -l, --limit=integer     : Limit overlapping agfam hits to this amount;
                              defaults to 3.

USAGE

my $g_Help;
my $g_Limit;

GetOptions("h|help", \$g_Help,
           "l|limit=i", \$g_Limit);

die $usage if ($g_Help);
$g_Limit = 3 if (!$g_Limit);
die qq(Limit must be a positive integer\n) if ($g_Limit < 1);

my $file = shift or die $usage;

open (IN, "< $file") or die $!;
while (<IN>) {
    chomp;
    my ($id, $json) = split("\t", $_, 2);
    my $hits = from_json($json);

    my $finalHits = (@$hits <= $g_Limit) ? $hits : &limitOverlapsToTop($hits);
    &Common::printJson($id, $finalHits);
}

sub limitOverlapsToTop {
    my $hits = shift or die;

    # Sort by evalue
    $hits = [ sort { $a->[-1] <=> $b->[-1] } @$hits ];

    my @bins = ();
    for (my $i=0; $i< @$hits; ++$i) {
        my $hit = $hits->[$i];

        # Does it overlap a bin?
        my $bin = &findOverlappingBin($hit, \@bins);
        if ($bin) {
            if (@$bin < $g_Limit) {
                push @$bin, $hit;
            }
        }
        else {
            push @bins, [ $hit ];
        }
    }

    # Flatten the array
    my @newHits;
    map { push @newHits, @{$_} } @bins;
    return \@newHits;
}

sub findOverlappingBin {
    my $hit = shift or die;
    my $bins = shift or die;

    my $minOverlap = .5;

    foreach my $bin (@$bins) {
        my ($muA, $muB) = &findAverageEndPoints($bin);
        my $muL = $muB - $muA + 1;

        my $c = $hit->[1];
        my $d = $hit->[2];

        # Coverage of muA .. muB by c .. d
        my $coverage = 0;
        if ($c <= $muA && $d >= $muB) {
            $coverage = 1;
        }
        elsif ($c >= $muA && $d <= $muB) {
            $coverage = 1;
        }
        elsif ($c >= $muA && $c <= $muB) {
            my $rightEnd = &min($d, $muB);
            $coverage = ($rightEnd - $c + 1) / $muL;
        }
        elsif ($d >= $muA && $d <= $muB) {
            my $leftEnd = &max($c, $muA);
            $coverage = ($d - $leftEnd + 1) / $muL;
        }

        return $bin if ($coverage > $minOverlap);
    }

    return undef;
}

sub findAverageEndPoints {
    my $bin = shift or die;

    my $aSum = 0;
    my $bSum = 0;
    my $n = @$bin;

    foreach my $agfam (@$bin) {
        $aSum += $agfam->[1];   # 1 = start column
        $bSum += $agfam->[2];   # 2 = stop column
    }

    my $aAvg = $aSum / $n;
    my $bAvg = $bSum / $n;

    return ($aAvg, $bAvg);
}