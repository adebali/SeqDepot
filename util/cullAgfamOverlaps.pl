#!/usr/bin/perl

$| = 1;

use strict;
use warnings;
use boolean;
use Getopt::Long;
use Data::Dumper;
use List::Util 'max', 'min';

use FindBin '$Bin';
use lib "$Bin/..";
use Common;

my $usage = <<USAGE;
Usage: $0 [options]

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

my $aseqs = &Common::aseqs();
&Common::startTicker(1000);
my $cursor = $aseqs->find({'t.agfam1' => {'$exists' => true}});
$cursor->fields({'t.agfam1' => true});
while (my $aseq = $cursor->next()) {
    my $nAgfams = @{ $aseq->{t}->{agfam1} };
    next if ($nAgfams <= $g_Limit);

    my $finalAgfams = &limitOverlapsToTop($aseq->{t}->{agfam1});
    my $changes = {'$set' => {'t.agfam1' => $finalAgfams}};

    $aseqs->update({_id => $aseq->{_id}}, $changes);
    &Common::tick();
}

sub limitOverlapsToTop {
    my $agfams = shift or die;

    # Sort by evalue
    $agfams = [ sort { $a->[-1] <=> $b->[-1] } @$agfams ];

    my @bins = ();
    for (my $i=0; $i< @$agfams; ++$i) {
        my $agfam = $agfams->[$i];

        # Does it overlap a bin?
        my $bin = &findOverlappingBin($agfam, \@bins);
        if ($bin) {
            if (@$bin < $g_Limit) {
                push @$bin, $agfam;
            }
        }
        else {
            push @bins, [ $agfam ];
        }
    }

    # Flatten the array
    my @newAgfams;
    map { push @newAgfams, @{$_} } @bins;
    return \@newAgfams;
}

sub findOverlappingBin {
    my $agfam = shift or die;
    my $bins = shift or die;

    my $minOverlap = .5;

    foreach my $bin (@$bins) {
        my ($muA, $muB) = &findAverageEndPoints($bin);
        my $muL = $muB - $muA + 1;

        my $c = $agfam->[1];
        my $d = $agfam->[2];

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