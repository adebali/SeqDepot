#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

my $usage = <<USAGE;
Usage: $0 <n>

  Outputs the summed natural log factorial in c format for
  use with the program seg. Should output to lnfac.h in the
  source code directory for seg and recompile.

USAGE

my $n = shift or die;

print qq(double lnfac[] = {\n);
my $x = 0;
print "  0.000000, ";
for (my $i=1; $i< $n-1; $i++) {
    $x += log($i);

    printf("%.6f,", $x);
    if (($i+1) % 8 == 0) {
        print "\n  ";
    }
    else {
        print " ";
    }
}
$x += log($n-1);
printf("%.6f", $x);
print "\n};\n";
