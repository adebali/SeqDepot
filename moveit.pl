#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

use Data::Dumper;

my $dir = shift or die "Usage: $0 <directory>\n";

opendir(my $dh, $dir) or die qq(Unable to read directory, $dir: $!\n);
my @subdirs = grep { !/^\./ && -d "$dir/$_" && $_ =~ /^\d+$/ } readdir($dh);
closedir($dh);

foreach my $batch (@subdirs) {
    print "Processing batch: $batch\n";

    my $nextDir = $dir . '/' . $batch . '/a';

    opendir($dh, $nextDir) or die qq(Unable to read directory, $nextDir: $!\n);
    my @jobDirs = grep { !/^\./ && -d "$nextDir/$_" } readdir($dh);
    closedir($dh);

    print Dumper(\@jobDirs);
}
