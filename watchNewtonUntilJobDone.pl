#!/usr/bin/perl

use strict;
use warnings;

my $usage = <<USAGE;
Usage: $0 <job name>

USAGE

my $g_JobName = shift or die $usage;
my $g_SleepTime = 120;

my $doneFile = "/lustre/home/genomics/ogun_data/seqdepot/jobs/$g_JobName-done";

my $cmd = "ssh newton 'ls $doneFile 2>&1' |";

my $done = 0;
while (!$done) {

    open (IN, $cmd) or die qq(Unable to execute ls on newton: $!\n);
    my $line = <IN>;
    chomp($line);
    $done = $line =~ /^$doneFile$/o;
    close (IN);
#    print($done);

    if (!$done) {
	
#	print("Hey");
#	print($done);
        print STDERR qq(Sleeping $g_SleepTime seconds...\n);
        sleep($g_SleepTime);
    }
}

print STDERR qq($g_JobName is complete.\n);
