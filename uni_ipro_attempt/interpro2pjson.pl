#!/usr/bin/perl

use strict;
use warnings;
use JSON;

use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 <match complete xml>

USAGE

my $g_File = shift or die $usage;

