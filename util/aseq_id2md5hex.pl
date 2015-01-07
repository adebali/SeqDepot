#!/usr/bin/perl

use strict;
use warnings;

use MIME::Base64;

while (<>) {
    chomp;

    print $_, "\t", &md5hex_toAseqID($_), "\n";

}

sub md5hex_toAseqID {
    my $aseq_id = shift or die;
    $aseq_id =~ tr/-_/+\//;
    my $x = decode_base64($aseq_id);
    return unpack('H*', $x);
}