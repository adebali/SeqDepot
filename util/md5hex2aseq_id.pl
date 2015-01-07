#!/usr/bin/perl

use strict;
use warnings;

use MIME::Base64;

while (<>) {
    chomp;

    print $_, "\t", &md5hex_toAseqID($_), "\n";

}

sub md5hex_toAseqID {
    my $md5hex = shift or die;
    my $x = encode_base64(pack('H*', $md5hex));
    chomp($x);
    $x =~ s/=+$//;
    $x =~ tr/+\//-_/;
    return $x;
}