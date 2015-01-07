#!/usr/bin/perl

use strict;
use warnings;
use Digest::MD5 'md5_base64';
use MIME::Base64;

while (<>) {
    my ($md5hex, $other) = split(/\t/, $_, 2);
    print &md5hex_toAseqID($md5hex), "\t", $other;
}

sub md5hex_toAseqID {
    my $md5hex = shift or die;
    my $x = encode_base64(pack('H*', $md5hex));
    chomp($x);
    $x =~ s/=+$//;
    $x =~ tr/+\//-_/;
    return $x;
}
