#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use FindBin '$Bin';
use lib $Bin;
use Common;

my $usage = <<USAGE;
Usage: $0 [options] <id map file>

    Options
    -------
     -h, --help   : Display this help page
     -q, --quiet  : Do not output tick results

    Note only extracts the GI, PDB, and UniProt ids and associates with the UPI id

    Id columns:
        1. UniProtKB-AC :: special; may be referenced using uni
        2. UniProtKB-ID :: special; may be referenced using uni
        3. GeneID (EntrezGene)
        4. RefSeq
        5. GI
        6. PDB
        7. GO
        8. IPI
        9. UniRef100
        10. UniRef90
        11. UniRef50
        12. UniParc
        13. PIR
        14. NCBI-taxon
        15. MIM
        16. UniGene
        17. PubMed
        18. EMBL
        19. EMBL-CDS
        20. Ensembl
        21. Ensembl_TRS
        22. Ensembl_PRO
        23. Additional PubMed

    Does not produce a unique set of ids per UPI. That should be performed by a post-processing step.

USAGE

my $g_Help;
my $g_Quiet;
GetOptions("h|help", \$g_Help,
           "q|quiet", \$g_Quiet);

die $usage if ($g_Help);

my $g_File = shift or die $usage;
my $fh = &Common::openFileOrGzFile($g_File);
&Common::startTicker(5000) if (!$g_Quiet);   # 5000 ticks per dot
while (<$fh>) {
    chomp;
    my @ids = split(/\t/);

    my $upi = $ids[11];
    my @gis = split(/; /, $ids[4]);
    my @pdb = split(/; /, $ids[5]);
    map { s/:.*$// } @pdb;
    my $pdb = &Common::unique(\@pdb);
    my @uni = ($upi);
    push @uni, split(/; /, $ids[0]);
    push @uni, split(/; /, $ids[1]);

    print $upi, "\t@gis\t@$pdb\t@uni\n";
    &Common::tick() if (!$g_Quiet);
}
print STDERR qq(\n\n);
