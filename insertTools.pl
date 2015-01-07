#!/usr/bin/perl

$| = 1;

use strict;
use warnings;
use MongoDB;

my @tools = (
    {_id => 'agfam1',
     d => 'Agile Genomics family models - version 1.0',
     h => 'AGfam 1',
     f => [qw(name start stop extent hmm_start hmm_stop hmm_extent score evalue)],
     hf => [qw(Name Start Stop Extent), 'HMM start', 'HMM stop', 'HMM extent', 'Score', 'E-value']},

    {_id => 'coils',
     d => 'Predicts coiled coil regions in protein sequences (Russell and Lupas, 1999)',
     h => 'Coiled-coils',
     f => ['start', 'stop'],
     hf => [qw(Start Stop)]},

    {_id => 'das',
     d => 'DAS-TMfilter version 5.0: Predict transmembrane regions',
     h => 'Transmembrane',
     f => [qw(start stop peak peak_score evalue)],
     hf => [qw(Start Stop Peak), 'Peak Score', 'E-value']},

    {_id => 'ecf',
     d => 'Predict Extracytoplasmic Function (ECF) domains',
     h => 'ECF',
     f => [qw(name start stop extent hmm_start hmm_stop hmm_extent score evalue)],
     hf => [qw(Name Start Stop Extent), 'HMM start', 'HMM stop', 'HMM extent', 'Score', 'E-value']},

    {_id => 'gene3d',
     d => 'Structures assigned to genomes',
     h => 'Gene 3D',
     f => [qw(code description start stop evalue)],
     hf => [qw(Code Description Start Stop E-value)]
    },

    {_id => 'hamap',
     d => 'High-quality Automated and Manual Annotation of Proteins',
     h => 'HAMAP',
     f => [qw(rule description start stop evalue)],
     hf => [qw(Rule Description Start Stop E-value)]
    },

    {_id => 'panther',
     d => 'Protein ANalysis THrough Evolutionary Relationships',
     h => 'Panther',
     f => [qw(accession start stop evalue)],
     hf => [qw(Accession Start Stop E-value)]
    },

    {_id => 'patscan',
     d => 'ProSite motif patterns',
     h => 'Patterns',
     f => [qw(accession description start stop)],
     hf => [qw(Accession Description Start Stop)]
    },

    # Original Pfam26 fields
    {_id => 'pfam26',
     d => 'Pfam-A hidden Markov model database version 26.0 (November 2011, 13672 families)',
     h => 'Pfam 26',
     f => [qw(name start stop extent bias hmm_start hmm_stop hmm_extent env_start env_stop env_extent score c_evalue i_evalue acc)],
     hf => [qw(Name Start Stop Extent Bias), 'HMM start', 'HMM stop', 'HMM extent', 'Env start', 'Env stop', 'Env extent', 'Score', 'Cond. E-value', 'Ind. E-value', 'Acc']},

    {_id => 'pir',
     d => 'Protein Information Resource HMMs',
     h => 'PIR',
     f => [qw(accession description start stop evalue)],
     hf => [qw(Accession Description Start Stop E-value)]
    },

    {_id => 'prints',
     d => 'Compendium of protein fingerprints',
     h => 'PRINTS',
     f => [qw(accession description start stop evalue)],
     hf => [qw(Accession Description Start Stop E-value)]
    },

    {_id => 'proscan',
     d => 'ProSite profile scan',
     h => 'Profiles',
     f => [qw(accession description start stop evalue)],
     hf => [qw(Accession Description Start Stop E-value)]
    },

    {_id => 'segs',
     d => 'Predicts regions of low-complexity',
     h => 'Low-complexity segments',
     f => ['start', 'stop'],
     hf => [qw(Start Stop)]},

    {_id => 'signalp',
     d => 'Signal peptide prediction',
     h => 'SignalP',
     f => [qw(gp gn e)],
     hf => [qw(Gram+ Gram- Eukaryotic)]
    },

    {_id => 'smart',
     d => 'Simple Modular Architecture Research Tool',
     h => 'SMART',
     f => [qw(accession name start stop evalue)],
     hf => [qw(Accession Name Start Stop E-value)]
    },

    {_id => 'superfam',
     d => 'Database of structural and functional annotation for all proteins and genomes',
     h => 'SuperFamily',
     f => [qw(accession description start stop evalue)],
     hf => [qw(Accession Description Start Stop E-value)]
    },

    {_id => 'targetp',
     d => 'Predicts subcellular location of eukaryotic proteins',
     h => 'TargetP',
     f => [qw(p np)],
     hf => [qw(Plant Non-plant)]
    },

    {_id => 'tigrfam',
     d => 'HMM resource to support automated annotation of proteins',
     h => 'TIGRFAM',
     f => [qw(accession name start stop evalue)],
     hf => [qw(Accession Name Start Stop E-value)]
    },

    {_id => 'tmhmm',
     d => 'Prediction of transmembrane helices in proteins',
     h => 'TM-HMM',
     f => [qw(start stop)],
     hf => [qw(Start Stop)]
    },

    {_id => 'pfam27',
     d => 'Pfam-A hidden Markov model database version 27.0 (March 2013, 14831 families)',
     h => 'Pfam 27',
     f => [qw(name start stop extent bias hmm_start hmm_stop hmm_extent env_start env_stop env_extent score c_evalue i_evalue acc)],
     hf => [qw(Name Start Stop Extent Bias), 'HMM start', 'HMM stop', 'HMM extent', 'Env start', 'Env stop', 'Env extent', 'Score', 'Cond. E-value', 'Ind. E-value', 'Acc']},
    
    {_id => 'tigrfam14',
     d => 'TIGRFAM 14.0 HMM resource to support automated annotation of proteins',
     h => 'TIGRFAM 14',
     f => [qw(name start stop extent bias hmm_start hmm_stop hmm_extent env_start env_stop env_extent score c_evalue i_evalue acc)],
     hf => [qw(Name Start Stop Extent Bias), 'HMM start', 'HMM stop', 'HMM extent', 'Env start', 'Env stop', 'Env extent', 'Score', 'Cond. E-value', 'Ind. E-value', 'Acc']}
);

$MongoDB::Cursor::timeout = -1;
my $client = new MongoDB::MongoClient(host => 'localhost', port => 27017);
my $db = $client->get_database('seqdepot');
my $collection = $db->get_collection('tools');

foreach my $tool (@tools) {
    my $tool_id = $tool->{_id};
    delete $tool->{_id};

    $collection->update({_id => $tool_id}, {'$set' => $tool}, {upsert => 1, safe => 1});
}
