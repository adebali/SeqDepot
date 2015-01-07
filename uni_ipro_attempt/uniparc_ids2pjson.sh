#!/bin/bash

USAGE="Usage: $0 <uniparc id map file>"
if [ $# -ne 1 ];
  then
    echo $USAGE
    exit 1
fi


SCRIPT_DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$SCRIPT_DIR/uniparc_idmap2tsv.pl $1 | sort -k1 --parallel=4 | $SCRIPT_DIR/tsvIdMap2pjson.pl