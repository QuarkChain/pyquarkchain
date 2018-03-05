#!/bin/bash
# This will add a transaction to shard 3

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

ADDRESS=$GENESIS_ACCOUNT
if [ ! -z $1 ]; then
    ADDRESS=$1
fi

$JRPC_CLIENT_BIN \
--method=getAccountBalance \
--params='{"addr":"'$ADDRESS'"}'
