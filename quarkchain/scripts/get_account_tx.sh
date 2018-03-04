#!/bin/bash
# This will add a transaction to shard 3

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

$JRPC_CLIENT_BIN \
--method=getAccountTx \
--params='{"addr":"'$GENESIS_ACCOUNT'"}'
