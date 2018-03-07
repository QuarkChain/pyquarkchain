#!/bin/bash
# Get all the tx of the given account
# ./get_account_tx.sh $ADDRESS

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

ADDRESS=$GENESIS_ACCOUNT
if [ ! -z $1 ]; then
    ADDRESS=$1
fi

$JRPC_CLIENT_BIN \
--method=getAccountTx \
--params='{"addr":"'$ADDRESS'", "limit":1}'
