#!/bin/bash
# This will add a transaction to shard 3

source constants.sh
$JRPC_CLIENT_BIN \
--method=getAccountBalance \
--params='{"addr":"'$GENESIS_ACCOUNT'"}'
