#!/bin/bash
# This will add a transaction to shard 3

source constants.sh

$JRPC_CLIENT_BIN \
--method=addTx \
--params='{"fromAddr":"'$GENESIS_ACCOUNT'", "toAddr":"'$GENESIS_ACCOUNT'", "key":"'$GENESIS_KEY'", "quarkash":0.5, "fee":0.1234}'
