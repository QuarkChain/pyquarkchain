#!/bin/bash
# Get all the tx in a given block
# ./get_block_tx.sh $SHARD_ID $HEIGHT

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

SHARD_ID=3
HEIGHT=0

if [ ! -z $1 ]; then
    SHARD_ID=$1
fi

if [ ! -z $2 ]; then
    HEIGHT=$2
fi

$JRPC_CLIENT_BIN \
--method=getBlockTx \
--params='{"shardId":"'$SHARD_ID'", "height":'$HEIGHT'}'
