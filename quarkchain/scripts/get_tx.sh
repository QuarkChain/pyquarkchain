#!/bin/bash
# Get tx detail
# ./get_tx.sh $TX_HASH

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

ROOT_COIN_BASE_TX_HASH="9073506e626637162700ff4da719785d1ffc469b750cc57795888c654218d5e1"
MINOR_COIN_BASE_TX_HASH="0c6a8447692aa75d3a5b958d8162375f1cc14dbfe19d92743c032384c061c40c"
TX_HASH=$MINOR_COIN_BASE_TX_HASH

if [ ! -z $1 ]; then
    TX_HASH=$1
fi

$JRPC_CLIENT_BIN \
--method=getTx \
--params='{"txHash":"'$TX_HASH'"}'
