#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

TX_HASH="9073506e626637162700ff4da719785d1ffc469b750cc57795888c654218d5e1"
INDEX=0

$JRPC_CLIENT_BIN \
--method=getTxOutputInfo \
--params='{"txHash":"'$TX_HASH'", "index":'$INDEX'}'
