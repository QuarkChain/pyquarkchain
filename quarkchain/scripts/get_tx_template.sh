#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

$JRPC_CLIENT_BIN \
--method=getTxTemplate \
--params='{"fromAddr":"'$GENESIS_ACCOUNT'", "toAddr":"'$GENESIS_ACCOUNT'", "quarkash":0.5, "fee":0.01}'
