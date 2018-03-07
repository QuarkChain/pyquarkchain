#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

$JRPC_CLIENT_BIN \
--method=getTxTemplate \
--params='{"fromAddr":"'$GENESIS_ACCOUNT'", "toAddr":"'$GENESIS_ACCOUNT'", "quarkash":123.456, "fee":0.0}'
