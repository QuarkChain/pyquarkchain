#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
source $SCRIPT_DIR/constants.sh

$JRPC_CLIENT_BIN \
--method=getTxTemplate \
--params='{"fromAddr":"'$GENESIS_ACCOUNT'", "toAddr":"'$GENESIS_ACCOUNT'", "quarkash":1234567890123456789012345678, "fee":3210123456789012345678}'
