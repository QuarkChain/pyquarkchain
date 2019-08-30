#!/bin/bash

QKC_CONFIG=`pwd`/mainnet/singularity/cluster_config_template.json ${PYTHON:=python3} quarkchain/tools/miner_address.py "$@"
