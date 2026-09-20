#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PYTHONPATH="${SCRIPT_DIR}${PYTHONPATH:+:${PYTHONPATH}}"

${PYTHON:=python3} quarkchain/cluster/cluster.py --cluster_config $(realpath mainnet/singularity/cluster_config_template${QKC_CONFIG_EXT:=}.json) "$@"
