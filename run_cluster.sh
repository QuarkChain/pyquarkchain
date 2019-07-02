#!/bin/bash

${PYTHON:=python3} quarkchain/cluster/cluster.py --cluster_config $(realpath mainnet/singularity/cluster_config_template${QKC_CONFIG_EXT:=}.json) "$@"
