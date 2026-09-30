#!/bin/bash

hard_nofile_limit=$(ulimit -Hn)
ulimit -Sn "$hard_nofile_limit" || {
    echo "Failed to set soft nofile limit to hard limit: $hard_nofile_limit" >&2
    exit 1
}
echo "Soft nofile limit set to: $(ulimit -Sn)"

${PYTHON:=python3} quarkchain/cluster/cluster.py --cluster_config $(realpath mainnet/singularity/cluster_config_template${QKC_CONFIG_EXT:=}.json) "$@"
