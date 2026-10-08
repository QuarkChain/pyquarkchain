#!/bin/bash

target_nofile_limit=1048576
recommended_nofile_limit=131072
hard_nofile_limit=$(ulimit -Hn 2>/dev/null)

if [[ "$hard_nofile_limit" =~ ^[0-9]+$ ]]; then
    echo "Hard nofile limit: $hard_nofile_limit"
    if (( hard_nofile_limit < target_nofile_limit )); then
        target_nofile_limit=$hard_nofile_limit
    fi
elif [[ "$hard_nofile_limit" != "unlimited" ]]; then
    echo "Warning: could not determine a valid hard nofile limit: ${hard_nofile_limit:-<empty>}; keeping the current soft limit" >&2
    target_nofile_limit=
fi

if [[ -n "$target_nofile_limit" ]] && ! ulimit -Sn "$target_nofile_limit"; then
    echo "Warning: failed to set soft nofile limit to $target_nofile_limit; continuing with the current limit" >&2
fi

soft_nofile_limit=$(ulimit -Sn 2>/dev/null)
if [[ "$soft_nofile_limit" =~ ^[0-9]+$ ]]; then
    echo "Soft nofile limit: $soft_nofile_limit"
    if (( soft_nofile_limit < recommended_nofile_limit )); then
        echo "Warning: soft nofile limit $soft_nofile_limit is below the recommended minimum of $recommended_nofile_limit; the node may run out of file descriptors" >&2
    fi
elif [[ "$soft_nofile_limit" == "unlimited" ]]; then
    echo "Soft nofile limit: unlimited"
else
    echo "Warning: could not determine the soft nofile limit after setup: ${soft_nofile_limit:-<empty>}" >&2
fi

${PYTHON:=python3} quarkchain/cluster/cluster.py --cluster_config $(realpath mainnet/singularity/cluster_config_template${QKC_CONFIG_EXT:=}.json) "$@"
