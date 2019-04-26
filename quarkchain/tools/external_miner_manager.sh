#!/bin/bash

set -u; set -e

type jq >/dev/null 2>&1 || { echo >&2 "Please install jq."; exit 1; }

# c -> config, p -> process number, t -> threads per miner process
# eg: external_miner_manager.sh -c ~/Documents/config.json -p 8 -h localhost
while getopts ":c:p:t:h:" opt; do
	case ${opt} in
		c )
			config=$OPTARG
			;;
		p )
			process=$OPTARG
			;;
		h )
			host=$OPTARG
			;;
		\? )
			echo "Invalid option: $OPTARG" 1>&2
			exit 1
			;;
		: )
			echo "Invalid option: $OPTARG requires an argument" 1>&2
			exit 1
			;;
	esac
done
shift $((OPTIND -1))

# TODO: following full shard key encoding only works for testnet2.4
shards_by_process=()
for chain_id in $(seq 0 7); do
	shard=16#${chain_id}0001
	shards_by_process[$(( chain_id % $process ))]+=" $(($shard))"
done

miner_py_path="$( cd "$(dirname "$0")" ; pwd -P )/external_miner.py"
for shards_per_process in "${shards_by_process[@]}"; do
	python3 $miner_py_path \
		--host   $host \
		--config $config \
		--worker 1 \
		--shards $shards_per_process &
done

wait
