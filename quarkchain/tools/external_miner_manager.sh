#!/bin/bash

set -u; set -e

type jq >/dev/null 2>&1 || { echo >&2 "Please install jq."; exit 1; }

# c -> config, p -> process number, t -> threads per miner process
# eg: external_miner_manager.sh -c ~/Documents/config.json -p 9 -t 1
while getopts ":c:p:t:" opt; do
	case ${opt} in
		c )
			config=$OPTARG
			;;
		p )
			process=$OPTARG
			;;
		t )
			thread=$OPTARG
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

shards=()
shard_cnt=$(jq '.QUARKCHAIN.SHARD_LIST | length' < $config)
end_shard=$(( $shard_cnt - 1))
for i in $(seq 0 $end_shard); do
	shards+=("$i")
done
shards+=("R")  # root chain

shards_by_process=()
i=0
for shard in "${shards[@]}"; do
	shards_by_process[$(( i % $process ))]+=" $shard"
	i=$(( $i + 1))
done

miner_py_path="$( cd "$(dirname "$0")" ; pwd -P )/external_miner.py"
for shards_per_process in "${shards_by_process[@]}"; do
	python $miner_py_path \
		--config $config \
		--worker $thread \
		--shards $shards_per_process &
done

wait
