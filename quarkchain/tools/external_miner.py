import argparse
import copy
import functools
import logging
import random
import signal
import threading
import time
from itertools import cycle
from typing import Dict, Optional, List, Tuple

import jsonrpcclient
from queue import LifoQueue

from quarkchain.cluster.miner import Miner, MiningWork, MiningResult
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.utils import int_left_most_bit

# disable jsonrpcclient verbose logging

logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


TIMEOUT = 10

cluster_host = "localhost"


@functools.lru_cache(maxsize=5)
def get_jsonrpc_cli(jrpc_url):
    return jsonrpcclient.HTTPClient(jrpc_url)


def get_work_rpc(
    full_shard_id: Optional[int],
    host: str = "localhost",
    jrpc_port: int = 38391,
    timeout=TIMEOUT,
) -> MiningWork:
    jrpc_url = "http://{}:{}".format(host, jrpc_port)
    cli = get_jsonrpc_cli(jrpc_url)
    header_hash, height, diff = cli.send(
        jsonrpcclient.Request(
            "getWork", hex(full_shard_id) if full_shard_id is not None else None
        ),
        timeout=timeout,
    )
    return MiningWork(bytes.fromhex(header_hash[2:]), int(height, 16), int(diff, 16))


def submit_work_rpc(
    full_shard_id: Optional[int],
    res: MiningResult,
    host: str = "localhost",
    jrpc_port: int = 38391,
    timeout=TIMEOUT,
) -> bool:
    jrpc_url = "http://{}:{}".format(host, jrpc_port)
    cli = get_jsonrpc_cli(jrpc_url)
    success = cli.send(
        jsonrpcclient.Request(
            "submitWork",
            hex(full_shard_id) if full_shard_id is not None else None,
            "0x" + res.header_hash.hex(),
            hex(res.nonce),
            "0x" + res.mixhash.hex(),
        ),
        timeout=timeout,
    )
    return success


def repr_shard(full_shard_id: Optional[int]):
    if full_shard_id is None:
        return "ROOT"
    chain = full_shard_id >> 16
    shard = full_shard_id & 0xffff
    shard -= 1 << (int_left_most_bit(shard) - 1)
    return "CHAIN %d SHARD %d" % (chain, shard)


class ExternalMiner(threading.Thread):
    """One external miner could handles multiple shards."""

    def __init__(self, configs, stopper: threading.Event):
        super().__init__()
        self.configs = configs
        self.stopper = stopper
        self.input_q = LifoQueue()
        self.output_q = LifoQueue()

    def run(self):
        global cluster_host
        # header hash -> (work, shard)
        work_map = {}  # type: Dict[bytes, Tuple[MiningWork, Optional[int]]]

        # start the thread to get work
        def get_work(configs, stopper, input_q, output_q):
            nonlocal work_map
            configs = copy.copy(configs)
            # shard -> work
            existing_work = {}  # type: Dict[int, MiningWork]
            mining_thread = None
            while not stopper.is_set():
                total_wait_time = random.uniform(2.0, 3.0)
                random.shuffle(configs)
                for config in configs:
                    # random sleep between each shard
                    time.sleep(total_wait_time / len(configs))
                    full_shard_id = config["full_shard_id"]
                    try:
                        work = get_work_rpc(full_shard_id, host=cluster_host)
                    except Exception as e:
                        # ignore network errors and try next one
                        print(
                            "Failed to get work for {}".format(
                                repr_shard(full_shard_id)
                            ),
                            e,
                        )
                        continue
                    # skip duplicate work
                    if (
                        full_shard_id in existing_work
                        and existing_work[full_shard_id].hash == work.hash
                    ):
                        continue
                    # bookkeeping
                    existing_work[full_shard_id] = work
                    work_map[work.hash] = (work, full_shard_id)

                    mining_params = {
                        "consensus_type": config["consensus_type"],
                        "shard": full_shard_id,
                        "target_time": config["target_block_time"] + time.time(),
                        "rounds": 100,
                    }
                    if mining_thread:
                        input_q.put((work, mining_params))
                        print(
                            "Added work to queue of %s height %d"
                            % (repr_shard(full_shard_id), work.height)
                        )
                    else:
                        # start the thread to mine
                        mining_thread = threading.Thread(
                            target=Miner.mine_loop,
                            args=(work, mining_params, input_q, output_q),
                        )
                        mining_thread.start()
                        print("Started mining thread on %s" % repr_shard(full_shard_id))

            # loop stopped, notify the mining thread
            if mining_thread:
                input_q.put((None, {}))
                mining_thread.join()

            # END OF `get_work` FUNCTION

        get_work_thread = threading.Thread(
            target=get_work,
            args=(self.configs, self.stopper, self.input_q, self.output_q),
        )
        get_work_thread.start()

        # the current thread handles the work submission
        while True:
            res = self.output_q.get(block=True)  # type: Optional[MiningResult]
            if not res:
                # get_work terminated -> mining terminated
                # join and terminate itself too
                get_work_thread.join()
                return
            work, full_shard_id = work_map.pop(res.header_hash)
            while True:
                try:
                    success = submit_work_rpc(full_shard_id, res, host=cluster_host)
                    break
                except Exception as e:
                    print("Failed to submit work, backing off...", e)
                    time.sleep(0.5)

            print(
                "Mining result submission result: %s for %s height %d"
                % (
                    "success" if success else "failure",
                    repr_shard(full_shard_id),
                    work.height,
                )
            )


class SigHandler:
    """Graceful exit for mining threads."""

    def __init__(self, stopper: threading.Event, threads: List[threading.Thread]):
        self.stopper = stopper
        self.threads = threads

    def __call__(self, signum, frame):
        self.stopper.set()
        for thread in self.threads:
            thread.join()
        print("Stop mining")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        required=True,
        type=str,
        help="<Required> path to config json file, same as the config running cluster",
    )
    parser.add_argument(
        "-s",
        "--shards",
        required=True,
        nargs="+",
        help='<Required> specify shards (identified by full_shard_key) to mine, use "R" to indicate root chain',
    )
    parser.add_argument(
        "--worker", type=int, help="number of worker threads", default=1
    )
    parser.add_argument(
        "--host", type=str, help="host address of the cluster", default="localhost"
    )
    args = parser.parse_args()

    with open(args.config) as f:
        cluster_config = ClusterConfig.from_json(f.read())
        qkc_config = cluster_config.QUARKCHAIN

    global cluster_host
    if args.host:
        cluster_host = args.host

    # 1 worker config <-> 1 mining thread <-> 1 or more shards
    worker_configs = [
        [] for _ in range(min(args.worker, len(args.shards)))
    ]  # type: List[List[Dict]]

    for worker_i, shard_str in zip(cycle(range(args.worker)), args.shards):
        if shard_str.isnumeric():
            full_shard_key = int(shard_str)
            full_shard_id = qkc_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
            c = qkc_config.shards[full_shard_id]
        else:
            full_shard_id = None
            c = qkc_config.ROOT
        worker_configs[worker_i].append(
            {
                "full_shard_id": full_shard_id,
                "consensus_type": c.CONSENSUS_TYPE,
                "target_block_time": c.CONSENSUS_CONFIG.TARGET_BLOCK_TIME,
            }
        )

    miners = []
    stopper = threading.Event()
    for config_list in worker_configs:
        ext_miner = ExternalMiner(config_list, stopper)
        ext_miner.start()
        miners.append(ext_miner)

    sig_handler = SigHandler(stopper, miners)
    signal.signal(signal.SIGINT, sig_handler)


if __name__ == "__main__":
    main()
