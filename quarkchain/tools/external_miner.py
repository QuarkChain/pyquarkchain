import argparse
import json
import logging
import random
import threading
import time
from typing import Dict, Optional, List, Tuple

import jsonrpcclient
from aioprocessing import AioProcess, AioQueue

from quarkchain.cluster.miner import Miner, MiningWork, MiningResult
from quarkchain.config import ConsensusType

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

logger = logging.getLogger("quarkchain.tools.external_miner")
logger.setLevel(logging.INFO)


def get_work_rpc(
    shard: Optional[int], host: str = "localhost", jrpc_port: int = 38391
) -> MiningWork:
    json_rpc_url = "http://{}:{}".format(host, jrpc_port)
    header_hash, height, diff = jsonrpcclient.request(
        json_rpc_url, "getWork", hex(shard) if shard is not None else None
    )
    return MiningWork(bytes.fromhex(header_hash[2:]), int(height, 16), int(diff, 16))


def submit_work_rpc(
    shard: Optional[int],
    res: MiningResult,
    host: str = "localhost",
    jrpc_port: int = 38391,
) -> bool:
    json_rpc_url = "http://{}:{}".format(host, jrpc_port)
    success = jsonrpcclient.request(
        json_rpc_url,
        "submitWork",
        hex(shard) if shard is not None else None,
        "0x" + res.header_hash.hex(),
        hex(res.nonce),
        "0x" + res.mixhash.hex(),
    )
    return success


def repr_shard(shard_id):
    return "SHARD %s" % shard_id if shard_id is not None else "ROOT"


class ExternalMiner(threading.Thread):
    """One external miner could handles multiple shards."""

    def __init__(self, configs):
        super().__init__()
        self.configs = configs
        self.input_q = AioQueue()
        self.output_q = AioQueue()
        self.process = None

    def run(self):
        work_map = {}  # type: Dict[bytes, Tuple[MiningWork, int]]

        # start the thread to get work
        def get_work():
            # hash -> shard
            nonlocal work_map, self
            # shard -> work
            existing_work = {}  # type: Dict[int, MiningWork]
            while True:
                for config in self.configs:
                    shard_id = config["shard_id"]
                    try:
                        work = get_work_rpc(shard_id)
                    except Exception:
                        # ignore network errors and try next one
                        logger.error("Failed to get work")
                        continue
                    # skip duplicate work
                    if (
                        shard_id in existing_work
                        and existing_work[shard_id].hash == work.hash
                    ):
                        continue
                    mining_params = {
                        "consensus_type": config["consensus_type"],
                        "shard": shard_id,
                    }
                    if self.process:
                        self.input_q.put((work, mining_params))
                        logger.info(
                            "Pushed work to %s height %d"
                            % (repr_shard(shard_id), work.height)
                        )
                    else:
                        # start the process to mine
                        self.process = AioProcess(
                            target=Miner.mine_loop,
                            args=(work, mining_params, self.input_q, self.output_q),
                        )
                        self.process.start()
                        logger.info(
                            "Started mining process for %s" % repr_shard(shard_id)
                        )
                    # bookkeeping
                    existing_work[shard_id] = work
                    work_map[work.hash] = (work, shard_id)
                # random sleep 1~2 secs
                time.sleep(random.uniform(1.0, 2.0))

        get_work_thread = threading.Thread(target=get_work)
        get_work_thread.start()

        # the current thread handles the work submission
        while True:
            res = self.output_q.get(block=True)  # type: MiningResult
            work, shard_id = work_map[res.header_hash]
            while True:
                try:
                    success = submit_work_rpc(shard_id, res)
                    break
                except Exception:
                    logger.error("Failed to submit work, backing off...")
                    time.sleep(0.5)

            logger.info(
                "Mining result submission result: %s for %s height %d"
                % (
                    "success" if success else "failure",
                    repr_shard(shard_id),
                    work.height,
                )
            )
            del work_map[res.header_hash]  # clear bookkeeping


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        required=True,
        type=str,
        help="path to config json file, same as the config running cluster",
    )
    parser.add_argument("--worker", type=int, help="number of workers", default=8)
    args = parser.parse_args()

    with open(args.config) as f:
        config_json = json.load(f)

    worker_configs = [[] for _ in range(args.worker)]  # type: List[List[Dict]]

    for i, shard_config in enumerate(config_json["QUARKCHAIN"]["SHARD_LIST"]):
        config = {
            "shard_id": i,
            "consensus_type": ConsensusType[shard_config["CONSENSUS_TYPE"]],
        }
        worker_configs[i % args.worker].append(config)

    # FIXME: manually add another worker dedicated for root chain
    worker_configs.append(
        [
            {
                "shard_id": None,
                "consensus_type": ConsensusType[
                    config_json["QUARKCHAIN"]["ROOT"]["CONSENSUS_TYPE"]
                ],
            }
        ]
    )

    for config_list in worker_configs:
        ext_miner = ExternalMiner(config_list)
        ext_miner.start()
