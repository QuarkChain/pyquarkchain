import logging
import time
from quarkchain.utils import token_id_encode
from typing import List, Tuple
from prometheus_client import start_http_server, Gauge
from quarkchain.cluster.cluster_config import PrometheusConfig
from quarkchain.tools.count_total_balance import get_jsonrpc_cli, count_total_balance

import jsonrpcclient

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

TIMEOUT = PrometheusConfig.TIMEOUT

host = PrometheusConfig.HOST


def get_latest_minor_block_id_from_root_block(
    root_block_height: int,
) -> Tuple[int, List[str]]:
    global host
    cli = get_jsonrpc_cli(host)
    res = cli.send(
        jsonrpcclient.Request("getRootBlockByHeight", hex(root_block_height)),
        timeout=TIMEOUT,
    )
    if not res:
        raise RuntimeError("Failed to query root block at height" % root_block_height)

    # Chain ID + shard ID uniquely determines a shard.
    shard_to_header = {}
    for mh in res["minorBlockHeaders"]:
        # Assumes minor blocks are sorted by shard and height.
        shard_to_header[mh["chainId"] + mh["shardId"]] = mh["id"]

    return res["timestamp"], list(shard_to_header.values())


def get_balance(root_block_height, token_id):
    # TODO: handle cases if the root block doesn't contain all the shards.
    rbt, minor_block_ids = get_latest_minor_block_id_from_root_block(root_block_height)

    total_balances = {}
    for block_id in minor_block_ids:
        shard = "0x" + block_id[-8:]
        total, starter, cnt = 0, None, 0
        while starter != "0x" + "0" * 40:
            balance, starter = count_total_balance(block_id, token_id, starter)
            total += balance
            cnt += 1
        total_balances[shard] = total
    return rbt, total_balances


def get_highest():
    global host
    cli = get_jsonrpc_cli(host)
    res = cli.send(jsonrpcclient.Request("getRootBlockByHeight"), timeout=TIMEOUT,)
    if not res:
        raise RuntimeError("Failed to get latest block height")
    return res["height"]


def main():
    global host
    # Assumes http by default.
    if not host.startswith("http"):
        host = "http://" + host

    root_block_height = 1
    # get latest height
    while True:
        try:
            root_block_height = int(get_highest(), 16)
            break
        except:
            time.sleep(3)
            continue
    tokens = {
        token_name: token_id_encode(token_name)
        for token_name in PrometheusConfig.TOKENS
    }

    start_http_server(PrometheusConfig.PORT)
    # Create a metric to track token total balance
    token_total_balance = Gauge(
        f"token_total_balance",
        f"Total balance of tokens",
        ("Root_block_height", "shard", "token"),
    )

    # TODO: Need a for loop to fetch height from 1 to Highest and expose them
    # expose highest
    # TODO: Update highest block height
    while True:
        try:
            # call when rpc server is ready
            # save timestamp as well, not used currently
            total_balance = {}
            for tname, tid in tokens.items():
                total_balance[tname] = get_balance(root_block_height, tid)
        except:
            time.sleep(3)
            continue
        for tname, bal in total_balance.items():
            token_total_balance.labels(root_block_height, "total", tname).set(
                sum(bal[1].values())
            )
            for shard, shard_bal in bal[1].items():
                token_total_balance.labels(root_block_height, shard, tname).set(
                    shard_bal
                )
            time.sleep(PrometheusConfig.INTERVAL)


if __name__ == "__main__":
    main()