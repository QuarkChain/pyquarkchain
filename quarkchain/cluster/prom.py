import functools
import logging
import time
from quarkchain.utils import token_id_decode, token_id_encode
from typing import List, Tuple
from prometheus_client import start_http_server, Gauge
from quarkchain.cluster.cluster_config import PrometheusConfig

import jsonrpcclient

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

TIMEOUT = PrometheusConfig.TIMEOUT

host = PrometheusConfig.HOST


@functools.lru_cache(maxsize=5)
def get_jsonrpc_cli(jrpc_url):
    return jsonrpcclient.HTTPClient(jrpc_url)


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


def count_total_balance(block_id: str, token_id: int, starter: str) -> Tuple[int, str]:
    global host
    cli = get_jsonrpc_cli(host)
    res = cli.send(
        jsonrpcclient.Request("getTotalBalance", block_id, hex(token_id), starter),
        timeout=TIMEOUT,
    )
    if not res:
        raise RuntimeError("Failed to count total balance")
    return int(res["totalBalance"], 16), res["next"]


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
    token_name = PrometheusConfig.TOKEN
    token_id = token_id_encode(token_name)

    start_http_server(PrometheusConfig.PORT)
    # Create a metric to track token total balance
    TOKEN_TOTAL_BALANCE = Gauge(
        f"{token_name}_total_balance",
        f"Total balance of {token_name}",
        ("Root_block_height", "shard"),
    )

    # TODO: Need a for loop to fetch height from 1 to Highest and expose them
    # expose highest
    # TODO: Update highest block height
    while True:
        # TOKEN_TOTAL_BALANCE.labels(root_block_height, 'total').set(0)
        try:
            # call when rpc server is ready
            # save timestamp as well, not used currently
            rbt, total_balance = get_balance(root_block_height, token_id)
        except:
            time.sleep(3)
            continue
        TOKEN_TOTAL_BALANCE.labels(root_block_height, "total").set(
            sum(total_balance.values())
        )
        for shard, bal in total_balance.items():
            TOKEN_TOTAL_BALANCE.labels(root_block_height, shard).set(bal)
        time.sleep(PrometheusConfig.GAP)


if __name__ == "__main__":
    main()
