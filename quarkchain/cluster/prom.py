import functools
import logging
import time
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


def get_latest_minor_block_id_from_root_block(root_block_height: int) -> List[str]:
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

    return list(shard_to_header.values())


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
    minor_block_ids = get_latest_minor_block_id_from_root_block(root_block_height)

    total_balances = {}
    for block_id in minor_block_ids:
        shard = "0x" + block_id[-8:]
        total, starter, cnt = 0, None, 0
        while starter != "0x" + "0" * 40:
            balance, starter = count_total_balance(block_id, token_id, starter)
            total += balance
            cnt += 1
        total_balances[shard] = total / 1e18
    return total_balances


def main():
    global host
    # Assumes http by default.
    if not host.startswith("http"):
        host = "http://" + host

    root_block_height = PrometheusConfig.ROOTBLOCK_HEIGHT
    token_id = int(PrometheusConfig.TOKEN)

    start_http_server(PrometheusConfig.PORT)
    # Create a metric to track qkc total balance
    QKC_TOTAL_BALANCE = Gauge("qkc_total_balance", "Total balance in current net")
    # Use a dict to store gauge for each shard
    QKC_SHARD_BALANCE = {}
    while True:
        try:
            # call when rpc server is ready
            total_balance = get_balance(root_block_height, token_id)
        except:
            time.sleep(5)
            continue
        QKC_TOTAL_BALANCE.set(sum(total_balance.values()))
        for shard, bal in total_balance.items():
            if shard not in QKC_SHARD_BALANCE:
                QKC_SHARD_BALANCE[shard] = Gauge(
                    f"qkc_shard_{shard}_balance", f"Total balance in shard {shard}"
                )
            QKC_SHARD_BALANCE[shard].set(bal)
        time.sleep(PrometheusConfig.GAP)


if __name__ == "__main__":
    main()
