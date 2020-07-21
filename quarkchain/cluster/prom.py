import logging
import argparse
import time
from quarkchain.utils import token_id_encode
from typing import List, Tuple
from prometheus_client import start_http_server, Gauge
from quarkchain.tools.count_total_balance import get_jsonrpc_cli, count_total_balance

import jsonrpcclient

# Disable jsonrpcclient verbose logging.
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

TIMEOUT = 10

host = "http://localhost:38391"


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


def get_time_and_balance(
    root_block_height: int, token_id: int
) -> Tuple[int, dict[str:int]]:
    # TODO: handle cases if the root block doesn't contain all the shards.
    time_stamp, minor_block_ids = get_latest_minor_block_id_from_root_block(
        root_block_height
    )

    total_balances = {}
    for block_id in minor_block_ids:
        shard = "0x" + block_id[-8:]
        total, starter, cnt = 0, None, 0
        while starter != "0x" + "0" * 40:
            balance, starter = count_total_balance(block_id, token_id, starter)
            # TODO: add gap to avoid spam.
            total += balance
            cnt += 1
        total_balances[shard] = total
    return time_stamp, total_balances


def get_highest() -> int:
    global host
    cli = get_jsonrpc_cli(host)
    res = cli.send(jsonrpcclient.Request("getRootBlockByHeight"), timeout=TIMEOUT)
    if not res:
        raise RuntimeError("Failed to get latest block height")
    return int(res["height"], 16)


def prometheus_balance(args):
    tokens = {
        token_name: token_id_encode(token_name)
        for token_name in args.tokens.strip().split(sep=",")
    }
    # Create a metric to track token total balance.
    balance_gauge = Gauge(
        "token_total_balance",
        "Total balance of specified tokens",
        ("root_block_height", "timestamp", "shard", "token"),
    )

    while True:
        try:
            # Call when rpc server is ready.
            latest_block_height = get_highest()
            total_balance = {}
            for token_name, token_id in tokens.items():
                total_balance[token_name] = get_time_and_balance(
                    latest_block_height, token_id
                )
        except:
            # Rpc not ready, wait and try again.
            time.sleep(3)
            continue
        for token_name, (ts, balance_dict) in total_balance.items():
            balance_gauge.labels(latest_block_height, ts, "total", token_name).set(
                sum(balance_dict.values())
            )
            for shard_id, shard_bal in balance_dict.items():
                balance_gauge.labels(latest_block_height, ts, shard_id, token_name).set(
                    shard_bal
                )
        time.sleep(args.interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--enable_count_balance",
        default=False,
        dest="balance",
        action="store_true",
        help="enable monitoring total balance",
    )
    parser.add_argument(
        "--interval", type=int, help="seconds between two queries", default=30
    )
    parser.add_argument(
        "--tokens",
        type=str,
        default="QKC",
        help="tokens to be monitored, separated by comma",
    )
    parser.add_argument("--host", type=str, help="host address of the cluster")
    parser.add_argument("--port", type=int, help="prometheus expose port", default=8000)
    args = parser.parse_args()
    global host
    if args.host:
        host = args.host
        # Assumes http by default.
        if not host.startswith("http"):
            host = "http://" + host

    start_http_server(args.port)
    if args.balance:
        prometheus_balance(args)


if __name__ == "__main__":
    main()
