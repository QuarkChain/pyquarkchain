import logging
import argparse
import time
from quarkchain.utils import token_id_encode
from typing import Tuple, Dict
from quarkchain.tools.count_total_balance import Fetcher

try:
    # Custom dependencies. Required if the user needs to set up a prometheus client.
    from prometheus_client import start_http_server, Gauge
except Exception as e:
    print("======")
    print("Dependency requirement for prometheus client is not met.")
    print("Don't run cluster in this mode.")
    print("======")
    raise e

import jsonrpcclient

# Disable jsonrpcclient verbose logging.
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

TIMEOUT = 10
fetcher = None


def get_time_and_balance(
    root_block_height: int, token_id: int
) -> Tuple[int, Dict[str, int]]:
    global fetcher
    assert isinstance(fetcher, Fetcher)

    rb, minor_block_ids = fetcher.get_latest_minor_block_id_from_root_block(
        root_block_height
    )
    timestamp = rb["timestamp"]

    total_balances = {}
    for block_id in minor_block_ids:
        shard = "0x" + block_id[-8:]
        total, start = 0, None
        while start != "0x" + "0" * 64:
            balance, start = fetcher.count_total_balance(block_id, token_id, start)
            # TODO: add gap to avoid spam.
            total += balance
        total_balances[shard] = total
    return timestamp, total_balances


def get_highest() -> int:
    global fetcher
    assert isinstance(fetcher, Fetcher)

    res = fetcher.cli.send(
        jsonrpcclient.Request("getRootBlockByHeight"), timeout=TIMEOUT
    )
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
        "token_total_balance", "Total balance of specified tokens", ("shard", "token")
    )
    # A meta gauge to track block height, because if things go wrong, we want
    # to know which root block has the wrong balance.
    block_height_gauge = Gauge("block_height", "Height for root block and minor block")

    while True:
        try:
            # Call when rpc server is ready.
            latest_block_height = get_highest()
            total_balance = {}
            for token_name, token_id in tokens.items():
                total_balance[token_name] = get_time_and_balance(
                    latest_block_height, token_id
                )
        except Exception as e:
            print("failed to get latest root block height", e)
            # Rpc not ready, wait and try again.
            time.sleep(3)
            continue
        block_height_gauge.set(latest_block_height)
        for token_name, (_, balance_dict) in total_balance.items():
            balance_gauge.labels("total", token_name).set(sum(balance_dict.values()))
            for shard_id, shard_bal in balance_dict.items():
                balance_gauge.labels(shard_id, token_name).set(shard_bal)
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

    host = "http://localhost:38391"
    if args.host:
        host = args.host
        # Assumes http by default.
        if not host.startswith("http"):
            host = "http://" + host

    # Local prometheus server.
    start_http_server(args.port)

    global fetcher
    fetcher = Fetcher(host, TIMEOUT)

    if args.balance:
        prometheus_balance(args)


if __name__ == "__main__":
    main()
