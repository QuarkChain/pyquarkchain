import argparse
import functools
import logging
from typing import List, Tuple

import jsonrpcclient

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


TIMEOUT = 10

host = "http://localhost:38391"


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

    # Chain ID + shard ID unique determines a shard.
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rheight", type=int, help="root block height to query", required=True
    )
    parser.add_argument("--token", type=str, help="token ID to query", default="0x8bb0")
    parser.add_argument("--host", type=str, help="host address of the cluster")
    args = parser.parse_args()

    global host
    if args.host:
        host = args.host
        # Assumes http by default.
        if not host.startswith("http"):
            host = "http://" + host

    token_id = int(args.token, 16)

    root_block_height = args.rheight
    # TODO: handle cases if a shard is not included in queried root block.
    minor_block_ids = get_latest_minor_block_id_from_root_block(root_block_height)
    print(
        "root block at height %d has minor block headers for %d shards"
        % (root_block_height, len(minor_block_ids))
    )

    total_balances = []
    for block_id in minor_block_ids:
        shard = "0x" + block_id[-8:]
        print("querying total balance for shard %s" % shard)
        total, starter, cnt = 0, None, 0
        while starter != "0x" + "0" * 40:
            balance, starter = count_total_balance(block_id, token_id, starter)
            total += balance
            cnt += 1
            if cnt % 10 == 0:
                print(
                    "shard %s: iteration %d, total balance is %.2f"
                    % (shard, cnt, total / 1e18)
                )

        total_balances.append(total)
        print("shard %s: finished, total balance is %.2f" % (shard, total / 1e18))
        print("======")

    print("counting finished, total balance is %.2f" % (sum(total_balances) / 1e18))


if __name__ == "__main__":
    main()
