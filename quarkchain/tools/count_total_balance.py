import argparse
import functools
import logging
from typing import List, Tuple, Dict, Any

import jsonrpcclient

logging.root.setLevel(logging.INFO)
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

TIMEOUT = 10
TOTAL_SHARD = 8


@functools.lru_cache(maxsize=5)
def get_jsonrpc_cli(jrpc_url):
    return jsonrpcclient.HTTPClient(jrpc_url)


class Fetcher(object):
    def __init__(self, host: str, timeout: int):
        self.cli = get_jsonrpc_cli(host)
        self.timeout = timeout
        self.shard_to_latest_id = {}

    def _get_root_block(self, root_block_height: int) -> Dict[str, Any]:
        res = self.cli.send(
            jsonrpcclient.Request("getRootBlockByHeight", hex(root_block_height)),
            timeout=self.timeout,
        )
        if not res:
            raise RuntimeError(
                "Failed to query root block at height" % root_block_height
            )
        return res

    def get_latest_minor_block_id_from_root_block(
        self, root_block_height: int
    ) -> Tuple[Dict[str, Any], List[str]]:
        rb = self._get_root_block(root_block_height)
        # Chain ID + shard ID uniquely determines a shard.
        for mh in rb["minorBlockHeaders"]:
            # Assumes minor blocks are sorted by shard and height.
            self.shard_to_latest_id[mh["chainId"] + mh["shardId"]] = mh["id"]
        latest_rb = rb

        # Loop until all shards' latest IDs have been fetched. Should be done at the very first time.
        while len(self.shard_to_latest_id) < TOTAL_SHARD:
            rb = self._get_root_block(root_block_height - 1)
            for mh in rb["minorBlockHeaders"]:
                key = mh["chainId"] + mh["shardId"]
                if key not in self.shard_to_latest_id:
                    self.shard_to_latest_id[key] = mh["id"]

        return latest_rb, list(self.shard_to_latest_id.values())

    def count_total_balance(
        self, block_id: str, token_id: int, start: str
    ) -> Tuple[int, str]:
        res = self.cli.send(
            jsonrpcclient.Request("getTotalBalance", block_id, hex(token_id), start),
            timeout=self.timeout,
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

    host = "http://localhost:38391"
    if args.host:
        host = args.host
        # Assumes http by default.
        if not host.startswith("http"):
            host = "http://" + host

    token_id = int(args.token, 16)

    root_block_height = args.rheight
    fetcher = Fetcher(host, TIMEOUT)
    _, minor_block_ids = fetcher.get_latest_minor_block_id_from_root_block(
        root_block_height
    )
    logging.info(
        "root block at height %d has minor block headers for %d shards"
        % (root_block_height, len(minor_block_ids))
    )

    total_balances = []
    for block_id in minor_block_ids:
        shard = "0x" + block_id[-8:]
        logging.info("querying total balance for shard %s" % shard)
        total, start, cnt = 0, None, 0
        while start != "0x" + "0" * 64:
            balance, start = fetcher.count_total_balance(block_id, token_id, start)
            total += balance
            cnt += 1
            if cnt % 10 == 0:
                logging.info(
                    "shard %s: iteration %d, total balance is %.2f"
                    % (shard, cnt, total / 1e18)
                )

        total_balances.append(total)
        logging.info(
            "shard %s: finished, total balance is %.2f" % (shard, total / 1e18)
        )
        logging.info("======")

    logging.info(
        "counting finished, total balance is %.2f" % (sum(total_balances) / 1e18)
    )


if __name__ == "__main__":
    main()
