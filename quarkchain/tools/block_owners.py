# Count minor blocks from db by mining algorithm and coinbase address

import argparse
import operator
from pprint import pprint

from quarkchain.db import PersistentDb
from quarkchain.cluster.root_state import RootDb


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/master.db", type=str)
    parser.add_argument("--root_height", default=0, type=int)
    args = parser.parse_args()
    return args


def shard_id_to_algorithm(shard_id: int) -> str:
    if shard_id < 4:
        return "ETHASH"
    if shard_id < 6:
        return "SHA2SHA2"
    return "QKCHASH"


def main():
    args = parse_args()
    db = RootDb(PersistentDb(args.db), 0)
    header = db.get_tip_header()
    if not header:
        raise RuntimeError("Not a valid RootDb")
    from_height = header.height if args.root_height <= 0 else args.root_height
    tip_header = None
    block = db.get_root_block_by_hash(header.get_hash(), False)
    shard_to_address_count = dict()  # shard -> (recipient -> count)
    while block.header.height > 0:
        if block.header.height > from_height:
            block = db.get_root_block_by_hash(block.header.hash_prev_block, False)
            continue
        if block.header.height == from_height:
            tip_header = block.header
        for minor_header in block.minor_block_header_list:
            shard = minor_header.branch.get_full_shard_id()
            address_hex = minor_header.coinbase_address.recipient.hex()
            address_to_count = shard_to_address_count.setdefault(shard, dict())
            current = address_to_count.setdefault(address_hex, 0)
            address_to_count[address_hex] = current + 1
        block = db.get_root_block_by_hash(block.header.hash_prev_block, False)

    algo_to_address_count = dict()  # algorithm -> (recipient -> count)
    for shard_id, address_to_count in shard_to_address_count.items():
        algo = shard_id_to_algorithm(shard_id)
        addr_to_count = algo_to_address_count.setdefault(algo, dict())
        for address, count in address_to_count.items():
            current = addr_to_count.setdefault(address, 0)
            addr_to_count[address] = current + count

    print(
        "Counting shard blocks from root block {} {}".format(
            tip_header.height, tip_header.get_hash().hex()
        )
    )

    for algo, address_count in algo_to_address_count.items():
        total = sum(address_count.values())

        print()
        print("{} has {} blocks".format(algo, total))
        sorted_by_count = sorted(
            address_count.items(), key=operator.itemgetter(1), reverse=True
        )
        for address, count in sorted_by_count:
            print("{} {} {:.2f}%".format(address, count, count / total * 100))


if __name__ == "__main__":
    main()
