import argparse
import json
from quarkchain.core import Address, Identity
from quarkchain.utils import sha3_256


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_accounts", default=10, type=int)
    parser.add_argument("--shard", default=-1, type=int)
    parser.add_argument("--shard_size", default=256, type=int)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    result = []
    while True:
        identity = Identity.create_random_identity()
        address = Address.create_from_identity(identity)
        if args.shard > -1:
            # Follow the same algorithm in testnet web
            fullShard = int.from_bytes(
                sha3_256(address.recipient.hex().encode("utf-8"))[:4], "big"
            )
            shard = fullShard & (args.shard_size - 1)
            if shard != args.shard:
                continue
            address = Address.create_from_identity(identity, fullShard)
        result.append({"address": address.to_hex(), "key": identity.get_key().hex()})
        args.num_accounts -= 1
        if args.num_accounts == 0:
            break

    print(json.dumps(result, indent=4))


if __name__ == "__main__":
    main()
