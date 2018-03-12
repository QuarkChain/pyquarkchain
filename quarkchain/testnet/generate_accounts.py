import argparse
import json
from quarkchain.core import Address, Identity


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_accounts", default=10, type=int)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    result = []
    for i in range(args.num_accounts):
        identity = Identity.createRandomIdentity()
        address = Address.createFromIdentity(identity)
        result.append({
            "address": address.toHex(),
            "key": identity.getKey().hex(),
        })

    print(json.dumps(result, indent=4))


if __name__ == "__main__":
    main()
