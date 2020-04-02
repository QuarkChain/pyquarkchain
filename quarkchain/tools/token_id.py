import argparse

from quarkchain.utils import token_id_encode, token_id_decode


parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, default=None, help="name to id")
parser.add_argument("--id", type=int, default=None, help="id to name")


def main():
    args = parser.parse_args()

    if args.name is not None:
        print(
            "Token name %s to id: %d (0x%s)"
            % (args.name, token_id_encode(args.name), hex(token_id_encode(args.name)))
        )

    if args.id is not None:
        print(
            "Token id %d (0x%s) to id: %s"
            % (args.id, hex(args.id), token_id_decode(args.id))
        )


if __name__ == "__main__":
    main()
