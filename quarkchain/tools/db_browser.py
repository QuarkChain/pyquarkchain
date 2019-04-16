import argparse
import os
import sys
from quarkchain.cluster.cluster_config import ClusterConfig

from quarkchain.cluster.root_state import (
    RootState
)
from quarkchain.cluster.shard import Shard
from quarkchain.env import DEFAULT_ENV
from quarkchain.db import PersistentDb
from pprint import pprint


def get_root_state():
    env, args = parse_args()
    return RootState(env)


def get_shard_state(full_shard_id):
    env, args = parse_args()
    shard = Shard(env, full_shard_id, None)
    return shard.state


def handle_root_print_tip(env, args):
    root_state = RootState(env)
    pprint(root_state.tip.to_dict())
    print("hash: {}".format(root_state.tip.get_hash().hex()))
    return 0


def handle_root_print_by_height(env, args):
    root_state = RootState(env)
    header = root_state.db.get_root_block_header_by_height(args.height)
    if header is None:
        print("header not found")
        return 1
    pprint(header.to_dict())
    print("hash: {}".format(header.get_hash().hex()))
    return 0


def handle_root_print_by_hash(env, args):
    root_state = RootState(env)
    header = root_state.db.get_root_block_header_by_hash(bytes.fromhex(args.hash))
    if header is None:
        print("header not found")
        return 1
    pprint(header.to_dict())
    print("hash: {}".format(header.get_hash().hex()))
    return 0


def handle_root_print_block(env, args):
    root_state = RootState(env)
    block = root_state.db.get_root_block_by_hash(bytes.fromhex(args.hash))
    if block is None:
        print("header not found")
        return 1
    pprint(block.to_dict())
    print("hash: {}".format(block.header.get_hash().hex()))
    return 0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", type=str, help="action to take")
    parser.add_argument("--height", type=int, help="block height to operate")
    parser.add_argument("--hash", type=str, help="block hash to operate")
    ClusterConfig.attach_arguments(parser)
    args = parser.parse_args()

    env = DEFAULT_ENV.copy()
    env.cluster_config = ClusterConfig.create_from_args(args)

    # initialize database
    if not env.cluster_config.use_mem_db():
        env.db = PersistentDb(
            "{path}/master.db".format(path=env.cluster_config.DB_PATH_ROOT),
            clean=env.cluster_config.CLEAN,
        )

    return env, args


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    env, args = parse_args()

    handlers = {
        "root_print_tip": handle_root_print_tip,
        "root_print_by_height": handle_root_print_by_height,
        "root_print_by_hash": handle_root_print_by_hash,
        "root_print_block": handle_root_print_block,
    }

    if args.action == "interactive":
        pass
    elif args.action not in handlers:
        print("Cannot recognize action {}".format(args.action))
        sys.exit(1)
    else:
        sys.exit(handlers[args.action](env, args))


if __name__ == "__main__":
    main()
