import argparse
import os
from pprint import pprint

from quarkchain.cluster.cluster_config import ClusterConfig

from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.shard import Shard
from quarkchain.env import DEFAULT_ENV
from quarkchain.evm.trie import Trie
from quarkchain.db import PersistentDb
from quarkchain.evm.state import _Account, TokenBalances
import rlp
from quarkchain.utils import token_id_encode


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full_shard_id", type=int, help="full shard id to operate")
    parser.add_argument(
        "--all_shards",
        action="store_true",
        default=False,
        help="query balances in all shards",
    )
    parser.add_argument(
        "--recipient", default=None, type=str, help="query a specific recipient"
    )
    parser.add_argument(
        "--minor_block_height",
        default=None,
        type=int,
        help="query balance at specific minor block height",
    )
    parser.add_argument(
        "--print_addr",
        default=False,
        action="store_true",
        help="show address instead of key",
    )
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


def print_shard_balance(env, rb, full_shard_id, args):
    shard = Shard(env, full_shard_id, None)
    state = shard.state
    state.init_from_root_block(rb)

    print("Full shard id: %d" % full_shard_id)
    print("Block height: %d" % state.header_tip.height)
    print("Trie hash: %s" % state.meta_tip.hash_evm_state_root.hex())

    trie = Trie(state.raw_db, state.meta_tip.hash_evm_state_root)

    key = trie.next(bytes(32))
    total = 0
    accounts = 0
    while key is not None:
        rlpdata = trie.get(key)
        o = rlp.decode(rlpdata, _Account)
        tb = TokenBalances(o.token_balances, state.raw_db)
        balance = tb.balance(token_id_encode("QKC"))
        if args.print_addr:
            addr = state.raw_db.get(key)
            print("Addr: %s, Balance: %s" % (addr.hex(), balance))
        else:
            print("Key: %s, Balance: %s" % (key.hex(), balance))

        total += balance
        accounts += 1

        key = trie.next(key)

    print("Total balance in shard: %d, accounts: %d" % (total, accounts))
    return total, state.header_tip.height, accounts


def print_all_shard_balances(env, rb, args):
    total = 0
    accounts = 0

    if args.recipient:
        for full_shard_id in env.quark_chain_config.shards:
            shard = Shard(env, full_shard_id, None)
            state = shard.state
            state.init_from_root_block(rb)
            total += sum(state.get_balances(args.recipient).values())
        print("\nTotal balance of recipient %d: %d" % (args.recipient, total))

    else:
        balance_list = []
        for full_shard_id in env.quark_chain_config.shards:
            balance_list.append(print_shard_balance(env, rb, full_shard_id, args))
            total += balance_list[-1][0]
            accounts += balance_list[-1][2]
        print("Summary:")
        print("Root block height: %d" % rb.header.height)
        for idx, full_shard_id in enumerate(env.quark_chain_config.shards):
            print(
                "Shard id: %d, height: %d, balance: %d, accounts: %d"
                % (
                    full_shard_id,
                    balance_list[idx][1],
                    balance_list[idx][0],
                    balance_list[idx][2],
                )
            )
        print("\nTotal accounts in network: %d" % accounts)
        print("Total balance in network: %d" % total)


def print_recipient_balance(env, rb, args):
    assert args.recipient.startswith("0x")
    recipient = bytes.fromhex(args.recipient[2:])
    shard = Shard(env, args.full_shard_id, None)
    state = shard.state
    state.init_from_root_block(rb)

    if args.minor_block_height:
        balance = state.get_balances(recipient, args.minor_block_height)
    else:
        balance = state.get_balances(recipient)

    print("Recipient: %s\nBalances: " % args.recipient, end="", flush=True)
    pprint(balance)


def print_balance_by_block_height(env, rb, minor_block_height):
    for full_shard_id in env.quark_chain_config.shards:
        balance, height, accounts = print_shard_balance(env, rb, full_shard_id, args)
        if height == minor_block_height:
            print("-" * 30)
            print(
                "Total balance in shard %d at height %d: %d, accounts: %d"
                % (full_shard_id, minor_block_height, balance, accounts)
            )
            return balance
    print("-" * 30)
    print("Invalid height")


def main():
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../cluster"))

    env, args = parse_args()

    rs = RootState(env)
    rb = rs.get_tip_block()
    print("Root block height: %d" % rb.header.height)

    if args.all_shards:
        print_all_shard_balances(env, rb, args)
    elif args.recipient:
        print_recipient_balance(env, rb, args)
    elif args.full_shard_id:
        print_shard_balance(env, rb, args.full_shard_id, args)
    elif args.minor_block_height:
        print_balance_by_block_height(env, rb, args.minor_block_height)


if __name__ == "__main__":
    main()
