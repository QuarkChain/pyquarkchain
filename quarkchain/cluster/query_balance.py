import argparse
import os
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
        "--all_shards", type=bool, default=False, help="query balances in all shards"
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


def print_shard_balance(env, rb, full_shard_id):
    shard = Shard(env, full_shard_id, None)
    state = shard.state
    state.init_from_root_block(rb)

    print("Full shard id: %d" % full_shard_id)
    print("Block height: %d" % state.header_tip.height)
    print("Trie hash: %s" % state.meta_tip.hash_evm_state_root.hex())

    trie = Trie(state.raw_db, state.meta_tip.hash_evm_state_root)

    key = trie.next(bytes(32))
    total = 0
    while key is not None:
        rlpdata = trie.get(key)
        o = rlp.decode(rlpdata, _Account)
        tb = TokenBalances(o.token_balances, state.raw_db)
        balance = tb.balance(token_id_encode("QKC"))
        print("Key: %s, Balance: %s" % (key.hex(), balance))
        total += balance

        key = trie.next(key)

    print("Total balance in shard: %d" % total)
    return total, state.header_tip.height


def print_all_shard_balances(env, rb):
    total = 0
    balance_list = []
    for full_shard_id in env.quark_chain_config.shards:
        balance_list.append(print_shard_balance(env, rb, full_shard_id))
        total += balance_list[-1][0]
    print("Summary:")
    print("Root block height: %d" % rb.header.height)
    for idx, full_shard_id in enumerate(env.quark_chain_config.shards):
        print(
            "Shard id: %d, height: %d, balance: %d"
            % (full_shard_id, balance_list[idx][1], balance_list[idx][0])
        )
    print("Total balance in network: %d" % total)


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    env, args = parse_args()

    rs = RootState(env)
    rb = rs.get_tip_block()
    print("Root block height: %d" % rb.header.height)

    if args.all_shards:
        print_all_shard_balances(env, rb)
    else:
        print_shard_balance(env, rb, args.full_shard_id)


if __name__ == "__main__":
    main()
