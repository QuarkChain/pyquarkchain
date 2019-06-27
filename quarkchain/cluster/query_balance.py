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
    parser.add_argument("--node_id", default="", type=str)
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

    rs = RootState(env)
    rb = rs.get_tip_block()
    print("Root block height: %d" % rb.header.height)

    shard = Shard(env, args.full_shard_id, None)
    state = shard.state
    state.init_from_root_block(rb)

    print("Full shard id: %d" % args.full_shard_id)
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

    print("Total balance: %d" % total)


if __name__ == "__main__":
    main()
