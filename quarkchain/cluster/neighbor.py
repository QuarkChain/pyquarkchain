from quarkchain.core import Branch
from quarkchain.utils import is_p2, check


def is_neighbor(b1: Branch, b2: Branch):
    """A naive algorithm to decide neighbor relationship
    Two shards are neighbor iff there is only 1 bit difference in their shard ids.
    This only applies if there are more than 32 shards in the network.
    Otherwise all shards are neighbor to each other.
    TODO: a better algorithm
    """
    check(b1.get_shard_size() == b2.get_shard_size())
    check(b1.get_shard_id() != b2.get_shard_id())

    if b1.get_shard_size() <= 32:
        return True

    return is_p2(abs(b1.get_shard_id() - b2.get_shard_id()))
