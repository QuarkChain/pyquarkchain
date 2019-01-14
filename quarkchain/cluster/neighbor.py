from quarkchain.core import Branch
from quarkchain.utils import is_p2, check


def is_neighbor(b1: Branch, b2: Branch, shard_size: int):
    """A naive algorithm to decide neighbor relationship
    TODO: a better algorithm, because the current one ensures 32 neighbors ONLY when there are 2^32 shards
    """
    if shard_size <= 32:
        return True
    if b1.get_chain_id() == b2.get_chain_id():
        return is_p2(abs(b1.get_shard_id() - b2.get_shard_id()))
    if b1.get_shard_id() == b2.get_shard_id():
        return is_p2(abs(b1.get_chain_id() - b2.get_chain_id()))
    return False
