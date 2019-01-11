from quarkchain.core import Branch
from quarkchain.utils import is_p2, check


def is_neighbor(b1: Branch, b2: Branch):
    """A naive algorithm to decide neighbor relationship
    TODO: a better algorithm
    """
    if b1.get_chain_id() == b2.get_chain_id():
        return is_p2(abs(b1.get_shard_id() - b2.get_shard_id()))
    if b1.get_shard_id() == b2.get_shard_id():
        return is_p2(abs(b1.get_chain_id() - b2.get_chain_id()))
    return False
