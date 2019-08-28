"""Some helper functions for PoSW-related stuff."""
from collections import deque, Counter
from typing import Callable, Optional, Union, Dict

from cachetools import LRUCache

from quarkchain.config import POSWConfig
from quarkchain.core import MinorBlockHeader, RootBlockHeader
from quarkchain.utils import check

Header = Union[MinorBlockHeader, RootBlockHeader]


def get_posw_coinbase_blockcnt(
    posw_config: POSWConfig,
    cache: LRUCache,
    header_hash: bytes,
    header_func: Callable[[bytes], Optional[Header]],
) -> Dict[bytes, int]:
    """ PoSW needed function: get coinbase address counts up until the given block
    hash (inclusive) within the PoSW window.

    Raise ValueError if anything goes wrong.
    """
    header = header_func(header_hash)
    length = posw_config.WINDOW_SIZE - 1
    if not header:
        raise ValueError("curr block not found: hash {}".format(header_hash.hex()))
    height = header.height
    prev_hash = header.hash_prev_block
    if prev_hash in cache:  # mem cache hit
        _, addrs = cache[prev_hash]
        addrs = addrs.copy()
        if len(addrs) == length:
            addrs.popleft()
        addrs.append(header.coinbase_address.recipient)
    else:  # miss, iterating DB
        addrs = deque()
        for _ in range(length):
            addrs.appendleft(header.coinbase_address.recipient)
            if header.height == 0:
                break
            header = header_func(header.hash_prev_block)
            check(header is not None, "mysteriously missing block")
    cache[header_hash] = (height, addrs)
    check(len(addrs) <= length)
    coinbase_addrs = list(addrs)
    return Counter(coinbase_addrs)
