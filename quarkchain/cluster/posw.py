"""Some helper functions for PoSW-related stuff."""
from collections import deque, Counter
from typing import Callable, Optional, Union, List, Dict

from cachetools import LRUCache

from quarkchain.config import POSWConfig
from quarkchain.core import MinorBlockHeader, RootBlockHeader
from quarkchain.utils import check

Header = Union[MinorBlockHeader, RootBlockHeader]


def _get_coinbase_addresses_until_block(
    posw_config: POSWConfig,
    cache: LRUCache,
    header_hash: bytes,
    header_func: Callable[[bytes], Optional[Header]],
) -> List[bytes]:
    """Get coinbase addresses up until block of given hash within the window."""
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
            header = header_func(header.hash_prev_minor_block)
            check(header is not None, "mysteriously missing block")
    cache[header_hash] = (height, addrs)
    check(len(addrs) <= length)
    return list(addrs)


def get_posw_coinbase_blockcnt(
    posw_config: POSWConfig,
    cache: LRUCache,
    header_hash: bytes,
    header_func: Callable[[bytes], Optional[Header]],
) -> Dict[bytes, int]:
    """ PoSW needed function: get coinbase addresses up until the given block
    hash (inclusive) along with block counts within the PoSW window.

    Raise ValueError if anything goes wrong.
    """
    coinbase_addrs = _get_coinbase_addresses_until_block(
        posw_config, cache, header_hash, header_func
    )
    return Counter(coinbase_addrs)
