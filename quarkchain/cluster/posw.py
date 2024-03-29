"""Some helper functions for PoSW-related stuff."""
from collections import deque, Counter
from typing import Callable, Optional, Union, Dict

from cachetools import LRUCache
from eth_keys.datatypes import Signature
from eth_keys.exceptions import BadSignature

from quarkchain.config import POSWConfig
from quarkchain.core import (
    MinorBlockHeader,
    RootBlockHeader,
    PoSWInfo,
    RootBlock,
    MinorBlock,
)
from quarkchain.utils import check

Header = Union[MinorBlockHeader, RootBlockHeader]
Block = Union[MinorBlock, RootBlock]


def get_posw_coinbase_blockcnt(
    window_size: int,
    cache: LRUCache,
    header_hash: bytes,
    header_func: Callable[[bytes], Optional[Header]],
) -> Dict[bytes, int]:
    """PoSW needed function: get coinbase address counts up until the given block
    hash (inclusive) within the PoSW window.

    Raise ValueError if anything goes wrong.
    """
    if header_hash in cache:
        addrs = cache[header_hash]
        return Counter(addrs)

    header = header_func(header_hash)
    length = window_size - 1
    if not header:
        raise ValueError("curr block not found: hash {}".format(header_hash.hex()))
    height = header.height
    prev_hash = header.hash_prev_block
    if prev_hash in cache:  # mem cache hit
        addrs = cache[prev_hash].copy()
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
    cache[header_hash] = addrs
    check(len(addrs) <= length)
    return Counter(addrs)


def get_posw_info(
    config: POSWConfig,
    header: Header,
    stakes: int,
    block_cnt: Dict[bytes, int],
    stake_per_block: Optional[int] = None,
    signer: Optional[bytes] = None,
) -> Optional[PoSWInfo]:
    if (
        not (config.ENABLED and header.create_time >= config.ENABLE_TIMESTAMP)
        or header.height == 0
    ):
        return None

    # evaluate stakes before the to-be-added block
    coinbase_recipient = header.coinbase_address.recipient

    required_stakes_per_block = stake_per_block or config.TOTAL_STAKE_PER_BLOCK
    block_threshold = min(config.WINDOW_SIZE, stakes // required_stakes_per_block)
    cnt = block_cnt.get(coinbase_recipient, 0)

    diff = header.difficulty
    ret = lambda success: PoSWInfo(
        diff // config.get_diff_divider(header.create_time) if success else diff,
        block_threshold,
        # mined blocks should include current one, assuming success
        posw_mined_blocks=cnt + 1,
    )

    # fast path
    if block_threshold == 0:
        return ret(False)

    # need to check signature if signer is specified. only applies for root chain
    if signer:
        check(isinstance(header, RootBlockHeader))
        if signer == bytes(20):
            return ret(False)
        block_sig = Signature(header.signature)
        try:
            pubk = block_sig.recover_public_key_from_msg_hash(
                header.get_hash_for_mining()
            )
        except BadSignature:
            return ret(False)

        if pubk.to_canonical_address() != signer:
            return ret(False)

    return ret(cnt < block_threshold)
