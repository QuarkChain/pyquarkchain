import warnings
from functools import lru_cache
from typing import Tuple, Optional, List, Union

from eth_utils import big_endian_to_int, int_to_big_endian

from ethereum.pow import ethash
from ethereum.pow.ethash_utils import (
    get_full_size,
    zpad,
    get_cache_size,
    serialize_cache,
)

try:
    import pyethash

    ETHASH_LIB = "pyethash"  # the C++ based implementation
except ImportError:
    ETHASH_LIB = "ethash"
    warnings.warn("using pure python implementation", ImportWarning)


# always have python implementation declared
def get_cache_slow(cache_size: int, block_number: int) -> List[List[int]]:
    return ethash.mkcache(cache_size, block_number)


def hashimoto_slow(
    block_number: int,
    full_size: int,
    cache: Union[List[List[int]], bytes],
    mining_hash: bytes,
    bin_nonce: bytes,
):
    return ethash.hashimoto_light(full_size, cache, mining_hash, bin_nonce)


if ETHASH_LIB == "ethash":
    get_cache = get_cache_slow
    hashimoto = hashimoto_slow
elif ETHASH_LIB == "pyethash":

    def get_cache(cache_size: int, block_number: int):
        return pyethash.mkcache_bytes(block_number)

    def hashimoto(
        block_number: int,
        full_size: int,
        cache: Union[List[List[int]], bytes],
        mining_hash: bytes,
        bin_nonce: bytes,
    ):
        return pyethash.hashimoto_light(
            block_number, cache, mining_hash, big_endian_to_int(bin_nonce)
        )


else:
    raise Exception("invalid ethash library set")


@lru_cache(maxsize=32)
def check_pow(
    block_number, header_hash, mixhash, nonce, difficulty, is_test=False
) -> bool:
    """Check if the proof-of-work of the block is valid."""
    if len(mixhash) != 32 or len(header_hash) != 32 or len(nonce) != 8:
        return False

    cache_gen, mining_gen = get_cache, hashimoto
    if is_test:
        cache_size, full_size = 1024, 32 * 1024
        # use python implementation to allow overriding cache & dataset size
        cache_gen = get_cache_slow
        mining_gen = hashimoto_slow
    else:
        cache_size, full_size = (
            get_cache_size(block_number),
            get_full_size(block_number),
        )

    cache = cache_gen(cache_size, block_number)
    mining_output = mining_gen(block_number, full_size, cache, header_hash, nonce)
    if mining_output[b"mix digest"] != mixhash:
        return False
    return big_endian_to_int(mining_output[b"result"]) <= 2 ** 256 // (difficulty or 1)


class EthashMiner:
    def __init__(
        self,
        block_number: int,
        difficulty: int,
        header_hash: bytes,
        is_test: bool = False,
    ):
        self.block_number = block_number
        self.difficulty = difficulty
        self.header_hash = header_hash
        self.is_test = is_test

    def mine(
        self, rounds=1000, start_nonce=0
    ) -> Tuple[Optional[bytes], Optional[bytes]]:
        bin_nonce, mixhash = mine(
            self.block_number,
            self.difficulty,
            self.header_hash,
            start_nonce=start_nonce,
            rounds=rounds,
            is_test=self.is_test,
        )
        if bin_nonce is not None:
            return bin_nonce, mixhash

        return None, None


def mine(
    block_number,
    difficulty,
    mining_hash,
    start_nonce: int = 0,
    rounds: int = 1000,
    is_test: bool = False,
) -> Tuple[Optional[bytes], Optional[bytes]]:
    cache_gen, mining_gen = get_cache, hashimoto
    if is_test:
        cache_size, full_size = 1024, 32 * 1024
        # use python implementation to allow overriding cache & dataset size
        cache_gen = get_cache_slow
        mining_gen = hashimoto_slow
    else:
        cache_size, full_size = (
            get_cache_size(block_number),
            get_full_size(block_number),
        )

    cache = cache_gen(cache_size, block_number)
    nonce = start_nonce
    target = zpad(int_to_big_endian(2 ** 256 // (difficulty or 1) - 1), 32)
    for i in range(1, rounds + 1):
        # hashimoto expected big-indian byte representation
        bin_nonce = (nonce + i).to_bytes(8, byteorder="big")
        o = mining_gen(block_number, full_size, cache, mining_hash, bin_nonce)
        if o[b"result"] <= target:
            assert len(bin_nonce) == 8
            assert len(o[b"mix digest"]) == 32
            return bin_nonce, o[b"mix digest"]
    return None, None
