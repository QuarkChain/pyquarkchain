import warnings
from functools import lru_cache
from typing import Tuple, Optional

from eth_utils import big_endian_to_int, int_to_big_endian

from ethereum.pow import ethash
from ethereum.pow.ethash_utils import get_full_size, zpad, get_cache_size

try:
    import pyethash

    ETHASH_LIB = "pyethash"  # the C++ based implementation
except ImportError:
    ETHASH_LIB = "ethash"
    warnings.warn("using pure python implementation", ImportWarning)

if ETHASH_LIB == "ethash":
    mkcache = ethash.mkcache
    hashimoto_light = ethash.hashimoto_light
    hashimoto_full = ethash.hashimoto_full
elif ETHASH_LIB == "pyethash":

    def hashimoto_light(s, c, h, n):
        return pyethash.hashimoto_light(s, c, h, big_endian_to_int(n))

    def hashimoto_full(*args, **kwargs):
        raise Exception("Not tested yet")

    mkcache = pyethash.mkcache_bytes
else:
    raise Exception("invalid ethash library set")


@lru_cache(maxsize=32)
def check_pow(
    block_number, header_hash, mixhash, nonce, difficulty, is_test=False
) -> bool:
    """Check if the proof-of-work of the block is valid."""
    if len(mixhash) != 32 or len(header_hash) != 32 or len(nonce) != 8:
        return False

    cache_size, full_size = (
        (get_cache_size(block_number), get_full_size(block_number))
        if not is_test
        else (1024, 32 * 1024)
    )
    cache = mkcache(cache_size, block_number)
    mining_output = hashimoto_light(full_size, cache, header_hash, nonce)
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
    cache_size, full_size = (
        (get_cache_size(block_number), get_full_size(block_number))
        if not is_test
        else (1024, 32 * 1024)
    )
    cache = mkcache(cache_size, block_number)
    nonce = start_nonce
    target = zpad(int_to_big_endian(2 ** 256 // (difficulty or 1) - 1), 32)
    for i in range(1, rounds + 1):
        # hashimoto expected big-indian byte representation
        bin_nonce = (nonce + i).to_bytes(8, byteorder="big")
        o = hashimoto_light(full_size, cache, mining_hash, bin_nonce)
        # TODO: maybe use hashimoto_full
        # o = hashimoto_full(dataset, mining_hash, bin_nonce)
        if o[b"result"] <= target:
            assert len(bin_nonce) == 8
            assert len(o[b"mix digest"]) == 32
            return bin_nonce, o[b"mix digest"]
    return None, None
