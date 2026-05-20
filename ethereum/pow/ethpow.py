from typing import Tuple, Optional
from functools import lru_cache

from ethereum.pow import ethash
from ethereum.pow.ethash_utils import get_full_size, get_cache_size, EPOCH_LENGTH


def get_cache(cache_size: int, epoch: int):
    return ethash.mkcache(cache_size, epoch)


def hashimoto(
    block_number: int,
    full_size: int,
    cache,
    mining_hash: bytes,
    bin_nonce: bytes,
):
    return ethash.hashimoto_light(full_size, cache, mining_hash, bin_nonce, block_number)


@lru_cache(maxsize=32)
def check_pow(
    block_number, header_hash, mixhash, nonce, difficulty, is_test=False
) -> bool:
    """Check if the proof-of-work of the block is valid."""
    if len(mixhash) != 32 or len(header_hash) != 32 or len(nonce) != 8:
        return False

    epoch = block_number // EPOCH_LENGTH
    if is_test:
        cache_size, full_size = 1024, 32 * 1024
    else:
        cache_size, full_size = (
            get_cache_size(epoch),
            get_full_size(epoch),
        )

    cache = get_cache(cache_size, epoch)
    mining_output = hashimoto(block_number, full_size, cache, header_hash, nonce)
    if mining_output[b"mix digest"] != mixhash:
        return False
    result = int.from_bytes(mining_output[b"result"], byteorder="big")
    return result <= 2 ** 256 // (difficulty or 1)


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
    epoch = block_number // EPOCH_LENGTH
    if is_test:
        cache_size, full_size = 1024, 32 * 1024 
    else:
        cache_size, full_size = (
            get_cache_size(epoch),
            get_full_size(epoch),
        )

    cache = get_cache(cache_size, epoch)
    nonce = start_nonce
    target = (2 ** 256 // (difficulty or 1) - 1).to_bytes(32, byteorder="big")
    for i in range(1, rounds + 1):
        bin_nonce = (nonce + i).to_bytes(8, byteorder="big")
        o = hashimoto(block_number, full_size, cache, mining_hash, bin_nonce)
        if o[b"result"] <= target:
            assert len(bin_nonce) == 8
            assert len(o[b"mix digest"]) == 32
            return bin_nonce, o[b"mix digest"]
    return None, None
