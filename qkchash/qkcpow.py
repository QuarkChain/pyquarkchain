import os
from functools import lru_cache
from typing import Optional, Tuple

from qkchash.qkchash import CACHE_ENTRIES, make_cache, qkchash, QkcHashNative, get_seed_from_block_number


def get_qkchashlib_path():
    """Assuming libqkchash.so is in the same dir as this file"""
    return os.path.join(os.path.dirname(__file__), "libqkchash.so")


def init_qkc_hash_native():
    try:
        return QkcHashNative(get_qkchashlib_path())
    except Exception:
        return None


QKC_HASH_NATIVE = init_qkc_hash_native()



@lru_cache(maxsize=32)
def check_pow(
    block_number: int, header_hash: bytes, mixhash: bytes, nonce: bytes, difficulty: int
) -> bool:
    """Check if the proof-of-work of the block is valid."""
    if len(mixhash) != 32 or len(header_hash) != 32 or len(nonce) != 8:
        return False

    if QKC_HASH_NATIVE is None:
        seed = get_seed_from_block_number(block_number)
        current_cache = make_cache(CACHE_ENTRIES, seed)	
        mining_output = qkchash(header_hash, nonce, current_cache)
    else:
        current_cache = QKC_HASH_NATIVE.make_cache_block_number(CACHE_ENTRIES, block_number)
        mining_output = QKC_HASH_NATIVE.calculate_hash(header_hash, nonce, current_cache)

    if mining_output["mix digest"] != mixhash:
        return False
    result = int.from_bytes(mining_output["result"], byteorder="big")
    return result <= 2 ** 256 // (difficulty or 1)


class QkchashMiner:
    def __init__(self, block_number: int, difficulty: int, header_hash: bytes):
        self.block_number = block_number
        self.difficulty = difficulty
        self.header_hash = header_hash

    def mine(
        self, rounds=1000, start_nonce=0
    ) -> Tuple[Optional[bytes], Optional[bytes]]:
        bin_nonce, mixhash = mine(
            self.block_number, self.difficulty, self.header_hash, start_nonce=start_nonce, rounds=rounds
        )
        if bin_nonce is not None:
            return bin_nonce, mixhash

        return None, None


def mine(
    block_number: int, difficulty: int, header_hash: bytes, start_nonce: int = 0, rounds: int = 1000
) -> Tuple[Optional[bytes], Optional[bytes]]:
    nonce = start_nonce
    target = 2 ** 256 // (difficulty or 1)
    for i in range(1, rounds + 1):
        # hashimoto expected big-indian byte representation
        bin_nonce = (nonce + i).to_bytes(8, byteorder="big")

        if QKC_HASH_NATIVE is None:
            seed = get_seed_from_block_number(block_number)
            current_cache = make_cache(CACHE_ENTRIES, seed)	
            mining_output = qkchash(header_hash, bin_nonce, current_cache)
        else:
            current_cache = QKC_HASH_NATIVE.make_cache_block_number(CACHE_ENTRIES, block_number)
            mining_output = QKC_HASH_NATIVE.calculate_hash(
                header_hash, bin_nonce, current_cache
            )

        result = int.from_bytes(mining_output["result"], byteorder="big")
        if result <= target:
            assert len(bin_nonce) == 8
            assert len(mining_output["mix digest"]) == 32
            return bin_nonce, mining_output["mix digest"]
    return None, None
