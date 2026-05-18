import math
from functools import lru_cache
from typing import Union
from Crypto.Hash import keccak

import numpy as np


def _sha3_256(x):
    return keccak.new(digest_bits=256, data=x).digest()

def _sha3_512(x):
    return keccak.new(digest_bits=512, data=x).digest()


WORD_BYTES = 4  # bytes in word
DATASET_BYTES_INIT = 2 ** 30  # bytes in dataset at genesis
DATASET_BYTES_GROWTH = 2 ** 23  # growth per epoch (~7 GB per year)
CACHE_BYTES_INIT = 2 ** 24  # Size of the dataset relative to the cache
CACHE_BYTES_GROWTH = 2 ** 17  # Size of the dataset relative to the cache
EPOCH_LENGTH = 30000  # blocks per epoch
MIX_BYTES = 128  # width of mix
HASH_BYTES = 64  # hash length in bytes
DATASET_PARENTS = 256  # number of parents of each dataset element
CACHE_ROUNDS = 3  # number of rounds in cache production
ACCESSES = 64  # number of accesses in hashimoto loop

FNV_PRIME = 0x01000193


def ethash_sha3_512(x: Union[bytes, np.ndarray]) -> np.ndarray:
    """sha3-512: bytes or ndarray in, little-endian uint32 ndarray (16,) out."""
    if isinstance(x, np.ndarray):
        x = x.astype("<u4", copy=False).tobytes()
    return np.frombuffer(_sha3_512(x), dtype="<u4").copy()


def ethash_sha3_256(x: Union[bytes, np.ndarray]) -> np.ndarray:
    """sha3-256: bytes or ndarray in, little-endian uint32 ndarray (8,) out."""
    if isinstance(x, np.ndarray):
        x = x.astype("<u4", copy=False).tobytes()
    return np.frombuffer(_sha3_256(x), dtype="<u4").copy()


def isprime(x):
    if x < 2:
        return False
    for i in range(2, math.isqrt(x) + 1):
        if not x % i:
            return False
    return True


@lru_cache(maxsize=256)
def get_cache_size(epoch):
    sz = CACHE_BYTES_INIT + CACHE_BYTES_GROWTH * epoch
    sz -= HASH_BYTES
    while not isprime(sz // HASH_BYTES):
        sz -= 2 * HASH_BYTES
    return sz


@lru_cache(maxsize=256)
def get_full_size(epoch):
    sz = DATASET_BYTES_INIT + DATASET_BYTES_GROWTH * epoch
    sz -= MIX_BYTES
    while not isprime(sz // MIX_BYTES):
        sz -= 2 * MIX_BYTES
    return sz
