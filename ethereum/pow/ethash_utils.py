import struct
from typing import List, Union

import numpy as np

try:
    from Crypto.Hash import keccak

    def _sha3_256(x):
        return keccak.new(digest_bits=256, data=x).digest()

    def _sha3_512(x):
        return keccak.new(digest_bits=512, data=x).digest()


except Exception:
    import sha3 as _sha3

    def _sha3_256(x):
        return _sha3.sha3_256(x).digest()

    def _sha3_512(x):
        return _sha3.sha3_512(x).digest()


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

# Pre-computed struct formats for common sizes
_FMT_16I = struct.Struct("<16I")  # 64 bytes = 16 uint32 (HASH_BYTES)
_FMT_8I = struct.Struct("<8I")   # 32 bytes = 8 uint32
_FMT_32I = struct.Struct("<32I") # 128 bytes = 32 uint32 (MIX_BYTES)


def fnv(v1, v2):
    return (v1 * FNV_PRIME ^ v2) & 0xFFFFFFFF


def serialize_hash(h: List[int]) -> bytes:
    n = len(h)
    if n == 16:
        return _FMT_16I.pack(*h)
    if n == 8:
        return _FMT_8I.pack(*h)
    if n == 32:
        return _FMT_32I.pack(*h)
    return struct.pack("<%dI" % n, *h)


def deserialize_hash(h: bytes) -> List[int]:
    n = len(h)
    if n == 64:
        return list(_FMT_16I.unpack(h))
    if n == 32:
        return list(_FMT_8I.unpack(h))
    if n == 128:
        return list(_FMT_32I.unpack(h))
    return list(struct.unpack("<%dI" % (n // 4), h))


def hash_words(h, sz, x) -> List[int]:
    if isinstance(x, list):
        x = serialize_hash(x)
    y = h(x)
    return deserialize_hash(y)


def xor(a, b):
    return a ^ b


# sha3 hash function, outputs 64 bytes
def ethash_sha3_512(x: Union[bytes, List[int]]) -> List[int]:
    if isinstance(x, list):
        x = serialize_hash(x)
    return list(_FMT_16I.unpack(_sha3_512(x)))


def ethash_sha3_256(x: Union[bytes, List[int]]) -> List[int]:
    if isinstance(x, list):
        x = serialize_hash(x)
    return list(_FMT_8I.unpack(_sha3_256(x)))


# numpy variants: accept bytes or ndarray, return uint32 ndarray
def ethash_sha3_512_np(x: Union[bytes, np.ndarray]) -> np.ndarray:
    if isinstance(x, np.ndarray):
        x = x.tobytes()
    return np.frombuffer(_sha3_512(x), dtype=np.uint32).copy()


def ethash_sha3_256_np(x: Union[bytes, np.ndarray]) -> np.ndarray:
    if isinstance(x, np.ndarray):
        x = x.tobytes()
    return np.frombuffer(_sha3_256(x), dtype=np.uint32).copy()


# Works for dataset and cache
def serialize_cache(ds):
    return b"".join([serialize_hash(h) for h in ds])


serialize_dataset = serialize_cache


def deserialize_cache(ds):
    return [
        deserialize_hash(ds[i : i + HASH_BYTES]) for i in range(0, len(ds), HASH_BYTES)
    ]


deserialize_dataset = deserialize_cache


def isprime(x):
    for i in range(2, int(x ** 0.5)):
        if not x % i:
            return False
    return True


def get_cache_size(block_number):
    sz = CACHE_BYTES_INIT + CACHE_BYTES_GROWTH * (block_number // EPOCH_LENGTH)
    sz -= HASH_BYTES
    while not isprime(sz // HASH_BYTES):
        sz -= 2 * HASH_BYTES
    return sz


def get_full_size(block_number):
    sz = DATASET_BYTES_INIT + DATASET_BYTES_GROWTH * (block_number // EPOCH_LENGTH)
    sz -= MIX_BYTES
    while not isprime(sz // MIX_BYTES):
        sz -= 2 * MIX_BYTES
    return sz
