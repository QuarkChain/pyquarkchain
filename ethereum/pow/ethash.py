import numpy as np
from functools import lru_cache
from typing import Callable, Dict, List

from ethereum.pow.ethash_utils import (
    ethash_sha3_512, ethash_sha3_256,
    FNV_PRIME, HASH_BYTES, WORD_BYTES, MIX_BYTES,
    DATASET_PARENTS, CACHE_ROUNDS, ACCESSES, EPOCH_LENGTH,
    get_cache_size, get_full_size,
)

_FNV_PRIME = np.uint32(FNV_PRIME)

try:
    import pyethash as _pyethash_mod
    ETHASH_LIB = "pyethash"
except ImportError:
    _pyethash_mod = None
    ETHASH_LIB = "python"


def set_ethash_lib(lib_name: str) -> None:
    """Switch the active ethash implementation at runtime. Clears LRU caches."""
    global ETHASH_LIB, _pyethash_mod
    if lib_name == "pyethash":
        import pyethash as _mod
        _pyethash_mod = _mod
        ETHASH_LIB = "pyethash"
    elif lib_name == "python":
        _pyethash_mod = None
        ETHASH_LIB = "python"
    else:
        raise ValueError(f"Unknown lib_name={lib_name!r}. Use 'pyethash' or 'python'.")
    _get_pyethash_cache.cache_clear()
    _get_cache.cache_clear()


@lru_cache(10)
def _get_pyethash_cache(epoch: int):
    """Returns (ndarray, raw_bytes) for the given epoch, both LRU-cached together.

    arr is a zero-copy view of raw (np.frombuffer without .copy()), so both share
    the same ~16 MB buffer. mkcache() uses arr for the numpy path; hashimoto_light()
    uses raw for the pyethash C-extension path. Calling _get_pyethash_cache() a second
    time within the same request is an O(1) LRU hit — no recomputation or extra copy.
    """
    raw = _pyethash_mod.mkcache_bytes(epoch * EPOCH_LENGTH)
    n = len(raw) // HASH_BYTES
    # No .copy(): arr is a read-only view into raw, keeping total memory at ~16 MB
    # per epoch instead of ~32 MB. Safe because calc_dataset_item always copies a
    # row before mutating it, so the cache array itself is never modified in-place.
    arr = np.frombuffer(raw, dtype="<u4").reshape(n, 16)
    return arr, raw


cache_seeds = [b"\x00" * 32]  # type: List[bytes]


@lru_cache(10)
def _get_cache(seed: bytes, n: int) -> np.ndarray:
    """Returns cache as uint32 ndarray of shape (n, 16)."""
    o = np.empty((n, 16), dtype=np.uint32)
    o[0] = ethash_sha3_512(seed)
    for i in range(1, n):
        o[i] = ethash_sha3_512(o[i - 1])
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = int(o[i, 0]) % n
            xored = o[(i - 1 + n) % n] ^ o[v]
            o[i] = ethash_sha3_512(xored)
    return o


def mkcache(cache_size: int, block_number) -> np.ndarray:
    n = block_number // EPOCH_LENGTH
    # pyethash always returns the canonical epoch cache size; fall back to pure
    # Python when a non-standard size is requested (e.g. is_test=True uses 1 KB).
    if ETHASH_LIB == "pyethash" and cache_size == get_cache_size(block_number):
        arr, _ = _get_pyethash_cache(n)
        return arr

    while len(cache_seeds) <= n:
        new_seed = ethash_sha3_256(cache_seeds[-1]).tobytes()
        cache_seeds.append(new_seed)

    seed = cache_seeds[n]
    return _get_cache(seed, cache_size // HASH_BYTES)


def hashimoto_light(
    full_size: int, cache: np.ndarray, header: bytes, nonce: bytes, block_number: int,
) -> Dict:
    # pyethash ignores full_size and derives it from block_number internally.
    # Only use it when full_size matches the canonical epoch dataset size to
    # avoid silently wrong results in is_test=True paths (32 KB dataset).
    if ETHASH_LIB == "pyethash" and full_size == get_full_size(block_number):
        n = block_number // EPOCH_LENGTH
        # Re-calling _get_pyethash_cache here is intentional: mkcache() already called
        # it to build arr, but hashimoto_light needs raw (bytes) for the C extension.
        # Since arr is a zero-copy view of raw and both are cached together, this second
        # call is an O(1) LRU hit with no extra allocation.
        _, raw = _get_pyethash_cache(n)
        nonce_int = int.from_bytes(nonce, byteorder="big")
        return _pyethash_mod.hashimoto_light(block_number, raw, header, nonce_int)
    return hashimoto(header, nonce, full_size, lambda x: calc_dataset_item(cache, x))


def calc_dataset_item(cache: np.ndarray, i: int) -> np.ndarray:
    n = len(cache)
    mix = cache[i % n].copy()
    mix[0] ^= i
    mix = ethash_sha3_512(mix)
    r = HASH_BYTES // WORD_BYTES   # 16
    # uint32 overflow is intentional in FNV arithmetic
    with np.errstate(over="ignore"):
        for j in range(DATASET_PARENTS):
            cache_index = ((i ^ j) * FNV_PRIME ^ int(mix[j % r])) & 0xFFFFFFFF
            mix *= _FNV_PRIME
            mix ^= cache[cache_index % n]
    return ethash_sha3_512(mix)


def calc_dataset(full_size, cache: np.ndarray) -> np.ndarray:
    rows = full_size // HASH_BYTES
    out = np.empty((rows, 16), dtype=np.uint32)
    for i in range(rows):
        out[i] = calc_dataset_item(cache, i)
    return out


def hashimoto(
    header: bytes,
    nonce: bytes,
    full_size: int,
    dataset_lookup: Callable[[int], np.ndarray],
) -> Dict:
    n = full_size // HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES // HASH_BYTES

    s = ethash_sha3_512(header + nonce[::-1])
    mix = np.tile(s, mixhashes)
    s0 = int(s[0])
    newdata = np.empty(w, dtype=np.uint32)

    # uint32 overflow is intentional in FNV arithmetic
    with np.errstate(over="ignore"):
        for i in range(ACCESSES):
            p = ((i ^ s0) * FNV_PRIME ^ int(mix[i % w])) & 0xFFFFFFFF
            p = p % (n // mixhashes) * mixhashes
            for j in range(mixhashes):
                newdata[j * 16:(j + 1) * 16] = dataset_lookup(p + j)
            mix *= _FNV_PRIME
            mix ^= newdata

        mix_r = mix.reshape(-1, 4)
        cmix = mix_r[:, 0] * _FNV_PRIME ^ mix_r[:, 1]
        cmix = cmix * _FNV_PRIME ^ mix_r[:, 2]
        cmix = cmix * _FNV_PRIME ^ mix_r[:, 3]

    cmix = cmix.astype("<u4", copy=False)
    s_cmix = np.concatenate([s, cmix])
    return {
        b"mix digest": cmix.tobytes(),
        b"result": ethash_sha3_256(s_cmix).tobytes(),
    }


def hashimoto_full(dataset: np.ndarray, header: bytes, nonce: bytes) -> Dict:
    return hashimoto(header, nonce, len(dataset) * HASH_BYTES, lambda x: dataset[x])
