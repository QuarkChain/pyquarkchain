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
cache_seeds = [b"\x00" * 32]  # type: List[bytes]

try:
    import pyethash as _pyethash_mod
    _pyethash_fn = _pyethash_mod.hashimoto_light  # pre-bound: avoids attr lookup per call
    ETHASH_LIB = "pyethash"
except ImportError:
    _pyethash_mod = None
    _pyethash_fn  = None
    ETHASH_LIB = "python"


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

def mkcache_pyethash(cache_size: int, epoch: int) -> np.ndarray:
    if cache_size != get_cache_size(epoch):
        return mkcache_python(cache_size, epoch)  # non-canonical size (e.g. is_test)

    arr, _ = _get_pyethash_cache(epoch)
    return arr

def mkcache_python(cache_size: int, epoch: int) -> np.ndarray:
    while len(cache_seeds) <= epoch:
        new_seed = ethash_sha3_256(cache_seeds[-1]).tobytes()
        cache_seeds.append(new_seed)

    seed = cache_seeds[epoch]
    return _get_cache(seed, cache_size // HASH_BYTES)


def hashimoto_light_python(
    full_size: int, cache: np.ndarray, header: bytes, nonce: bytes, block_number: int,
) -> Dict:
    return hashimoto(header, nonce, full_size, lambda x: calc_dataset_item(cache, x))

def hashimoto_light_pyethash(
    full_size: int, cache: np.ndarray, header: bytes, nonce: bytes, block_number: int,
) -> Dict:
    n = block_number // EPOCH_LENGTH
    if full_size != get_full_size(n):
        return hashimoto_light_python(full_size, cache, header, nonce, block_number)  # non-canonical size (e.g. is_test)

    _, raw = _get_pyethash_cache(n)
    nonce_int = int.from_bytes(nonce, byteorder="big")
    return _pyethash_fn(block_number, raw, header, nonce_int)


# Mutable impl pointers — updated by set_ethash_lib() at runtime.
# mkcache / hashimoto_light are stable wrapper functions so callers that do
# "from ethash import mkcache" keep working after a set_ethash_lib() call.
if ETHASH_LIB == "pyethash":
    _mkcache_impl         = mkcache_pyethash
    _hashimoto_light_impl = hashimoto_light_pyethash
else:
    _mkcache_impl         = mkcache_python
    _hashimoto_light_impl = hashimoto_light_python


def mkcache(cache_size: int, epoch: int) -> np.ndarray:
    return _mkcache_impl(cache_size, epoch)


def hashimoto_light(
    full_size: int, cache: np.ndarray, header: bytes, nonce: bytes, block_number: int,
) -> Dict:
    return _hashimoto_light_impl(full_size, cache, header, nonce, block_number)


def set_ethash_lib(lib_name: str) -> None:
    """Switch the active ethash implementation at runtime."""
    global ETHASH_LIB, _pyethash_mod, _pyethash_fn, _mkcache_impl, _hashimoto_light_impl
    if lib_name == "pyethash":
        import pyethash as _mod
        _pyethash_mod         = _mod
        _pyethash_fn          = _mod.hashimoto_light
        _mkcache_impl         = mkcache_pyethash
        _hashimoto_light_impl = hashimoto_light_pyethash
        ETHASH_LIB            = "pyethash"
        _get_pyethash_cache.cache_clear()
    elif lib_name == "python":
        _pyethash_mod         = None
        _pyethash_fn          = None
        _mkcache_impl         = mkcache_python
        _hashimoto_light_impl = hashimoto_light_python
        ETHASH_LIB            = "python"
        _get_cache.cache_clear()
    else:
        raise ValueError(f"Unknown lib_name={lib_name!r}. Use 'pyethash' or 'python'.")


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
