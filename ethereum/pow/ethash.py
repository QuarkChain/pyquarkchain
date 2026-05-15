import importlib
import os
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

# ---------------------------------------------------------------------------
# ETHASH_LIB selects the implementation used for PoW verification.
#   "ethash"    — pure-Python + numpy (always available)
#   "ethash_cy" — Cython + C keccak  (requires python setup.py build_ext)
#   "ethash_rs" — Rust + tiny-keccak (requires python setup.py build_ext)
#   "pyethash"  — pyethash C extension (requires: pip install git+https://github.com/QuarkChain/ethash.git#egg=pyethash)
# Default: auto-detect best available (pyethash → ethash_rs → ethash_cy → ethash)
# ---------------------------------------------------------------------------
_impl_hashimoto_light = None
_impl_mkcache = None
_mix_parents_fn = None
_pyethash_mod = None

ETHASH_LIB = os.environ.get("ETHASH_LIB", "auto")

if ETHASH_LIB == "auto":
    # Priority: pyethash (C extension) → ethash_rs → ethash_cy → pure-Python
    try:
        _mod = importlib.import_module("pyethash")
        if hasattr(_mod, "mkcache_bytes"):
            ETHASH_LIB = "pyethash"
    except ImportError:
        pass

    if ETHASH_LIB == "auto":
        # Check symbol presence to avoid false-positives from namespace packages
        # (e.g. the ethash_rs/ Cargo source directory looks like a package but has
        # no compiled symbols until the extension is built).
        _REQUIRED = {"ethash_rs": "rs_hashimoto_light", "ethash_cy": "cy_hashimoto_light"}
        for _candidate in ("ethash_rs", "ethash_cy"):
            try:
                _mod = importlib.import_module(f"ethereum.pow.{_candidate}")
                if hasattr(_mod, _REQUIRED[_candidate]):
                    ETHASH_LIB = _candidate
                    break
            except ImportError:
                continue

    if ETHASH_LIB == "auto":
        ETHASH_LIB = "ethash"

if ETHASH_LIB == "ethash_cy":
    from ethereum.pow.ethash_cy import cy_hashimoto_light as _impl_hashimoto_light
    from ethereum.pow.ethash_cy import cy_mkcache as _impl_mkcache
    from ethereum.pow.ethash_cy import mix_parents as _mix_parents_fn

elif ETHASH_LIB == "ethash_rs":
    from ethereum.pow.ethash_rs import rs_hashimoto_light as _impl_hashimoto_light
    from ethereum.pow.ethash_rs import rs_mkcache as _impl_mkcache
    from ethereum.pow.ethash_rs import mix_parents as _mix_parents_fn

elif ETHASH_LIB == "pyethash":
    import pyethash as _pyethash_mod

elif ETHASH_LIB != "ethash":
    raise ValueError(f"Unknown ETHASH_LIB={ETHASH_LIB!r}. "
                     f"Use 'ethash', 'ethash_cy', 'ethash_rs', 'pyethash', or 'auto'.")

print(f"[ethash] using implementation: {ETHASH_LIB}")


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
    if _impl_mkcache is not None:
        return _impl_mkcache(np.frombuffer(seed, dtype=np.uint8), n)
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
    if _pyethash_mod is not None and cache_size == get_cache_size(block_number):
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
    if _pyethash_mod is not None and full_size == get_full_size(block_number):
        n = block_number // EPOCH_LENGTH
        # Re-calling _get_pyethash_cache here is intentional: mkcache() already called
        # it to build arr, but hashimoto_light needs raw (bytes) for the C extension.
        # Since arr is a zero-copy view of raw and both are cached together, this second
        # call is an O(1) LRU hit with no extra allocation.
        _, raw = _get_pyethash_cache(n)
        nonce_int = int.from_bytes(nonce, byteorder="big")
        return _pyethash_mod.hashimoto_light(block_number, raw, header, nonce_int)
    if _impl_hashimoto_light is not None:
        return _impl_hashimoto_light(
            full_size, cache,
            np.frombuffer(header, dtype=np.uint8),
            np.frombuffer(nonce, dtype=np.uint8),
        )
    return hashimoto(header, nonce, full_size, lambda x: calc_dataset_item(cache, x))


def calc_dataset_item(cache: np.ndarray, i: int) -> np.ndarray:
    n = len(cache)
    mix = cache[i % n].copy()
    mix[0] ^= i
    mix = ethash_sha3_512(mix)
    if _mix_parents_fn is not None:
        _mix_parents_fn(mix, cache, i)
    else:
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
