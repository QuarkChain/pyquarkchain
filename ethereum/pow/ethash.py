import importlib
import os
import numpy as np
from functools import lru_cache
from typing import Callable, Dict, List

from ethereum.pow.ethash_utils import (
    ethash_sha3_512, ethash_sha3_256,
    FNV_PRIME, HASH_BYTES, WORD_BYTES, MIX_BYTES,
    DATASET_PARENTS, CACHE_ROUNDS, ACCESSES, EPOCH_LENGTH,
)

_FNV_PRIME = np.uint32(FNV_PRIME)

# ---------------------------------------------------------------------------
# ETHASH_LIB selects the implementation used for PoW verification.
#   "ethash"    — pure-Python + numpy (always available)
#   "ethash_cy" — Cython + C keccak  (requires python setup.py build_ext)
#   "ethash_rs" — Rust + tiny-keccak (requires python setup.py build_ext)
# Default: auto-detect best available (ethash_rs → ethash_cy → ethash)
# ---------------------------------------------------------------------------
_impl_hashimoto_light = None
_impl_mkcache = None
_mix_parents_fn = None

ETHASH_LIB = os.environ.get("ETHASH_LIB", "auto")

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
    else:
        ETHASH_LIB = "ethash"

if ETHASH_LIB == "ethash_cy":
    from ethereum.pow.ethash_cy import cy_hashimoto_light as _impl_hashimoto_light
    from ethereum.pow.ethash_cy import cy_mkcache as _impl_mkcache
    from ethereum.pow.ethash_cy import mix_parents as _mix_parents_fn

elif ETHASH_LIB == "ethash_rs":
    from ethereum.pow.ethash_rs import rs_hashimoto_light as _impl_hashimoto_light
    from ethereum.pow.ethash_rs import rs_mkcache as _impl_mkcache
    from ethereum.pow.ethash_rs import mix_parents as _mix_parents_fn

elif ETHASH_LIB != "ethash":
    raise ValueError(f"Unknown ETHASH_LIB={ETHASH_LIB!r}. "
                     f"Use 'ethash', 'ethash_cy', 'ethash_rs', or 'auto'.")

print(f"[ethash] using implementation: {ETHASH_LIB}")


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
    while len(cache_seeds) <= block_number // EPOCH_LENGTH:
        new_seed = ethash_sha3_256(cache_seeds[-1]).tobytes()
        cache_seeds.append(new_seed)

    seed = cache_seeds[block_number // EPOCH_LENGTH]
    return _get_cache(seed, cache_size // HASH_BYTES)


def hashimoto_light(
    full_size: int, cache: np.ndarray, header: bytes, nonce: bytes
) -> Dict:
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

    s_cmix = np.concatenate([s, cmix])
    return {
        b"mix digest": cmix.tobytes(),
        b"result": ethash_sha3_256(s_cmix).tobytes(),
    }


def hashimoto_full(dataset: np.ndarray, header: bytes, nonce: bytes) -> Dict:
    return hashimoto(header, nonce, len(dataset) * HASH_BYTES, lambda x: dataset[x])
