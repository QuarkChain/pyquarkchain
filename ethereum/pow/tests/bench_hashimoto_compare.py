"""
Benchmark hashimoto_light: old (hex-based) vs R1 (struct+list) vs R2 (numpy) vs R3 (numpy+Cython).

- old: original hex-based implementation
- R1:  struct.pack/unpack + Python list
- R2:  struct.pack/unpack + numpy ndarray
- R3:  R2 + Cython inner loop for calc_dataset_item (256-iter FNV mixing)

Uses is_test=True sizes (cache=1024B, dataset=32KB) for fast iteration.
Run with:
    PYTHONPATH=. python ethereum/pow/tests/bench_hashimoto_compare.py
"""

import copy
import struct
import time

import numpy as np
from eth_utils import encode_hex, decode_hex
from Crypto.Hash import keccak

# ---------------------------------------------------------------------------
# Shared keccak
# ---------------------------------------------------------------------------
def _sha3_256(x): return keccak.new(digest_bits=256, data=x).digest()
def _sha3_512(x): return keccak.new(digest_bits=512, data=x).digest()

WORD_BYTES = 4
HASH_BYTES = 64
MIX_BYTES = 128
ACCESSES = 64
DATASET_PARENTS = 256
CACHE_ROUNDS = 3
FNV_PRIME = 0x01000193

# ===========================================================================
# OLD — hex-based
# ===========================================================================
def _old_decode_int(s):
    return int(encode_hex(s[::-1]), 16) if s else 0

def _old_encode_int(s):
    a = "%x" % s
    return b"" if s == 0 else decode_hex("0" * (len(a) % 2) + a)[::-1]

def _old_serialize_hash(h):
    return b"".join([_old_encode_int(x).ljust(4, b"\x00") for x in h])

def _old_deserialize_hash(h):
    return [_old_decode_int(h[i:i + WORD_BYTES]) for i in range(0, len(h), WORD_BYTES)]

def _old_sha3_512(x):
    if isinstance(x, list): x = _old_serialize_hash(x)
    return _old_deserialize_hash(_sha3_512(x))

def _old_sha3_256(x):
    if isinstance(x, list): x = _old_serialize_hash(x)
    return _old_deserialize_hash(_sha3_256(x))

def _old_fnv(v1, v2):
    return (v1 * FNV_PRIME ^ v2) % 2 ** 32

def old_mkcache(cache_size, seed):
    n = cache_size // HASH_BYTES
    o = [_old_sha3_512(seed)]
    for i in range(1, n):
        o.append(_old_sha3_512(o[-1]))
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = o[i][0] % n
            o[i] = _old_sha3_512([a ^ b for a, b in zip(o[(i - 1 + n) % n], o[v])])
    return o

def old_calc_dataset_item(cache, i):
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES
    mix = copy.copy(cache[i % n])
    mix[0] ^= i
    mix = _old_sha3_512(mix)
    for j in range(DATASET_PARENTS):
        cache_index = _old_fnv(i ^ j, mix[j % r])
        mix = list(map(_old_fnv, mix, cache[cache_index % n]))
    return _old_sha3_512(mix)

def old_hashimoto_light(full_size, cache, header, nonce):
    n = full_size // HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES // HASH_BYTES
    s = _old_sha3_512(header + nonce[::-1])
    mix = []
    for _ in range(mixhashes):
        mix.extend(s)
    for i in range(ACCESSES):
        p = _old_fnv(i ^ s[0], mix[i % w]) % (n // mixhashes) * mixhashes
        newdata = []
        for j in range(mixhashes):
            newdata.extend(old_calc_dataset_item(cache, p + j))
        mix = list(map(_old_fnv, mix, newdata))
    cmix = []
    for i in range(0, len(mix), 4):
        cmix.append(_old_fnv(_old_fnv(_old_fnv(mix[i], mix[i+1]), mix[i+2]), mix[i+3]))
    return {
        b"mix digest": _old_serialize_hash(cmix),
        b"result": _old_serialize_hash(_old_sha3_256(s + cmix)),
    }

# ===========================================================================
# Round 1 — struct+list
# ===========================================================================
_FMT_16I = struct.Struct("<16I")
_FMT_8I  = struct.Struct("<8I")
_FMT_32I = struct.Struct("<32I")
_MID_FNV_PRIME = FNV_PRIME

def _r1_serialize(h):
    n = len(h)
    if n == 16: return _FMT_16I.pack(*h)
    if n == 8:  return _FMT_8I.pack(*h)
    if n == 32: return _FMT_32I.pack(*h)
    return struct.pack("<%dI" % n, *h)

def _r1_deserialize(h):
    n = len(h)
    if n == 64:  return list(_FMT_16I.unpack(h))
    if n == 32:  return list(_FMT_8I.unpack(h))
    if n == 128: return list(_FMT_32I.unpack(h))
    return list(struct.unpack("<%dI" % (n // 4), h))

def _r1_sha3_512(x):
    if isinstance(x, list): x = _r1_serialize(x)
    return list(_FMT_16I.unpack(_sha3_512(x)))

def _r1_sha3_256(x):
    if isinstance(x, list): x = _r1_serialize(x)
    return list(_FMT_8I.unpack(_sha3_256(x)))

def _r1_fnv(v1, v2):
    return (v1 * _MID_FNV_PRIME ^ v2) & 0xFFFFFFFF

def r1_mkcache(cache_size, seed):
    n = cache_size // HASH_BYTES
    o = [_r1_sha3_512(seed)]
    for i in range(1, n):
        o.append(_r1_sha3_512(o[-1]))
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = o[i][0] % n
            o[i] = _r1_sha3_512([a ^ b for a, b in zip(o[(i - 1 + n) % n], o[v])])
    return o

def r1_calc_dataset_item(cache, i):
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES
    mix = copy.copy(cache[i % n])
    mix[0] ^= i
    mix = _r1_sha3_512(mix)
    for j in range(DATASET_PARENTS):
        cache_index = _r1_fnv(i ^ j, mix[j % r])
        mix = list(map(_r1_fnv, mix, cache[cache_index % n]))
    return _r1_sha3_512(mix)

def r1_hashimoto_light(full_size, cache, header, nonce):
    n = full_size // HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES // HASH_BYTES
    s = _r1_sha3_512(header + nonce[::-1])
    mix = list(s) * mixhashes
    for i in range(ACCESSES):
        p = _r1_fnv(i ^ s[0], mix[i % w]) % (n // mixhashes) * mixhashes
        newdata = []
        for j in range(mixhashes):
            newdata.extend(r1_calc_dataset_item(cache, p + j))
        mix = list(map(_r1_fnv, mix, newdata))
    cmix = []
    for i in range(0, len(mix), 4):
        cmix.append(_r1_fnv(_r1_fnv(_r1_fnv(mix[i], mix[i+1]), mix[i+2]), mix[i+3]))
    return {
        b"mix digest": _r1_serialize(cmix),
        b"result": _r1_serialize(_r1_sha3_256(s + cmix)),
    }

# ===========================================================================
# Round 2 — numpy ndarray (current implementation)
# ===========================================================================
np.seterr(over="ignore")

from ethereum.pow.ethash import (
    mkcache as r2_mkcache,
    hashimoto_light as r2_hashimoto_light,
    _cy_mix_parents,
)
from ethereum.pow.ethash_utils import ethash_sha3_512 as _r2_sha3_512

_R2_FNV_PRIME = np.uint32(FNV_PRIME)


def r2_calc_dataset_item(cache, i):
    """R2: numpy inner loop (pure Python, no Cython)."""
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES
    mix = cache[i % n].copy()
    mix[0] ^= i
    mix = _r2_sha3_512(mix)
    for j in range(DATASET_PARENTS):
        cache_index = ((i ^ j) * FNV_PRIME ^ int(mix[j % r])) & 0xFFFFFFFF
        mix *= _R2_FNV_PRIME
        mix ^= cache[cache_index % n]
    return _r2_sha3_512(mix)


# ===========================================================================
# Round 3 — numpy + Cython inner loop
# ===========================================================================
_has_cython = _cy_mix_parents is not None


def r3_calc_dataset_item(cache, i):
    """R3: Cython inner loop."""
    n = len(cache)
    mix = cache[i % n].copy()
    mix[0] ^= i
    mix = _r2_sha3_512(mix)
    _cy_mix_parents(mix, cache, i)
    return _r2_sha3_512(mix)

# ===========================================================================
# Main
# ===========================================================================
if __name__ == "__main__":
    CACHE_SIZE = 1024
    FULL_SIZE  = 32 * 1024
    SEED   = b"\x00" * 32
    HEADER = bytes.fromhex("c9149cc0386e689d789a1c2f3d5d169a61a6218ed30e74414dc736e442ef3d1f")
    NONCE  = (0).to_bytes(8, byteorder="big")

    # ---- build caches ----
    print("Building caches...")
    t0 = time.perf_counter(); old_cache = old_mkcache(CACHE_SIZE, SEED); t_oc = time.perf_counter() - t0
    t0 = time.perf_counter(); r1_cache = r1_mkcache(CACHE_SIZE, SEED); t_mc = time.perf_counter() - t0
    t0 = time.perf_counter(); r2_cache = r2_mkcache(CACHE_SIZE, 0);    t_nc = time.perf_counter() - t0
    print(f"  mkcache  old={t_oc*1000:.1f}ms  R1={t_mc*1000:.1f}ms  R2={t_nc*1000:.1f}ms  "
          f"old/R1={t_oc/t_mc:.1f}x  old/R2={t_oc/t_nc:.1f}x")

    # ---- correctness ----
    old_r = old_hashimoto_light(FULL_SIZE, old_cache, HEADER, NONCE)
    mid_r = r1_hashimoto_light(FULL_SIZE, r1_cache, HEADER, NONCE)
    new_r = r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
    assert old_r == mid_r, "old/R1 MISMATCH"
    assert old_r == new_r, "old/R2 MISMATCH"

    if _has_cython:
        # R3 uses the same cache as R2 (numpy ndarray)
        for i in range(16):
            r2_item = r2_calc_dataset_item(r2_cache, i)
            r3_item = r3_calc_dataset_item(r2_cache, i)
            assert np.array_equal(r2_item, r3_item), f"R2/R3 mismatch at item {i}"
    cython_tag = "OK" if _has_cython else "SKIP (not built)"
    print(f"  result   match=OK  R3={cython_tag}  mix={old_r[b'mix digest'].hex()[:16]}...\n")

    # ---- calc_dataset_item breakdown ----
    N2 = 300
    print(f"calc_dataset_item  x{N2} calls")

    t0 = time.perf_counter()
    for i in range(N2): old_calc_dataset_item(old_cache, i)
    t_old_i = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N2): r1_calc_dataset_item(r1_cache, i)
    t_mid_i = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N2): r2_calc_dataset_item(r2_cache, i)
    t_r2_i = time.perf_counter() - t0

    print(f"  old  {t_old_i:.3f}s  {t_old_i/N2*1000:.2f}ms/call")
    print(f"  R1   {t_mid_i:.3f}s  {t_mid_i/N2*1000:.2f}ms/call  old/R1={t_old_i/t_mid_i:.2f}x")
    print(f"  R2   {t_r2_i:.3f}s  {t_r2_i/N2*1000:.2f}ms/call  old/R2={t_old_i/t_r2_i:.2f}x", end="")
    if _has_cython:
        t0 = time.perf_counter()
        for i in range(N2): r3_calc_dataset_item(r2_cache, i)
        t_r3_i = time.perf_counter() - t0
        print(f"\n  R3   {t_r3_i:.3f}s  {t_r3_i/N2*1000:.2f}ms/call  old/R3={t_old_i/t_r3_i:.2f}x  R2/R3={t_r2_i/t_r3_i:.1f}x")
    else:
        print("\n  R3   (skipped — Cython extension not built)")

    # ---- hashimoto_light benchmark ----
    N = 30
    print(f"\nhashimoto_light  x{N} calls  (cache=1KB, dataset=32KB)")

    for _ in range(2):
        old_hashimoto_light(FULL_SIZE, old_cache, HEADER, NONCE)
        r1_hashimoto_light(FULL_SIZE, r1_cache, HEADER, NONCE)
        r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)

    t0 = time.perf_counter()
    for i in range(N): old_hashimoto_light(FULL_SIZE, old_cache, HEADER, i.to_bytes(8, "big"))
    t_old = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N): r1_hashimoto_light(FULL_SIZE, r1_cache, HEADER, i.to_bytes(8, "big"))
    t_mid = time.perf_counter() - t0

    # R2 without Cython: temporarily disable
    import ethereum.pow.ethash as _ethmod
    _save = _ethmod._cy_mix_parents
    _ethmod._cy_mix_parents = None
    for _ in range(2):
        r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
    t0 = time.perf_counter()
    for i in range(N): r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, i.to_bytes(8, "big"))
    t_r2 = time.perf_counter() - t0
    _ethmod._cy_mix_parents = _save

    print(f"  old  {t_old:.3f}s  {t_old/N*1000:.1f}ms/call")
    print(f"  R1   {t_mid:.3f}s  {t_mid/N*1000:.1f}ms/call  old/R1={t_old/t_mid:.2f}x")
    print(f"  R2   {t_r2:.3f}s  {t_r2/N*1000:.1f}ms/call  old/R2={t_old/t_r2:.2f}x", end="")
    if _has_cython:
        # R3: hashimoto_light with Cython (already the default import path)
        for _ in range(2):
            r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
        t0 = time.perf_counter()
        for i in range(N): r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, i.to_bytes(8, "big"))
        t_r3 = time.perf_counter() - t0
        print(f"\n  R3   {t_r3:.3f}s  {t_r3/N*1000:.1f}ms/call  old/R3={t_old/t_r3:.2f}x  R2/R3={t_r2/t_r3:.1f}x")
    else:
        print("\n  R3   (skipped — Cython extension not built)")
