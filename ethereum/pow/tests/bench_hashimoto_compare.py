"""
Benchmark suite: old (hex-based) vs R1 (struct+list) vs R2 (numpy) vs R3 (numpy+Cython) vs R4 (full Cython).

- old: original hex-based implementation
- R1:  struct.pack/unpack + Python list
- R2:  struct.pack/unpack + numpy ndarray
- R3:  R2 + Cython inner loop for calc_dataset_item (256-iter FNV mixing)
- R4:  full Cython + C keccak (no Python overhead in hot path)

Sections:
  1. mkcache build time
  2. Correctness assertions
  3. Primitive micro-benchmarks (serialize/fnv/sha3)
  4. calc_dataset_item throughput
  5. hashimoto_light throughput
  6. check_pow end-to-end

Uses is_test=True sizes (cache=1024B, dataset=32KB) for fast iteration.
Run with:
    PYTHONPATH=. python -m ethereum.pow.tests.bench_hashimoto_compare
"""
import copy
import struct
import time

import numpy as np
from Crypto.Hash import keccak

from . import old_ethash

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
# OLD — hex-based (imported from old_ethash.py, which is a copy of the original ethash.py)
# ===========================================================================
old_mkcache           = old_ethash.mkcache
old_calc_dataset_item = old_ethash.calc_dataset_item
old_hashimoto_light   = old_ethash.hashimoto_light
_old_fnv              = old_ethash.fnv
_old_serialize_hash   = old_ethash.serialize_hash
_old_deserialize_hash = old_ethash.deserialize_hash
_old_sha3_512         = old_ethash.sha3_512
_old_sha3_256         = old_ethash.sha3_256

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
    hashimoto as _r2_hashimoto,
)
from ethereum.pow.ethash_utils import ethash_sha3_512 as _r2_sha3_512

_R2_FNV_PRIME = np.uint32(FNV_PRIME)


def r2_calc_dataset_item(cache: np.ndarray, i: int) -> np.ndarray:
    """R2: pure-Python numpy calc_dataset_item."""
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES   # 16
    mix = cache[i % n].copy()
    mix[0] ^= i
    mix = _r2_sha3_512(mix)
    for j in range(DATASET_PARENTS):
        cache_index = ((i ^ j) * FNV_PRIME ^ int(mix[j % r])) & 0xFFFFFFFF
        mix *= _R2_FNV_PRIME
        mix ^= cache[cache_index % n]
    return _r2_sha3_512(mix)

def r2_hashimoto_light(full_size, cache, header, nonce):
    """R2: pure-Python hashimoto_light (numpy + pycryptodome keccak)."""
    return _r2_hashimoto(header, nonce, full_size, lambda x: r2_calc_dataset_item(cache, x))


# ===========================================================================
# Round 3 — numpy + Cython mix_parents
# ===========================================================================
try:
    from ethereum.pow.ethash_cy import mix_parents as _cy_mix_parents
    _has_cython = True
except ImportError:
    _has_cython = False


def r3_calc_dataset_item(cache, i):
    """R3: Cython inner loop (mix_parents only, Python sha3)."""
    n = len(cache)
    mix = cache[i % n].copy()
    mix[0] ^= i
    mix = _r2_sha3_512(mix)
    _cy_mix_parents(mix, cache, i)
    return _r2_sha3_512(mix)


def r3_hashimoto_light(full_size, cache, header, nonce):
    """R3: hashimoto using r3_calc_dataset_item (Cython mix_parents + Python sha3)."""
    return _r2_hashimoto(header, nonce, full_size, lambda x: r3_calc_dataset_item(cache, x))


# ===========================================================================
# Round 4 — full Cython + C keccak (no Python overhead in hot path)
# ===========================================================================
try:
    from ethereum.pow.ethash_cy import (
        cy_calc_dataset_item as r4_calc_dataset_item,
        cy_hashimoto_light as _r4_hashimoto_light_raw,
    )
    _has_r4 = True
except ImportError:
    _has_r4 = False


def r4_hashimoto_light(full_size, cache, header, nonce):
    """R4: full Cython hashimoto_light. Adapts bytes args to uint8 arrays."""
    return _r4_hashimoto_light_raw(
        full_size, cache,
        np.frombuffer(header, dtype=np.uint8),
        np.frombuffer(nonce, dtype=np.uint8),
    )


# ===========================================================================
# Round 5 — Rust + tiny-keccak (ethash_rs)
# ===========================================================================
try:
    from ethereum.pow.ethash_rs import (
        rs_calc_dataset_item as r5_calc_dataset_item,
        rs_hashimoto_light as _r5_hashimoto_light_raw,
        rs_mkcache as _r5_mkcache_raw,
    )
    _has_r5 = True
except ImportError:
    _has_r5 = False


def r5_mkcache(cache_size, block_number):
    """R5: Rust mkcache."""
    from ethereum.pow.ethash import mkcache as _r2_mkcache
    # Use the same seed derivation as the Python path, then delegate to Rust.
    # rs_mkcache takes (seed: uint8[32], n: int).
    from ethereum.pow.ethash import cache_seeds
    from ethereum.pow.ethash_utils import EPOCH_LENGTH, HASH_BYTES
    from ethereum.pow.ethash_utils import ethash_sha3_256
    while len(cache_seeds) <= block_number // EPOCH_LENGTH:
        new_seed = ethash_sha3_256(cache_seeds[-1]).tobytes()
        cache_seeds.append(new_seed)
    seed = cache_seeds[block_number // EPOCH_LENGTH]
    n = cache_size // HASH_BYTES
    return _r5_mkcache_raw(np.frombuffer(seed, dtype=np.uint8), n)


def r5_hashimoto_light(full_size, cache, header, nonce):
    """R5: Rust hashimoto_light. Adapts bytes args to uint8 arrays."""
    return _r5_hashimoto_light_raw(
        full_size, cache,
        np.frombuffer(header, dtype=np.uint8),
        np.frombuffer(nonce, dtype=np.uint8),
    )

# ===========================================================================
# Micro-benchmark helpers
# ===========================================================================
def _bench(func, args, rounds=200_000):
    for _ in range(1000):
        func(*args)
    t0 = time.perf_counter()
    for _ in range(rounds):
        func(*args)
    return time.perf_counter() - t0

def _row3(label, fns_and_args, N):
    times = [_bench(fn, args, N) for fn, args in fns_and_args]
    t0 = times[0]
    cols = "".join(f"{t:>10.4f}" for t in times)
    ratios = "".join(f"{t0/t:>8.1f}x" for t in times[1:])
    print(f"{label:<30}{cols}{ratios}")

def _row_partial(label, fns_and_args, N):
    times = [_bench(fn, args, N) if fn is not None else None
             for fn, args in fns_and_args]
    t0 = times[0]
    cols = "".join(f"{t:>10.4f}" if t is not None else f"{'N/A':>10}" for t in times)
    ratios = "".join(
        f"{t0/t:>8.1f}x" if t is not None else f"{'N/A':>8}"
        for t in times[1:]
    )
    print(f"{label:<30}{cols}{ratios}")

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
    r5_cache = None
    if _has_r5:
        t0 = time.perf_counter(); r5_cache = r5_mkcache(CACHE_SIZE, 0); t_r5c = time.perf_counter() - t0
        print(f"  mkcache  old={t_oc*1000:.1f}ms  R1={t_mc*1000:.1f}ms  R2={t_nc*1000:.1f}ms  R5={t_r5c*1000:.1f}ms  "
              f"old/R1={t_oc/t_mc:.1f}x  old/R2={t_oc/t_nc:.1f}x  old/R5={t_oc/t_r5c:.1f}x")
    else:
        print(f"  mkcache  old={t_oc*1000:.1f}ms  R1={t_mc*1000:.1f}ms  R2={t_nc*1000:.1f}ms  "
              f"old/R1={t_oc/t_mc:.1f}x  old/R2={t_oc/t_nc:.1f}x")

    # ---- correctness ----
    old_r = old_hashimoto_light(FULL_SIZE, old_cache, HEADER, NONCE)
    mid_r = r1_hashimoto_light(FULL_SIZE, r1_cache, HEADER, NONCE)
    new_r = r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
    assert old_r == mid_r, "old/R1 MISMATCH"
    assert old_r == new_r, "old/R2 MISMATCH"

    if _has_cython:
        for i in range(16):
            r2_item = r2_calc_dataset_item(r2_cache, i)
            r3_item = r3_calc_dataset_item(r2_cache, i)
            assert np.array_equal(r2_item, r3_item), f"R2/R3 mismatch at item {i}"
    if _has_r4:
        for i in range(16):
            r2_item = r2_calc_dataset_item(r2_cache, i)
            r4_item = r4_calc_dataset_item(r2_cache, i)
            assert np.array_equal(r2_item, r4_item), f"R2/R4 mismatch at item {i}"
        r4_r = r4_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
        assert old_r == r4_r, "old/R4 hashimoto MISMATCH"
    if _has_r5:
        for i in range(16):
            r2_item = r2_calc_dataset_item(r2_cache, i)
            r5_item = r5_calc_dataset_item(r5_cache, i)
            assert np.array_equal(r2_item, r5_item), f"R2/R5 mismatch at item {i}"
        r5_r = r5_hashimoto_light(FULL_SIZE, r5_cache, HEADER, NONCE)
        assert old_r == r5_r, "old/R5 hashimoto MISMATCH"
    cy_tag = "OK" if _has_cython else "SKIP"
    r4_tag = "OK" if _has_r4 else "SKIP"
    r5_tag = "OK" if _has_r5 else "SKIP"
    print(f"  result   match=OK  R3={cy_tag}  R4={r4_tag}  R5={r5_tag}  mix={old_r[b'mix digest'].hex()[:16]}...\n")

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
        print(f"\n  R3   {t_r3_i:.3f}s  {t_r3_i/N2*1000:.2f}ms/call  old/R3={t_old_i/t_r3_i:.2f}x  R2/R3={t_r2_i/t_r3_i:.1f}x", end="")
    else:
        print("\n  R3   (skipped — Cython extension not built)", end="")
    if _has_r4:
        t0 = time.perf_counter()
        for i in range(N2): r4_calc_dataset_item(r2_cache, i)
        t_r4_i = time.perf_counter() - t0
        print(f"\n  R4   {t_r4_i:.3f}s  {t_r4_i/N2*1000:.2f}ms/call  old/R4={t_old_i/t_r4_i:.2f}x  R3/R4={t_r3_i/t_r4_i:.1f}x", end="")
    else:
        print("\n  R4   (skipped — Cython R4 not built)", end="")
    if _has_r5:
        t0 = time.perf_counter()
        for i in range(N2): r5_calc_dataset_item(r5_cache, i)
        t_r5_i = time.perf_counter() - t0
        prev_i = t_r4_i if _has_r4 else t_r2_i
        prev_label = "R4" if _has_r4 else "R2"
        print(f"\n  R5   {t_r5_i:.3f}s  {t_r5_i/N2*1000:.2f}ms/call  old/R5={t_old_i/t_r5_i:.2f}x  {prev_label}/R5={prev_i/t_r5_i:.1f}x")
    else:
        print("\n  R5   (skipped — Rust extension not built)")

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

    # R2: pure Python hashimoto_light (always the _slow variant)
    for _ in range(2):
        r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
    t0 = time.perf_counter()
    for i in range(N): r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, i.to_bytes(8, "big"))
    t_r2 = time.perf_counter() - t0

    print(f"  old  {t_old:.3f}s  {t_old/N*1000:.1f}ms/call")
    print(f"  R1   {t_mid:.3f}s  {t_mid/N*1000:.1f}ms/call  old/R1={t_old/t_mid:.2f}x")
    print(f"  R2   {t_r2:.3f}s  {t_r2/N*1000:.1f}ms/call  old/R2={t_old/t_r2:.2f}x", end="")
    if _has_cython:
        # R3: hashimoto_light with Cython mix_parents + Python sha3
        for _ in range(2):
            r3_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
        t0 = time.perf_counter()
        for i in range(N): r3_hashimoto_light(FULL_SIZE, r2_cache, HEADER, i.to_bytes(8, "big"))
        t_r3 = time.perf_counter() - t0
        print(f"\n  R3   {t_r3:.3f}s  {t_r3/N*1000:.1f}ms/call  old/R3={t_old/t_r3:.2f}x  R2/R3={t_r2/t_r3:.1f}x", end="")
    else:
        print("\n  R3   (skipped — Cython extension not built)", end="")
    if _has_r4:
        # R4: full Cython + C keccak
        for _ in range(2):
            r4_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
        t0 = time.perf_counter()
        for i in range(N): r4_hashimoto_light(FULL_SIZE, r2_cache, HEADER, i.to_bytes(8, "big"))
        t_r4 = time.perf_counter() - t0
        print(f"\n  R4   {t_r4:.3f}s  {t_r4/N*1000:.1f}ms/call  old/R4={t_old/t_r4:.2f}x  R3/R4={t_r3/t_r4:.1f}x", end="")
    else:
        print("\n  R4   (skipped — Cython R4 not built)", end="")
    if _has_r5:
        # R5: Rust + tiny-keccak
        for _ in range(2):
            r5_hashimoto_light(FULL_SIZE, r5_cache, HEADER, NONCE)
        t0 = time.perf_counter()
        for i in range(N): r5_hashimoto_light(FULL_SIZE, r5_cache, HEADER, i.to_bytes(8, "big"))
        t_r5 = time.perf_counter() - t0
        prev = t_r4 if _has_r4 else t_r2
        prev_label = "R4" if _has_r4 else "R2"
        print(f"\n  R5   {t_r5:.3f}s  {t_r5/N*1000:.1f}ms/call  old/R5={t_old/t_r5:.2f}x  {prev_label}/R5={prev/t_r5:.1f}x")
    else:
        print("\n  R5   (skipped — Rust extension not built)")

    # ---- primitive micro-benchmarks (old vs R1 vs R2) ----
    NM = 200_000
    hash_list_16  = [i * 1000003 & 0xFFFFFFFF for i in range(16)]
    hash_list_8   = [i * 1000003 & 0xFFFFFFFF for i in range(8)]
    hash_bytes_64 = _old_serialize_hash(hash_list_16)
    hash_bytes_32 = _old_serialize_hash(hash_list_8)

    def _r2_sha3_512_list(x):
        if isinstance(x, list):
            x = _FMT_16I.pack(*x)
        return _r2_sha3_512(x)

    print(f"\nprimitive micro-benchmarks  x{NM:,} rounds")
    print(f"{'Function':<30} {'Old (s)':>10} {'R1 (s)':>10} {'R2 (s)':>10} {'old/R1':>8} {'old/R2':>8}")
    print("-" * 82)
    _row_partial("serialize_hash (16 ints)",
        [(_old_serialize_hash, (hash_list_16,)),
         (_r1_serialize,       (hash_list_16,)),
         (None, None)], NM)
    _row_partial("serialize_hash (8 ints)",
        [(_old_serialize_hash, (hash_list_8,)),
         (_r1_serialize,       (hash_list_8,)),
         (None, None)], NM)
    _row_partial("deserialize_hash (64B)",
        [(_old_deserialize_hash, (hash_bytes_64,)),
         (_r1_deserialize,       (hash_bytes_64,)),
         (None, None)], NM)
    _row_partial("deserialize_hash (32B)",
        [(_old_deserialize_hash, (hash_bytes_32,)),
         (_r1_deserialize,       (hash_bytes_32,)),
         (None, None)], NM)
    _row_partial("fnv",
        [(_old_fnv,  (0xDEADBEEF, 0xCAFEBABE)),
         (_r1_fnv,   (0xDEADBEEF, 0xCAFEBABE)),
         (None, None)], NM)
    _row3("ethash_sha3_512 (bytes)",
        [(_old_sha3_512, (hash_bytes_64,)),
         (_r1_sha3_512,  (hash_bytes_64,)),
         (_r2_sha3_512,  (hash_bytes_64,))], NM)
    _row3("ethash_sha3_512 (list)",
        [(_old_sha3_512,      (hash_list_16,)),
         (_r1_sha3_512,       (hash_list_16,)),
         (_r2_sha3_512_list,  (hash_list_16,))], NM)

    # ---- check_pow end-to-end ----
    print("\ncheck_pow end-to-end  (is_test=True)")
    from ethereum.pow.ethpow import check_pow
    _cp_header = b"\xca/\xf0l\xaa\xe7\xc9M\xc9h\xbe}v\xd0\xfb\xf6\r\xd2\xe1\x98\x9e\xe9\xbf\rY1\xe4\x85d\xd5\x14;"
    _cp_nonce  = (44).to_bytes(8, byteorder="big")
    _cp_mix    = bytes.fromhex("5dd318d2dff0aac95a3af5617db0bfb07eee8b0ab4a42f01d6161336be758106")
    N3 = 20
    check_pow.cache_clear()
    check_pow(1, _cp_header, _cp_mix, _cp_nonce, 100, is_test=True)
    check_pow.cache_clear()
    t0 = time.perf_counter()
    for _ in range(N3):
        check_pow.cache_clear()
        check_pow(1, _cp_header, _cp_mix, _cp_nonce, 100, is_test=True)
    t_cp = time.perf_counter() - t0
    print(f"  x{N3}: {t_cp:.4f}s  ({t_cp/N3*1000:.1f}ms/call)")
