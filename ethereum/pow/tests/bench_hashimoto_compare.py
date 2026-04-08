"""
Benchmark hashimoto_light: old (hex-based) vs mid (struct+list) vs new (struct+numpy).

- old: original hex-based implementation
- mid: struct.pack/unpack + Python list
- new: struct.pack/unpack + numpy ndarray (current implementation)

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
# MID — struct+list
# ===========================================================================
_FMT_16I = struct.Struct("<16I")
_FMT_8I  = struct.Struct("<8I")
_FMT_32I = struct.Struct("<32I")
_MID_FNV_PRIME = FNV_PRIME

def _mid_serialize(h):
    n = len(h)
    if n == 16: return _FMT_16I.pack(*h)
    if n == 8:  return _FMT_8I.pack(*h)
    if n == 32: return _FMT_32I.pack(*h)
    return struct.pack("<%dI" % n, *h)

def _mid_deserialize(h):
    n = len(h)
    if n == 64:  return list(_FMT_16I.unpack(h))
    if n == 32:  return list(_FMT_8I.unpack(h))
    if n == 128: return list(_FMT_32I.unpack(h))
    return list(struct.unpack("<%dI" % (n // 4), h))

def _mid_sha3_512(x):
    if isinstance(x, list): x = _mid_serialize(x)
    return list(_FMT_16I.unpack(_sha3_512(x)))

def _mid_sha3_256(x):
    if isinstance(x, list): x = _mid_serialize(x)
    return list(_FMT_8I.unpack(_sha3_256(x)))

def _mid_fnv(v1, v2):
    return (v1 * _MID_FNV_PRIME ^ v2) & 0xFFFFFFFF

def mid_mkcache(cache_size, seed):
    n = cache_size // HASH_BYTES
    o = [_mid_sha3_512(seed)]
    for i in range(1, n):
        o.append(_mid_sha3_512(o[-1]))
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = o[i][0] % n
            o[i] = _mid_sha3_512([a ^ b for a, b in zip(o[(i - 1 + n) % n], o[v])])
    return o

def mid_calc_dataset_item(cache, i):
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES
    mix = copy.copy(cache[i % n])
    mix[0] ^= i
    mix = _mid_sha3_512(mix)
    for j in range(DATASET_PARENTS):
        cache_index = _mid_fnv(i ^ j, mix[j % r])
        mix = list(map(_mid_fnv, mix, cache[cache_index % n]))
    return _mid_sha3_512(mix)

def mid_hashimoto_light(full_size, cache, header, nonce):
    n = full_size // HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES // HASH_BYTES
    s = _mid_sha3_512(header + nonce[::-1])
    mix = list(s) * mixhashes
    for i in range(ACCESSES):
        p = _mid_fnv(i ^ s[0], mix[i % w]) % (n // mixhashes) * mixhashes
        newdata = []
        for j in range(mixhashes):
            newdata.extend(mid_calc_dataset_item(cache, p + j))
        mix = list(map(_mid_fnv, mix, newdata))
    cmix = []
    for i in range(0, len(mix), 4):
        cmix.append(_mid_fnv(_mid_fnv(_mid_fnv(mix[i], mix[i+1]), mix[i+2]), mix[i+3]))
    return {
        b"mix digest": _mid_serialize(cmix),
        b"result": _mid_serialize(_mid_sha3_256(s + cmix)),
    }

# ===========================================================================
# NEW — numpy ndarray (current implementation)
# ===========================================================================
np.seterr(over="ignore")

from ethereum.pow.ethash import (
    mkcache as new_mkcache,
    calc_dataset_item as new_calc_dataset_item,
    hashimoto_light as new_hashimoto_light,
)

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
    t0 = time.perf_counter(); mid_cache = mid_mkcache(CACHE_SIZE, SEED); t_mc = time.perf_counter() - t0
    t0 = time.perf_counter(); new_cache = new_mkcache(CACHE_SIZE, 0);    t_nc = time.perf_counter() - t0
    print(f"  mkcache  old={t_oc*1000:.1f}ms  mid={t_mc*1000:.1f}ms  new={t_nc*1000:.1f}ms  "
          f"old/mid={t_oc/t_mc:.1f}x  old/new={t_oc/t_nc:.1f}x")

    # ---- correctness ----
    old_r = old_hashimoto_light(FULL_SIZE, old_cache, HEADER, NONCE)
    mid_r = mid_hashimoto_light(FULL_SIZE, mid_cache, HEADER, NONCE)
    new_r = new_hashimoto_light(FULL_SIZE, new_cache, HEADER, NONCE)
    assert old_r == mid_r, f"old/mid MISMATCH"
    assert old_r == new_r, f"old/new MISMATCH"
    print(f"  result   match=OK  mix={old_r[b'mix digest'].hex()[:16]}...\n")

    # ---- hashimoto_light benchmark ----
    N = 30
    print(f"{'':45} {'total':>8} {'per call':>10}")
    print(f"hashimoto_light  x{N} calls  (cache=1KB, dataset=32KB)")

    for _ in range(2):
        old_hashimoto_light(FULL_SIZE, old_cache, HEADER, NONCE)
        mid_hashimoto_light(FULL_SIZE, mid_cache, HEADER, NONCE)
        new_hashimoto_light(FULL_SIZE, new_cache, HEADER, NONCE)

    t0 = time.perf_counter()
    for i in range(N): old_hashimoto_light(FULL_SIZE, old_cache, HEADER, i.to_bytes(8, "big"))
    t_old = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N): mid_hashimoto_light(FULL_SIZE, mid_cache, HEADER, i.to_bytes(8, "big"))
    t_mid = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N): new_hashimoto_light(FULL_SIZE, new_cache, HEADER, i.to_bytes(8, "big"))
    t_new = time.perf_counter() - t0

    print(f"  old  {t_old:.3f}s  {t_old/N*1000:.1f}ms/call")
    print(f"  mid  {t_mid:.3f}s  {t_mid/N*1000:.1f}ms/call  old/mid={t_old/t_mid:.2f}x")
    print(f"  new  {t_new:.3f}s  {t_new/N*1000:.1f}ms/call  old/new={t_old/t_new:.2f}x\n")

    # ---- calc_dataset_item breakdown ----
    N2 = 300
    print(f"calc_dataset_item  x{N2} calls")

    t0 = time.perf_counter()
    for i in range(N2): old_calc_dataset_item(old_cache, i)
    t_old_i = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N2): mid_calc_dataset_item(mid_cache, i)
    t_mid_i = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N2): new_calc_dataset_item(new_cache, i)
    t_new_i = time.perf_counter() - t0

    print(f"  old  {t_old_i:.3f}s  {t_old_i/N2*1000:.2f}ms/call")
    print(f"  mid  {t_mid_i:.3f}s  {t_mid_i/N2*1000:.2f}ms/call  old/mid={t_old_i/t_mid_i:.2f}x")
    print(f"  new  {t_new_i:.3f}s  {t_new_i/N2*1000:.2f}ms/call  old/new={t_old_i/t_new_i:.2f}x")
