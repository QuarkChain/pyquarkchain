"""
Benchmark: old (hex-based) vs mid (struct+list) vs new (struct+numpy) implementations.

- old: original implementation using hex-based encode/decode
- mid: struct.pack/unpack + Python list (first optimization)
- new: struct.pack/unpack + numpy ndarray (current implementation)

Run with:
    PYTHONPATH=. python ethereum/pow/tests/bench_before_after.py
"""

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
FNV_PRIME = 0x01000193

# ===========================================================================
# OLD — hex-based (original implementation before any optimization)
# ===========================================================================
def old_decode_int(s):
    return int(encode_hex(s[::-1]), 16) if s else 0

def old_encode_int(s):
    a = "%x" % s
    return b"" if s == 0 else decode_hex("0" * (len(a) % 2) + a)[::-1]

def old_zpad(s, length):
    return s + b"\x00" * max(0, length - len(s))

def old_serialize_hash(h):
    return b"".join([old_zpad(old_encode_int(x), 4) for x in h])

def old_deserialize_hash(h):
    return [old_decode_int(h[i:i + WORD_BYTES]) for i in range(0, len(h), WORD_BYTES)]

def old_fnv(v1, v2):
    return (v1 * FNV_PRIME ^ v2) % 2 ** 32

def old_ethash_sha3_512(x):
    if isinstance(x, list):
        x = old_serialize_hash(x)
    return old_deserialize_hash(_sha3_512(x))

# ===========================================================================
# MID — struct+list (first optimization: replace hex with struct)
# ===========================================================================
_FMT_16I = struct.Struct("<16I")
_FMT_8I  = struct.Struct("<8I")
_FMT_32I = struct.Struct("<32I")

def mid_serialize_hash(h):
    n = len(h)
    if n == 16: return _FMT_16I.pack(*h)
    if n == 8:  return _FMT_8I.pack(*h)
    if n == 32: return _FMT_32I.pack(*h)
    return struct.pack("<%dI" % n, *h)

def mid_deserialize_hash(h):
    n = len(h)
    if n == 64:  return list(_FMT_16I.unpack(h))
    if n == 32:  return list(_FMT_8I.unpack(h))
    if n == 128: return list(_FMT_32I.unpack(h))
    return list(struct.unpack("<%dI" % (n // 4), h))

def mid_fnv(v1, v2):
    return (v1 * FNV_PRIME ^ v2) & 0xFFFFFFFF

def mid_ethash_sha3_512(x):
    if isinstance(x, list):
        x = mid_serialize_hash(x)
    return list(_FMT_16I.unpack(_sha3_512(x)))

# ===========================================================================
# NEW — numpy ndarray (current implementation)
# ===========================================================================
np.seterr(over="ignore")

from ethereum.pow.ethash_utils import ethash_sha3_512_np

_FMT_16I_NEW = struct.Struct("<16I")

def new_fnv(v1, v2):
    return (v1 * FNV_PRIME ^ v2) & 0xFFFFFFFF

def new_ethash_sha3_512_np(x):
    """Wrapper: accepts list or bytes, returns ndarray."""
    if isinstance(x, list):
        x = _FMT_16I_NEW.pack(*x)
    return ethash_sha3_512_np(x)

# ===========================================================================
# Benchmark harness
# ===========================================================================
def bench(func, args, rounds=200_000):
    for _ in range(1000):
        func(*args)
    t0 = time.perf_counter()
    for _ in range(rounds):
        func(*args)
    return time.perf_counter() - t0

def row3(label, old_fn, old_args, mid_fn, mid_args, new_fn, new_args, N):
    t_old = bench(old_fn, old_args, N)
    t_mid = bench(mid_fn, mid_args, N)
    t_new = bench(new_fn, new_args, N)
    print(f"{label:<30} {t_old:>10.4f} {t_mid:>10.4f} {t_new:>10.4f} "
          f"{t_old/t_mid:>7.1f}x {t_old/t_new:>7.1f}x")

def row2(label, old_fn, old_args, mid_fn, mid_args, N):
    """For functions removed in new — only old vs mid."""
    t_old = bench(old_fn, old_args, N)
    t_mid = bench(mid_fn, mid_args, N)
    print(f"{label:<30} {t_old:>10.4f} {t_mid:>10.4f} {'N/A':>10} "
          f"{t_old/t_mid:>7.1f}x {'N/A':>7}")

if __name__ == "__main__":
    hash_list_16  = [i * 1000003 & 0xFFFFFFFF for i in range(16)]
    hash_list_8   = [i * 1000003 & 0xFFFFFFFF for i in range(8)]
    hash_bytes_64 = old_serialize_hash(hash_list_16)
    hash_bytes_32 = old_serialize_hash(hash_list_8)

    N = 200_000
    print(f"Rounds: {N:,}\n")
    print(f"{'Function':<30} {'Old (s)':>10} {'Mid (s)':>10} {'New (s)':>10} {'old/mid':>8} {'old/new':>8}")
    print("-" * 82)

    # serialize/deserialize removed in new — old vs mid only
    row2("serialize_hash (16 ints)",
         old_serialize_hash, (hash_list_16,),
         mid_serialize_hash, (hash_list_16,), N)
    row2("serialize_hash (8 ints)",
         old_serialize_hash, (hash_list_8,),
         mid_serialize_hash, (hash_list_8,), N)
    row2("deserialize_hash (64B)",
         old_deserialize_hash, (hash_bytes_64,),
         mid_deserialize_hash, (hash_bytes_64,), N)
    row2("deserialize_hash (32B)",
         old_deserialize_hash, (hash_bytes_32,),
         mid_deserialize_hash, (hash_bytes_32,), N)

    # fnv and sha3 — all three versions
    row3("fnv",
         old_fnv, (0xDEADBEEF, 0xCAFEBABE),
         mid_fnv, (0xDEADBEEF, 0xCAFEBABE),
         new_fnv, (0xDEADBEEF, 0xCAFEBABE), N)
    row3("ethash_sha3_512 (bytes)",
         old_ethash_sha3_512,    (hash_bytes_64,),
         mid_ethash_sha3_512,    (hash_bytes_64,),
         new_ethash_sha3_512_np, (hash_bytes_64,), N)
    row3("ethash_sha3_512 (list)",
         old_ethash_sha3_512,    (hash_list_16,),
         mid_ethash_sha3_512,    (hash_list_16,),
         new_ethash_sha3_512_np, (hash_list_16,), N)

    # End-to-end: check_pow
    print("\n--- End-to-end: check_pow (is_test=True) ---")
    from ethereum.pow.ethpow import check_pow
    header_hash = b"\xca/\xf0l\xaa\xe7\xc9M\xc9h\xbe}v\xd0\xfb\xf6\r\xd2\xe1\x98\x9e\xe9\xbf\rY1\xe4\x85d\xd5\x14;"
    nonce_found = (44).to_bytes(8, byteorder="big")
    mixhash = bytes.fromhex("5dd318d2dff0aac95a3af5617db0bfb07eee8b0ab4a42f01d6161336be758106")

    N3 = 20
    check_pow.cache_clear()
    check_pow(1, header_hash, mixhash, nonce_found, 100, is_test=True)
    check_pow.cache_clear()

    t0 = time.perf_counter()
    for i in range(N3):
        check_pow.cache_clear()
        check_pow(1, header_hash, mixhash, nonce_found, 100, is_test=True)
    t_e2e = time.perf_counter() - t0
    print(f"check_pow x{N3}: {t_e2e:.4f}s  ({t_e2e/N3*1000:.1f}ms per call)")
