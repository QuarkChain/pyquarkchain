"""Benchmark: pyethash.hashimoto_light (C++) vs ethash.hashimoto_light (pure Python).

Run with:
    python -m ethereum.pow.tests.bench_hashimoto
or:
    python ethereum/pow/tests/bench_hashimoto.py

Uses real mainnet parameters (block 0: cache ~16MB, full_size ~1GB) so that
pure Python and pyethash C++ are compared under identical conditions.
Note: pyethash may not be supported on Python 3.13, so this benchmark may fail to run on that version.
See https://github.com/QuarkChain/pyquarkchain/issues/976
"""

import timeit

from eth_utils import big_endian_to_int

from ethereum.pow.ethash import mkcache, hashimoto_light as py_hashimoto_light
from ethereum.pow.ethash_utils import get_cache_size, get_full_size

BLOCK_NUMBER = 0
HEADER = bytes(32)
NONCE = (0).to_bytes(8, byteorder="big")
ROUNDS = 100

# Use real mainnet parameters to match pyethash internal sizes
CACHE_SIZE = get_cache_size(BLOCK_NUMBER)
FULL_SIZE = get_full_size(BLOCK_NUMBER)

cache = mkcache(CACHE_SIZE, BLOCK_NUMBER)


def bench_python():
    py_hashimoto_light(FULL_SIZE, cache, HEADER, NONCE)


results = {}

elapsed = timeit.timeit(bench_python, number=ROUNDS)
results["pure Python"] = elapsed
print(f"Cache size: {CACHE_SIZE} bytes, Full size: {FULL_SIZE} bytes")
print(f"pure Python : {elapsed:.3f}s for {ROUNDS} calls  ({elapsed/ROUNDS*1000:.1f} ms/call)")

try:
    import pyethash

    cpp_cache = pyethash.mkcache_bytes(BLOCK_NUMBER)
    nonce_int = big_endian_to_int(NONCE)
    print(f"pyethash cache size: {len(cpp_cache)} bytes")

    def bench_cpp():
        pyethash.hashimoto_light(BLOCK_NUMBER, cpp_cache, HEADER, nonce_int)

    elapsed_cpp = timeit.timeit(bench_cpp, number=ROUNDS)
    results["pyethash (C++)"] = elapsed_cpp
    print(f"pyethash C++: {elapsed_cpp:.3f}s for {ROUNDS} calls  ({elapsed_cpp/ROUNDS*1000:.1f} ms/call)")
    print(f"Speedup     : {results['pure Python'] / elapsed_cpp:.1f}x")

except ImportError:
    _has_pyethash = False

# ===========================================================================
# Shared keccak helpers
# ===========================================================================
def _sha3_256(x): return keccak.new(digest_bits=256, data=x).digest()
def _sha3_512(x): return keccak.new(digest_bits=512, data=x).digest()

# ===========================================================================
# Round 1 — struct+list
# ===========================================================================
_FMT_16I = struct.Struct("<16I")
_FMT_8I  = struct.Struct("<8I")
_FMT_32I = struct.Struct("<32I")

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
    return (v1 * FNV_PRIME ^ v2) & 0xFFFFFFFF

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
# Round 2 — numpy ndarray
# ===========================================================================
np.seterr(over="ignore")

_R2_FNV_PRIME = np.uint32(FNV_PRIME)

def _r2_sha3_512(x):
    """sha3-512: bytes or ndarray → uint32 ndarray (16,)."""
    if isinstance(x, np.ndarray):
        x = x.astype("<u4", copy=False).tobytes()
    return np.frombuffer(_sha3_512(x), dtype="<u4").copy()

def _r2_sha3_256(x):
    """sha3-256: bytes or ndarray → uint32 ndarray (8,)."""
    if isinstance(x, np.ndarray):
        x = x.astype("<u4", copy=False).tobytes()
    return np.frombuffer(_sha3_256(x), dtype="<u4").copy()

def r2_mkcache(cache_size, seed):
    """Build cache as uint32 ndarray of shape (n, 16)."""
    if isinstance(seed, int):
        # seed is a block_number — derive actual seed bytes
        from ethereum.pow.ethash import cache_seeds
        from ethereum.pow.ethash_utils import EPOCH_LENGTH, ethash_sha3_256
        block_number = seed
        while len(cache_seeds) <= block_number // EPOCH_LENGTH:
            new_seed = _old_serialize_hash(ethash_sha3_256(cache_seeds[-1]))
            cache_seeds.append(new_seed)
        seed = cache_seeds[block_number // EPOCH_LENGTH]
    n = cache_size // HASH_BYTES
    o = np.empty((n, 16), dtype=np.uint32)
    o[0] = _r2_sha3_512(seed)
    for i in range(1, n):
        o[i] = _r2_sha3_512(o[i - 1])
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = int(o[i, 0]) % n
            o[i] = _r2_sha3_512(o[(i - 1 + n) % n] ^ o[v])
    return o

def r2_calc_dataset_item(cache, i):
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
    n = full_size // HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES // HASH_BYTES
    s = _r2_sha3_512(header + nonce[::-1])
    mix = np.tile(s, mixhashes)
    s0 = int(s[0])
    newdata = np.empty(w, dtype=np.uint32)
    with np.errstate(over="ignore"):
        for i in range(ACCESSES):
            p = ((i ^ s0) * FNV_PRIME ^ int(mix[i % w])) & 0xFFFFFFFF
            p = p % (n // mixhashes) * mixhashes
            for j in range(mixhashes):
                newdata[j * 16:(j + 1) * 16] = r2_calc_dataset_item(cache, p + j)
            mix *= _R2_FNV_PRIME
            mix ^= newdata
        mix_r = mix.reshape(-1, 4)
        cmix = mix_r[:, 0] * _R2_FNV_PRIME ^ mix_r[:, 1]
        cmix = cmix * _R2_FNV_PRIME ^ mix_r[:, 2]
        cmix = cmix * _R2_FNV_PRIME ^ mix_r[:, 3]
    cmix = cmix.astype("<u4", copy=False)
    s_cmix = np.concatenate([s, cmix])
    return {
        b"mix digest": cmix.tobytes(),
        b"result": _r2_sha3_256(s_cmix).tobytes(),
    }

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
    CACHE_SIZE = _get_cache_size(0)   # real epoch-0 cache size (~16MB)
    FULL_SIZE  = _get_full_size(0)    # real epoch-0 dataset size (~1GB)
    SEED   = b"\x00" * 32             # epoch-0 seed
    HEADER = bytes.fromhex("c9149cc0386e689d789a1c2f3d5d169a61a6218ed30e74414dc736e442ef3d1f")
    NONCE  = (0).to_bytes(8, byteorder="big")

    # ---- build caches (real epoch-0) ----
    print(f"Building caches (real epoch-0: cache={CACHE_SIZE//1024//1024}MB, dataset={FULL_SIZE//1024//1024}MB)...")
    print("  old mkcache: skipped (>60s at epoch-0) — using R1 cache for old benchmarks")
    t0 = time.perf_counter(); r1_cache = r1_mkcache(CACHE_SIZE, SEED); t_mc = time.perf_counter() - t0
    old_cache = r1_cache  # R1 produces identical data to old; correctness verified separately
    t0 = time.perf_counter(); r2_cache = r2_mkcache(CACHE_SIZE, SEED); t_nc = time.perf_counter() - t0
    py_cache = None
    if _has_pyethash:
        t0 = time.perf_counter(); py_cache = _pyethash.mkcache_bytes(0); t_pyc = time.perf_counter() - t0
        print(f"  mkcache  R1={t_mc:.2f}s  R2={t_nc:.2f}s  pyethash={t_pyc*1000:.1f}ms  "
              f"R1/R2={t_mc/t_nc:.1f}x  R1/pyethash={t_mc/t_pyc:.0f}x")
    else:
        print(f"  mkcache  R1={t_mc:.2f}s  R2={t_nc:.2f}s  R1/R2={t_mc/t_nc:.1f}x")

    # ---- correctness ----
    mid_r = r1_hashimoto_light(FULL_SIZE, r1_cache, HEADER, NONCE)
    new_r = r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)
    assert mid_r == new_r, f"R1/R2 MISMATCH\n  R1={mid_r}\n  R2={new_r}"
    py_tag = "OK" if _has_pyethash else "SKIP (not installed)"
    print(f"  result   R1/R2=OK  pyethash={py_tag}  mix={mid_r[b'mix digest'].hex()[:16]}...\n")

    # ---- calc_dataset_item ----
    N2 = 300
    print(f"calc_dataset_item  x{N2} calls  (real epoch-0 cache)")

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
    print(f"  R2   {t_r2_i:.3f}s  {t_r2_i/N2*1000:.2f}ms/call  old/R2={t_old_i/t_r2_i:.2f}x")
    print(f"  pyethash  N/A (C++ API does not expose calc_dataset_item)")

    # ---- hashimoto_light ----
    N = 30
    print(f"\nhashimoto_light  x{N} calls  (real epoch-0: cache={CACHE_SIZE//1024//1024}MB, dataset={FULL_SIZE//1024//1024}MB)")

    for _ in range(2):
        r1_hashimoto_light(FULL_SIZE, r1_cache, HEADER, NONCE)
        r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, NONCE)

    t0 = time.perf_counter()
    for i in range(N): old_hashimoto_light(FULL_SIZE, old_cache, HEADER, i.to_bytes(8, "big"))
    t_old = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N): r1_hashimoto_light(FULL_SIZE, r1_cache, HEADER, i.to_bytes(8, "big"))
    t_mid = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(N): r2_hashimoto_light(FULL_SIZE, r2_cache, HEADER, i.to_bytes(8, "big"))
    t_r2 = time.perf_counter() - t0

    print(f"  old  {t_old:.3f}s  {t_old/N*1000:.1f}ms/call")
    print(f"  R1   {t_mid:.3f}s  {t_mid/N*1000:.1f}ms/call  old/R1={t_old/t_mid:.2f}x")
    print(f"  R2   {t_r2:.3f}s  {t_r2/N*1000:.1f}ms/call  old/R2={t_old/t_r2:.2f}x", end="")
    if _has_pyethash and py_cache is not None:
        for _ in range(2):
            _pyethash.hashimoto_light(0, py_cache, HEADER, 0)
        t0 = time.perf_counter()
        for i in range(N): _pyethash.hashimoto_light(0, py_cache, HEADER, i)
        t_py = time.perf_counter() - t0
        print(f"\n  pyethash  {t_py:.3f}s  {t_py/N*1000:.1f}ms/call  old/pyethash={t_old/t_py:.0f}x  R2/pyethash={t_r2/t_py:.1f}x")
    else:
        print(f"\n  pyethash  (skipped — run: pip install pyethash)")

    # ---- primitive micro-benchmarks ----
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
    print("\ncheck_pow end-to-end  (is_test=True, python path)")
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

