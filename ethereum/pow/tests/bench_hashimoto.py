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
    print("pyethash not installed — skipping C++ comparison.")
    print("Install with: pip install pyethash")
