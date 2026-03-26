"""Benchmark: pyethash.hashimoto_light (C++) vs ethash.hashimoto_light (pure Python).

Run with:
    python -m ethereum.pow.tests.bench_hashimoto
or:
    python ethereum/pow/tests/bench_hashimoto.py

Uses test-sized DAG parameters (cache_size=1024, full_size=32*1024) so the
benchmark completes quickly without generating a full mainnet DAG.
"""

import timeit

from eth_utils import big_endian_to_int

from ethereum.pow.ethash import mkcache, hashimoto_light as py_hashimoto_light

# Fixed inputs — mirror the is_test=True path in ethpow.py
CACHE_SIZE = 1024
FULL_SIZE = 32 * 1024
BLOCK_NUMBER = 0
HEADER = bytes(32)
NONCE = (0).to_bytes(8, byteorder="big")
ROUNDS = 100

cache = mkcache(CACHE_SIZE, BLOCK_NUMBER)


def bench_python():
    py_hashimoto_light(FULL_SIZE, cache, HEADER, NONCE)


results = {}

elapsed = timeit.timeit(bench_python, number=ROUNDS)
results["pure Python"] = elapsed
print(f"pure Python : {elapsed:.3f}s for {ROUNDS} calls  ({elapsed/ROUNDS*1000:.1f} ms/call)")

try:
    import pyethash

    # pyethash.mkcache_bytes takes block_number directly (mirrors calculate_cache in ethpow.py)
    cpp_cache = pyethash.mkcache_bytes(BLOCK_NUMBER)
    nonce_int = big_endian_to_int(NONCE)

    def bench_cpp():
        # Signature matches ethpow.py line 53-54:
        #   pyethash.hashimoto_light(block_number, cache_bytes, mining_hash, nonce_int)
        pyethash.hashimoto_light(BLOCK_NUMBER, cpp_cache, HEADER, nonce_int)

    elapsed_cpp = timeit.timeit(bench_cpp, number=ROUNDS)
    results["pyethash (C++)"] = elapsed_cpp
    print(f"pyethash C++: {elapsed_cpp:.3f}s for {ROUNDS} calls  ({elapsed_cpp/ROUNDS*1000:.1f} ms/call)")
    print(f"Speedup     : {results['pure Python'] / elapsed_cpp:.1f}x")

except ImportError:
    print("pyethash not installed — skipping C++ comparison.")
    print("Install with: pip install pyethash")
