"""
Benchmark pure-Python vs pyethash across 30 epochs.

    python -m ethereum.pow.tests.bench_hashimoto_compare

Per epoch: 1 mkcache + 100 hashimoto_light + 100 check_pow calls.
check_pow uses is_test=False (real dataset sizes).
Expected runtime: pyethash ~25s, python ~10min.
"""
import time

from ethereum.pow.ethpow import check_pow
from ethereum.pow.ethash import hashimoto_light, mkcache, set_ethash_lib
from ethereum.pow.ethash_utils import (
    get_cache_size, get_full_size,
    EPOCH_LENGTH,
)

N_EPOCHS = 30
N_HASH_PER_EPOCH = 100

HEADER = bytes.fromhex("c9149cc0386e689d789a1c2f3d5d169a61a6218ed30e74414dc736e442ef3d1f")


def _bench(lib_name):
    """Run benchmark; prints per-epoch table and returns (mc_list, h_list, cp_list)."""

    print(f"\n=== {lib_name}  epochs 0-{N_EPOCHS - 1}, {N_HASH_PER_EPOCH} calls/epoch ===")
    print(f"{'epoch':>5}  {'mkcache(ms)':>12}  {'hashimoto(ms)':>14}  {'check_pow(ms)':>14}")
    mc_list, h_list, cp_list = [], [], []
    set_ethash_lib(lib_name)  # global switch for which ethash implementation to use

    for epoch in range(N_EPOCHS):
        block      = epoch * EPOCH_LENGTH
        cache_size = get_cache_size(epoch)
        full_size  = get_full_size(epoch)

        t0 = time.perf_counter()
        cache = mkcache(cache_size, epoch)
        mc_list.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        for i in range(N_HASH_PER_EPOCH):
            hashimoto_light(full_size, cache, HEADER, i.to_bytes(8, "big"), block)
        h_list.append((time.perf_counter() - t0) / N_HASH_PER_EPOCH)

        t0 = time.perf_counter()
        for i in range(N_HASH_PER_EPOCH):
            check_pow(block, HEADER, bytes(32), i.to_bytes(8, "big"), 2**256 - 1)
        cp_list.append((time.perf_counter() - t0) / N_HASH_PER_EPOCH)

        print(f"{epoch:>5}  {mc_list[-1]*1000:>12.0f}  "
              f"{h_list[-1]*1000:>14.1f}  {cp_list[-1]*1000:>14.1f}")

    total = sum(mc_list) + (sum(h_list) + sum(cp_list)) * N_HASH_PER_EPOCH
    print(f"  total={total:.0f}s")
    return mc_list, h_list, cp_list


def _avg(lst): return sum(lst) / len(lst)


if __name__ == "__main__":
    pe_mc, pe_h, pe_cp = _bench("pyethash")
    py_mc, py_h, py_cp = _bench("python")

    # ---- summary table ----
    pe_mc_avg = _avg(pe_mc) * 1000     # ms
    py_mc_avg = _avg(py_mc) * 1000
    pe_h_avg  = _avg(pe_h)  * 1000     # ms
    py_h_avg  = _avg(py_h)  * 1000
    pe_cp_avg = _avg(pe_cp) * 1000     # ms
    py_cp_avg = _avg(py_cp) * 1000
    pe_total  = sum(pe_mc) + sum(pe_h) * N_HASH_PER_EPOCH + sum(pe_cp) * N_HASH_PER_EPOCH
    py_total  = sum(py_mc) + sum(py_h) * N_HASH_PER_EPOCH + sum(py_cp) * N_HASH_PER_EPOCH

    W = 18
    print(f"\nResults  ({N_EPOCHS} epochs × {N_HASH_PER_EPOCH} calls)")
    print(f"{'':12} {'mkcache(ms)':>{W}} {'hashimoto(ms)':>{W}} {'check_pow(ms)':>{W}} {'total(s)':>{W}}")
    print(f"{'pyethash':12} {pe_mc_avg:>{W}.0f} {pe_h_avg:>{W}.1f} {pe_cp_avg:>{W}.1f} {pe_total:>{W}.1f}")
    print(f"{'python':12} {py_mc_avg:>{W}.0f} {py_h_avg:>{W}.1f} {py_cp_avg:>{W}.1f} {py_total:>{W}.1f}")
    print(f"{'speedup':12} {py_mc_avg/pe_mc_avg:>{W}.0f}x"
          f" {py_h_avg/pe_h_avg:>{W}.0f}x"
          f" {py_cp_avg/pe_cp_avg:>{W}.0f}x"
          f" {py_total/pe_total:>{W}.0f}x")
