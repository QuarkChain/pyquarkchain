# Performance of verification of transactions
#
# Some numbers on my machine (i7 7700K 4.2 GHZ):
# SHA3 80000 shas/sec
# SHA3 514.86 Mb/s
# One i7 4700K 3.5 GHZ):
# SHA3 69000 shas/sec with pycryptodome.
# SHA3 134000 shas/sec with pysha3.

from quarkchain.core import MinorBlockHeader
import argparse
import time
import profile
import os

from quarkchain.utils import sha3_256


def test_perf():
    N = 20000
    start_time = time.time()
    m_header = MinorBlockHeader()
    for i in range(N):
        m_header.nonce = i
        m_header.get_hash()
    duration = time.time() - start_time
    print("TPS: %.2f" % (N / duration))


def test_perf_data():
    N = 100
    S = 1024 * 1024
    start_time = time.time()
    b = os.urandom(S)
    for i in range(N):
        sha3_256(b)
    duration = time.time() - start_time
    print("Data per sec: %.2f Mb/s" % (N * S / 1024 / 1024 / duration))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=False)
    args = parser.parse_args()

    if args.profile:
        profile.run("test_perf()")
        profile.run("test_perf_data()")
    else:
        test_perf()
        test_perf_data()


if __name__ == "__main__":
    main()
