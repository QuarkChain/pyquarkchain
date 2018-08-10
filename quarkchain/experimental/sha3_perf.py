
# Performance of verification of transactions
#
# Some numbers on my machine (i7 7700K 4.2 GHZ):
# SHA3 80000 shas/sec
# One i7 4700K 3.5 GHZ):
# SHA3 69000 shas/sec with pycryptodome.
# SHA3 134000 shas/sec with pysha3.

from quarkchain.core import MinorBlockHeader
import argparse
import time
import profile


def test_perf():
    N = 20000
    start_time = time.time()
    m_header = MinorBlockHeader()
    for i in range(N):
        m_header.nonce = i
        m_header.get_hash()
    duration = time.time() - start_time
    print("TPS: %.2f" % (N / duration))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=False)
    args = parser.parse_args()

    if args.profile:
        profile.run("test_perf()")
    else:
        test_perf()


if __name__ == "__main__":
    main()
