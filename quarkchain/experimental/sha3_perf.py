
# Performance of verification of transactions
#
# Some numbers on my machine (i7 7700K 4.2 GHZ):
# SHA3 80000 shas/sec

from quarkchain.core import MinorBlockHeader
import argparse
import time
import profile


def test_perf():
    N = 20000
    startTime = time.time()
    mHeader = MinorBlockHeader()
    for i in range(N):
        mHeader.nonce = i
        mHeader.getHash()
    duration = time.time() - startTime
    print("TPS: %.2f" % (N / duration))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=False)
    args = parser.parse_args()

    if args.profile:
        profile.run('test_perf()')
    else:
        test_perf()


if __name__ == '__main__':
    main()
