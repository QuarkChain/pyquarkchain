
# Performance of verification of transactions
#
# Some numbers on my machine (i7 7700K 4.2 GHZ):
# Verifications per second: 170.16 (using slow ecdsa_raw_recover)
# Verifications per second: 9549.38 (using coincurve)
#
# The number of openssl: openssl speed ecdsap256
# Verificatoins per second: 19088.5

from quarkchain.tests.test_utils import create_random_test_transaction
from quarkchain.core import Identity, Address
import argparse
import random
import time
import profile


def test_perf():
    N = 5000
    IDN = 10
    print("Creating %d identities" % IDN)
    idList = []
    for i in range(IDN):
        idList.append(Identity.createRandomIdentity())

    accList = []
    for i in range(IDN):
        accList.append(Address.createFromIdentity(idList[i]))

    print("Creating %d transactions..." % N)
    startTime = time.time()
    txList = []
    recList = []
    for i in range(N):
        fromId = idList[random.randint(0, IDN - 1)]
        toAddr = accList[random.randint(0, IDN - 1)]
        txList.append(create_random_test_transaction(fromId, toAddr))
        recList.append(fromId.getRecipient())
    duration = time.time() - startTime
    print("Creations PS: %.2f" % (N / duration))

    print("Verifying transactions")
    startTime = time.time()
    for i in range(N):
        assert(txList[i].verifySignature([recList[i]]))
    duration = time.time() - startTime
    print("Verifications PS: %.2f" % (N / duration))


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
