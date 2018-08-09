
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
from quarkchain.evm.transactions import Transaction as EvmTransaction
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
        idList.append(Identity.create_random_identity())

    accList = []
    for i in range(IDN):
        accList.append(Address.create_from_identity(idList[i]))

    print("Creating %d transactions..." % N)
    startTime = time.time()
    txList = []
    recList = []
    for i in range(N):
        fromId = idList[random.randint(0, IDN - 1)]
        toAddr = accList[random.randint(0, IDN - 1)]
        txList.append(create_random_test_transaction(fromId, toAddr))
        recList.append(fromId.get_recipient())
    duration = time.time() - startTime
    print("Creations PS: %.2f" % (N / duration))

    print("Verifying transactions")
    startTime = time.time()
    for i in range(N):
        assert(txList[i].verify_signature([recList[i]]))
    duration = time.time() - startTime
    print("Verifications PS: %.2f" % (N / duration))


def test_perf_evm():
    N = 5000
    IDN = 10
    print("Creating %d identities" % IDN)
    idList = []
    for i in range(IDN):
        idList.append(Identity.create_random_identity())

    accList = []
    for i in range(IDN):
        accList.append(Address.create_from_identity(idList[i]))

    print("Creating %d transactions..." % N)
    startTime = time.time()
    txList = []
    fromList = []
    for i in range(N):
        fromId = idList[random.randint(0, IDN - 1)]
        toAddr = accList[random.randint(0, IDN - 1)]
        evmTx = EvmTransaction(
            nonce=0,
            gasprice=1,
            startgas=2,
            to=toAddr.recipient,
            value=3,
            data=b'',
            fromFullShardId=0,
            toFullShardId=0,
            networkId=1)
        evmTx.sign(
            key=fromId.get_key())
        txList.append(evmTx)
        fromList.append(fromId.get_recipient())
    duration = time.time() - startTime
    print("Creations PS: %.2f" % (N / duration))

    print("Verifying transactions")
    startTime = time.time()
    for i in range(N):
        txList[i]._sender = None
        assert(txList[i].sender == fromList[i])
    duration = time.time() - startTime
    print("Verifications PS: %.2f" % (N / duration))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=False)
    parser.add_argument("--evm", default=False)
    args = parser.parse_args()

    if args.profile:
        profile.run('test_perf()')
    else:
        if args.evm:
            test_perf_evm()
        else:
            test_perf()


if __name__ == '__main__':
    main()
