
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
    id_list = []
    for i in range(IDN):
        id_list.append(Identity.create_random_identity())

    acc_list = []
    for i in range(IDN):
        acc_list.append(Address.create_from_identity(id_list[i]))

    print("Creating %d transactions..." % N)
    start_time = time.time()
    tx_list = []
    rec_list = []
    for i in range(N):
        from_id = id_list[random.randint(0, IDN - 1)]
        to_addr = acc_list[random.randint(0, IDN - 1)]
        tx_list.append(create_random_test_transaction(from_id, to_addr))
        rec_list.append(from_id.get_recipient())
    duration = time.time() - start_time
    print("Creations PS: %.2f" % (N / duration))

    print("Verifying transactions")
    start_time = time.time()
    for i in range(N):
        assert tx_list[i].verify_signature([rec_list[i]])
    duration = time.time() - start_time
    print("Verifications PS: %.2f" % (N / duration))


def test_perf_evm():
    N = 5000
    IDN = 10
    print("Creating %d identities" % IDN)
    id_list = []
    for i in range(IDN):
        id_list.append(Identity.create_random_identity())

    acc_list = []
    for i in range(IDN):
        acc_list.append(Address.create_from_identity(id_list[i]))

    print("Creating %d transactions..." % N)
    start_time = time.time()
    tx_list = []
    from_list = []
    for i in range(N):
        from_id = id_list[random.randint(0, IDN - 1)]
        to_addr = acc_list[random.randint(0, IDN - 1)]
        evm_tx = EvmTransaction(
            nonce=0,
            gasprice=1,
            startgas=2,
            to=to_addr.recipient,
            value=3,
            data=b"",
            from_full_shard_id=0,
            to_full_shard_id=0,
            network_id=1,
        )
        evm_tx.sign(key=from_id.get_key())
        tx_list.append(evm_tx)
        from_list.append(from_id.get_recipient())
    duration = time.time() - start_time
    print("Creations PS: %.2f" % (N / duration))

    print("Verifying transactions")
    start_time = time.time()
    for i in range(N):
        tx_list[i]._sender = None
        assert tx_list[i].sender == from_list[i]
    duration = time.time() - start_time
    print("Verifications PS: %.2f" % (N / duration))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=False)
    parser.add_argument("--evm", default=False)
    args = parser.parse_args()

    if args.profile:
        profile.run("test_perf()")
    else:
        if args.evm:
            test_perf_evm()
        else:
            test_perf()


if __name__ == "__main__":
    main()
