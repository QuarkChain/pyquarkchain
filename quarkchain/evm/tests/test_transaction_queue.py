import unittest

from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.evm.transaction_queue import OrderableTx, TransactionQueue


def make_test_tx(s=100000, g=50, data=b"", nonce=0):
    return EvmTransaction(
        nonce=nonce,
        startgas=s,
        gasprice=g,
        value=0,
        data=data,
        to=b"\x35" * 20,
        gas_token_id=0,
        transfer_token_id=0,
    )


class TestTransactionQueue(unittest.TestCase):
    def test(self):
        q = TransactionQueue()
        # (startgas, gasprice) pairs
        params = [
            (100000, 81),
            (50000, 74),
            (40000, 65),
            (60000, 39),
            (30000, 50),
            (30000, 50),
            (30000, 80),
        ]
        # (maxgas, expected_startgas, expected_gasprice) triplets
        operations = [
            (999999, 100000, 81),
            (999999, 30000, 80),
            (30000, 30000, 50),
            (29000, None, None),
            (30000, 30000, 50),
            (30000, None, None),
            (999999, 50000, 74),
        ]
        # Add transactions to queue
        for param in params:
            q.add_transaction(make_test_tx(s=param[0], g=param[1]))
        # Attempt pops from queue
        for (maxgas, expected_s, expected_g) in operations:
            tx = q.pop_transaction(max_gas=maxgas)
            if tx:
                assert (tx.startgas, tx.gasprice) == (expected_s, expected_g)
            else:
                assert expected_s is expected_g is None
        print("Test successful")

    def test_diff(self):
        tx1 = make_test_tx(data=b"foo")
        tx2 = make_test_tx(data=b"bar")
        tx3 = make_test_tx(data=b"baz")
        tx4 = make_test_tx(data=b"foobar")
        q1 = TransactionQueue()
        for tx in [tx1, tx2, tx3, tx4]:
            q1.add_transaction(tx)
        q2 = q1.diff([tx2])
        assert len(q2) == 3
        assert tx1 in [item.tx for item in q2.txs]
        assert tx3 in [item.tx for item in q2.txs]
        assert tx4 in [item.tx for item in q2.txs]

        q3 = q2.diff([tx4])
        assert len(q3) == 2
        assert tx1 in [item.tx for item in q3.txs]
        assert tx3 in [item.tx for item in q3.txs]

    def test_orderable_tx(self):
        assert OrderableTx(-1, 0, None) < OrderableTx(0, 0, None)
        assert OrderableTx(-1, 0, None) < OrderableTx(-1, 1, None)
        assert not OrderableTx(1, 0, None) < OrderableTx(-1, 0, None)
        assert not OrderableTx(1, 1, None) < OrderableTx(-1, 0, None)

    def test_ordering_for_same_prio(self):
        q = TransactionQueue()
        count = 10
        # Add <count> transactions to the queue, all with the same
        # startgas/gasprice but with sequential nonces.
        for i in range(count):
            q.add_transaction(make_test_tx(nonce=i))

        expected_nonce_order = [i for i in range(count)]
        nonces = []
        for i in range(count):
            tx = q.pop_transaction()
            nonces.append(tx.nonce)
        # Since they have the same gasprice they should have the same priority and
        # thus be popped in the order they were inserted.
        assert nonces == expected_nonce_order
