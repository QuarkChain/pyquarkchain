import unittest

from quarkchain.core import TypedTransaction, SerializedEvmTransaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.evm.transaction_queue import OrderableTx, TransactionQueue


def make_test_tx(s=100000, g=50, data=b"", nonce=0, key=None):
    evm_tx = EvmTransaction(
        nonce=nonce,
        startgas=s,
        gasprice=g,
        value=0,
        data=data,
        to=b"\x35" * 20,
        gas_token_id=0,
        transfer_token_id=0,
        r=1,
        s=1,
        v=28,
    )
    if key:
        evm_tx.sign(key=key)

    return TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))


class TestTransactionQueue(unittest.TestCase):
    @staticmethod
    def _gasprices(q: TransactionQueue):
        return [i.tx.tx.evm_tx.gasprice for i in q.txs]

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
        for (max_gas, expected_s, expected_g) in operations:
            tx = q.pop_transaction(req_nonce_getter=lambda _: 0, max_gas=max_gas)
            if tx:
                evm_tx = tx.tx.evm_tx
                assert (evm_tx.startgas, evm_tx.gasprice) == (expected_s, expected_g)
            else:
                assert expected_s is expected_g is None

    def test_diff(self):
        key = b"00000000000000000000000000000000"
        tx1 = make_test_tx(data=b"foo", nonce=1, key=key)
        tx2 = make_test_tx(data=b"bar")
        tx3 = make_test_tx(data=b"baz")
        tx4 = make_test_tx(data=b"foobar")
        tx5 = make_test_tx(data=b"foobaz", nonce=1, key=key)

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

        q3.add_transaction(tx5)
        assert len(q3) == 3
        assert tx1 in [item.tx for item in q3.txs]
        assert tx5 in [item.tx for item in q3.txs]
        q4 = q3.diff([tx5])
        assert len(q4) == 1
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
            tx = q.pop_transaction(req_nonce_getter=lambda _: i).tx.evm_tx
            nonces.append(tx.nonce)
        # Since they have the same gasprice they should have the same priority and
        # thus be popped in the order they were inserted.
        assert nonces == expected_nonce_order

    def test_future_tx_higher_nonce(self):
        q = TransactionQueue()
        nonce_getter_maker = lambda i: (lambda _: i)
        # Expect nonce to be 0
        q.add_transaction(make_test_tx(nonce=1))
        res = q.pop_transaction(nonce_getter_maker(0))
        self.assertIsNone(res)
        # Now add a current tx
        q.add_transaction(make_test_tx(nonce=0))
        res = q.pop_transaction(nonce_getter_maker(0))
        self.assertIsNotNone(res)
        # Now try fetching the future tx with updated nonce state
        res = q.pop_transaction(nonce_getter_maker(1))
        self.assertIsNotNone(res)

    def test_multiple_future_tx(self):
        q = TransactionQueue()
        nonce_getter_maker = lambda i: (lambda _: i)
        # Future tx with increasing gas price
        for i in range(1, 6):
            q.add_transaction(make_test_tx(nonce=1, g=i))
        # Expect nonce to be 0
        res = q.pop_transaction(nonce_getter_maker(0))
        self.assertIsNone(res)
        # Internal state: total tx size == 5
        self.assertEqual(len(q), 5)
        # Add first valid tx
        q.add_transaction(make_test_tx(nonce=0))
        res = q.pop_transaction(nonce_getter_maker(0)).tx.evm_tx
        self.assertIsNotNone(res)
        # Now verify next tx
        res = q.pop_transaction(nonce_getter_maker(1)).tx.evm_tx
        self.assertEqual(res.nonce, 1)
        self.assertEqual(res.gasprice, 5)
        # Verify internal state, still have remaining txs with nonce == 1
        self.assertEqual(len(q), 4)
        # Stale tx will be cleaned up in the future
        res = q.pop_transaction(nonce_getter_maker(2))
        self.assertIsNone(res)
        self.assertEqual(len(q), 0)

    def test_discard_underpriced_tx(self):
        q = TransactionQueue(limit=6)
        # (startgas, gasprice) pairs
        params = [
            (100000, 81),
            (50000, 74),
            (40000, 65),
            (60000, 39),
            (30000, 50),
            (30000, 80),
        ]
        # Add transactions to queue
        for param in params:
            q.add_transaction(make_test_tx(s=param[0], g=param[1]))

        prices = self._gasprices(q)
        self.assertListEqual(prices, [81, 80, 74, 65, 50, 39])

        # Add a lower priced tx is a no-op
        q.add_transaction(make_test_tx(s=100000, g=38))
        self.assertListEqual(self._gasprices(q), prices)

        # Add a higher priced tx, should be placed inside the queue
        q.add_transaction(make_test_tx(s=100000, g=70))
        self.assertListEqual(self._gasprices(q), [81, 80, 74, 70, 65, 50])
