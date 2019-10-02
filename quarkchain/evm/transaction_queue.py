import bisect
from functools import total_ordering

from typing import Callable

from quarkchain.core import TypedTransaction, SerializedEvmTransaction


@total_ordering
class OrderableTx(object):
    def __init__(self, prio, counter, tx):
        self.prio = prio
        self.counter = counter
        self.tx = tx

    def __lt__(self, other):
        if self.prio < other.prio:
            return True
        elif self.prio == other.prio:
            return self.counter < other.counter
        else:
            return False


class TransactionQueue(object):
    def __init__(self, limit: int = 10000):
        self.counter = 0
        self.limit = limit
        self.txs = []
        self.tx_dict = dict()  # type: Dict[hash, OrderableTx]

    def __len__(self):
        return len(self.txs)

    def __contains__(self, tx_hash):
        return tx_hash in self.tx_dict

    def __remove_hash(self, evm_tx):
        tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
        self.tx_dict.pop(tx.get_hash(), None)

    def add_transaction(self, evm_tx):
        tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
        if len(self.txs) >= self.limit:
            if evm_tx.gasprice < self.txs[-1].tx.gasprice:
                return  # no-op
            pop_tx = self.txs.pop(-1)
            self.__remove_hash(pop_tx.tx)
        prio = -evm_tx.gasprice
        ordered_tx = OrderableTx(prio, self.counter, evm_tx)
        # amortized O(n) cost, ~9x slower than heapq push, may need optimization if becoming a bottleneck
        bisect.insort(self.txs, ordered_tx)
        self.tx_dict[tx.get_hash()] = ordered_tx
        self.counter += 1

    def pop_transaction(
        self, req_nonce_getter: Callable[[bytes], int], max_gas=9999999999
    ):
        i, found = 0, None
        while i < len(self.txs):
            item = self.txs[i]
            tx = item.tx
            # discard old tx
            if tx.nonce < req_nonce_getter(tx.sender):
                pop_tx = self.txs.pop(i)
                self.__remove_hash(pop_tx.tx)
                continue
            # target found
            if tx.startgas <= max_gas and req_nonce_getter(tx.sender) == tx.nonce:
                pop_tx = self.txs.pop(i)
                self.__remove_hash(pop_tx.tx)
                return tx
            i += 1
        return None

    def peek(self, num=None):
        if num:
            return self.txs[0:num]
        else:
            return self.txs

    def diff(self, txs):
        remove_txs = [(tx.sender, tx.nonce) for tx in txs]
        keep_txs = [
            item
            for item in self.txs
            if (item.tx.sender, item.tx.nonce) not in remove_txs
        ]
        q = TransactionQueue(self.limit)
        q.txs = keep_txs
        q.counter = self.counter
        return q

    def get_transaction_by_hash(self, hash):
        evm_tx = self.tx_dict[hash].tx
        return TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
