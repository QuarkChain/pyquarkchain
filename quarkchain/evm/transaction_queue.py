import bisect
from functools import total_ordering

from typing import Callable


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

    def __len__(self):
        return len(self.txs)

    def add_transaction(self, tx):
        if len(self.txs) >= self.limit:
            if tx.gasprice < self.txs[-1].tx.gasprice:
                return  # no-op
            self.txs.pop(-1)
        prio = -tx.gasprice
        ordered_tx = OrderableTx(prio, self.counter, tx)
        # amortized O(n) cost, ~9x slower than heapq push, may need optimization if becoming a bottleneck
        bisect.insort(self.txs, ordered_tx)
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
                self.txs.pop(i)
                continue
            # target found
            if tx.startgas <= max_gas and req_nonce_getter(tx.sender) == tx.nonce:
                self.txs.pop(i)
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
        remove_hashes = []
        keep_txs = []
        for item in self.txs:
            if (item.tx.sender, item.tx.nonce) in remove_txs:
                remove_hashes.append(item.tx.hash)
            else:
                keep_txs.append(item)

        q = TransactionQueue(self.limit)
        q.txs = keep_txs
        q.counter = self.counter
        return q, remove_hashes
