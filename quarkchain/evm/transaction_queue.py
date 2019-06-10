import heapq

from typing import Callable


def heaptop(x):
    return x[0]


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
    def __init__(self):
        self.counter = 0
        self.txs = []
        # in case start gas is greater than required max gas
        self.aside = []

    def __len__(self):
        return len(self.txs)

    def add_transaction(self, tx):
        prio = -tx.gasprice
        heapq.heappush(self.txs, OrderableTx(prio, self.counter, tx))
        self.counter += 1

    def pop_transaction(
        self, req_nonce_getter: Callable[[bytes], int], max_gas=9999999999
    ):
        while len(self.aside):
            top = heaptop(self.aside)
            if top.prio > max_gas:
                break  # not enough gas to process items in aside
            heapq.heappop(self.aside)
            top.prio = -top.tx.gasprice
            heapq.heappush(self.txs, top)
        for i in range(len(self.txs)):
            item = heaptop(self.txs)
            tx = item.tx
            # target found
            if tx.startgas <= max_gas and req_nonce_getter(tx.sender) == tx.nonce:
                heapq.heappop(self.txs)
                return tx
            heapq.heappop(self.txs)
            item.prio = tx.startgas
            heapq.heappush(self.aside, item)
        return None

    def peek(self, num=None):
        if num:
            return self.txs[0:num]
        else:
            return self.txs

    def diff(self, txs):
        remove_hashes = [tx.hash for tx in txs]
        keep_txs = [item for item in self.txs if item.tx.hash not in remove_hashes]
        keep_aside = [item for item in self.aside if item.tx.hash not in remove_hashes]
        q = TransactionQueue()
        q.txs = keep_txs
        q.aside = keep_aside
        heapq.heapify(q.txs)
        heapq.heapify(q.aside)
        return q
