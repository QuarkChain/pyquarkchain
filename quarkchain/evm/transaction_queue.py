import bisect
from functools import total_ordering

from typing import Callable, List

from quarkchain.core import TypedTransaction


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
        self.txs = []  # type: List[OrderableTx]
        self.tx_dict = dict()  # type: Dict[bytes, OrderableTx]

    def __len__(self):
        return len(self.txs)

    def __contains__(self, tx_hash):
        return tx_hash in self.tx_dict

    def __getitem__(self, h):
        return self.tx_dict[h].tx

    def add_transaction(self, tx: TypedTransaction):
        evm_tx = tx.tx.to_evm_tx()
        if len(self.txs) >= self.limit:
            if evm_tx.gasprice < self.txs[-1].tx.tx.to_evm_tx().gasprice:
                return  # no-op
            pop_tx = self.txs.pop(-1).tx  # type: TypedTransaction
            self.tx_dict.pop(pop_tx.get_hash(), None)
        prio = -evm_tx.gasprice
        ordered_tx = OrderableTx(prio, self.counter, tx)
        # amortized O(n) cost, ~9x slower than heapq push, may need optimization if becoming a bottleneck
        bisect.insort(self.txs, ordered_tx)
        self.tx_dict[tx.get_hash()] = ordered_tx
        self.counter += 1

    def pop_transaction(
        self, req_nonce_getter: Callable[[bytes], int], max_gas=9999999999
    ):
        i, found = 0, None
        while i < len(self.txs):
            item = self.txs[i]  # type: OrderableTx
            tx = item.tx  # type: TypedTransaction
            evm_tx = tx.tx.to_evm_tx()
            # discard old tx
            if evm_tx.nonce < req_nonce_getter(evm_tx.sender):
                pop_tx = self.txs.pop(i).tx  # type: TypedTransaction
                self.tx_dict.pop(pop_tx.get_hash(), None)
                continue
            # target found
            if (
                evm_tx.startgas <= max_gas
                and req_nonce_getter(evm_tx.sender) == evm_tx.nonce
            ):
                pop_tx = self.txs.pop(i).tx  # type: TypedTransaction
                self.tx_dict.pop(pop_tx.get_hash(), None)
                return tx
            i += 1
        return None

    def peek(self, num=None):
        if num:
            return self.txs[0:num]
        else:
            return self.txs

    def diff(self, txs: List[TypedTransaction]):
        remove_txs = [(tx.tx.to_evm_tx().sender, tx.tx.to_evm_tx().nonce) for tx in txs]

        keep_txs = []
        tx_hashes = dict()
        for item in self.txs:
            tx = item.tx
            evm_tx = tx.tx.to_evm_tx()
            if (evm_tx.sender, evm_tx.nonce) not in remove_txs:
                keep_txs.append(item)
                tx_hashes[tx.get_hash()] = item

        q = TransactionQueue(self.limit)
        q.txs = keep_txs
        q.counter = self.counter
        q.tx_dict = tx_hashes
        return q
