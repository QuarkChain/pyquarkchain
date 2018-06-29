import heapq
heapq.heaptop = lambda x: x[0]
PRIO_INFINITY = -2**100


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


class TransactionQueue():

    def __init__(self):
        self.counter = 0
        self.txs = []
        self.aside = []

    def __len__(self):
        return len(self.txs)

    def add_transaction(self, tx, force=False):
        prio = PRIO_INFINITY if force else -tx.gasprice
        heapq.heappush(self.txs, OrderableTx(prio, self.counter, tx))
        self.counter += 1

    def pop_transaction(self, max_gas=9999999999,
                        max_seek_depth=16, min_gasprice=0):
        while len(self.aside) and max_gas >= heapq.heaptop(self.aside).prio:
            item = heapq.heappop(self.aside)
            item.prio = -item.tx.gasprice
            heapq.heappush(self.txs, item)
        for i in range(min(len(self.txs), max_seek_depth)):
            item = heapq.heaptop(self.txs)
            if item.tx.startgas > max_gas:
                heapq.heappop(self.txs)
                item.prio = item.tx.startgas
                heapq.heappush(self.aside, item)
            elif item.tx.gasprice >= min_gasprice or item.prio == PRIO_INFINITY:
                heapq.heappop(self.txs)
                return item.tx
            else:
                return None
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
