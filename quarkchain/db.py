#!/usr/bin/python3

from quarkchain.core import Transaction, RootBlockHeader


class InMemoryDb:
    """ A simple in-memory key-value database
    TODO: Need to support range operation (e.g., leveldb)
    """

    def __init__(self):
        self.kv = dict()

    def get(self, key, default=None):
        return self.kv.get(key, default)

    def put(self, key, value):
        self.kv[key] = value

    def remove(self, key):
        del self.kv[key]

    def __contains__(self, key):
        return key in self.kv

    def putTx(self, tx, rootBlockHeader=None, txHash=None):
        if txHash is None:
            txHash = tx.getHash()
        self.put(b'tx_' + txHash, tx.serialize())
        if rootBlockHeader is not None:
            self.put(b'txRootBlockHeader_' + txHash,
                     rootBlockHeader.serialize())

    def getTx(self, txHash):
        return Transaction.deserialize(self.get(b'tx_' + txHash))

    def getTxRootBlockHeader(self, txHash):
        return RootBlockHeader.deserialize(self.get(b'txRootBlockHeader_' + txHash))


DB = InMemoryDb()
