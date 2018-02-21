#!/usr/bin/python3

from quarkchain.core import Transaction, RootBlockHeader
from quarkchain.core import MinorBlock, RootBlock


class InMemoryDb:
    """ A simple in-memory key-value database
    TODO: Need to support range operation (e.g., leveldb)
    """

    def __init__(self):
        self.kv = dict()

    def get(self, key, default=None):
        return self.kv.get(key, default)

    def put(self, key, value):
        self.kv[key] = bytes(value)

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

    def getMinorBlockByHash(self, h):
        return MinorBlock.deserialize(self.get(b"mblock_" + h))

    def putMinorBlock(self, mBlock, mBlockHash=None):
        if mBlockHash is None:
            mBlockHash = mBlock.header.getHash()
        self.put(b'mblock_' + mBlockHash, mBlock.serialize())
        self.put(b'mblockCoinbaseTx_' + mBlockHash, mBlock.txList[0].serialize())
        self.put(b'mblockTxCount_' + mBlockHash, len(mBlock.txList).to_bytes(4, byteorder="big"))

    def putRootBlock(self, rBlock, rBlockHash=None):
        if rBlockHash is None:
            rBlockHash = rBlock.header.getHash()

        self.put(b'rblock_' + rBlockHash, rBlock.serialize())
        self.put(b'rblockHeader_' + rBlockHash, rBlock.header.serialize())

    def getRootBlockByHash(self, h):
        return RootBlock.deserialize(self.get(b"rblock_" + h))

    def getRootBlockHeaderByHash(self, h):
        return RootBlockHeader.deserialize(self.get(b"rblockHeader_" + h))

    def getMinorBlockTxCount(self, h):
        key = b"mblockTxCount_" + h
        if key not in self.kv:
            return 0
        return int.from_bytes(self.get(key), byteorder="big")


DB = InMemoryDb()
