#!/usr/bin/python3

from quarkchain.core import (
    Constant, MinorBlock, RootBlock, RootBlockHeader, Transaction)
import leveldb


class Db:

    def __putTxToAccounts(self, tx, txHash):
        done = set()
        for txInput in tx.inList:
            t = self.getTx(txInput.hash)
            addr = t.outList[txInput.index].address
            if addr.recipient not in done:
                self.put(b'addr_' + addr.serialize() + txHash,
                         b'out')
                done.add(addr.recipient)
        for txOutput in tx.outList:
            addr = txOutput.address
            if addr not in done:
                self.put(b'addr_' + addr.serialize() + txHash,
                         b'in')
                done.add(addr)

    def putTx(self, tx, rootBlockHeader=None, txHash=None):
        if txHash is None:
            txHash = tx.getHash()
        self.put(b'tx_' + txHash, tx.serialize())
        if rootBlockHeader is not None:
            self.put(b'txRootBlockHeader_' + txHash,
                     rootBlockHeader.serialize())
        for txIn in tx.inList:
            self.put(b'spent_' + txIn.serialize(), txHash)
        self.__putTxToAccounts(tx, txHash)

    def getTx(self, txHash):
        return Transaction.deserialize(self.get(b'tx_' + txHash))

    def accountTxIter(self, address):
        prefix = b'addr_'
        start = prefix + address.serialize()
        length = len(start)
        end = (int.from_bytes(start, byteorder="big") + 1).to_bytes(length, byteorder="big")
        for k, v in self.rangeIter(start, end):
            txHash = k[len(prefix) + Constant.ADDRESS_LENGTH:]
            yield txHash

    def getSpentTxHash(self, txIn):
        return self.get(b'spent_' + txIn.serialize())

    def getTxRootBlockHeader(self, txHash):
        return RootBlockHeader.deserialize(self.get(b'txRootBlockHeader_' + txHash))

    def getMinorBlockByHash(self, h):
        return MinorBlock.deserialize(self.get(b"mblock_" + h))

    def putMinorBlock(self, mBlock, mBlockHash=None):
        if mBlockHash is None:
            mBlockHash = mBlock.header.getHash()
        self.put(b'mblock_' + mBlockHash, mBlock.serialize())
        self.put(b'mblockCoinbaseTx_' + mBlockHash,
                 mBlock.txList[0].serialize())
        self.put(b'mblockTxCount_' + mBlockHash,
                 len(mBlock.txList).to_bytes(4, byteorder="big"))

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
        if key not in self:
            return 0
        return int.from_bytes(self.get(key), byteorder="big")

    def close():
        pass


class InMemoryDb(Db):
    """ A simple in-memory key-value database
    TODO: Need to support range operation (e.g., leveldb)
    """

    def __init__(self):
        self.kv = dict()

    def rangeIter(self, start, end):
        raise RuntimeError("In memory db does not support rangeIter!")

    def get(self, key, default=None):
        return self.kv.get(key, default)

    def put(self, key, value):
        self.kv[key] = bytes(value)

    def remove(self, key):
        del self.kv[key]

    def __contains__(self, key):
        return key in self.kv


class PersistentDb(Db):

    def __init__(self, path="./db", clean=False):
        if clean:
            leveldb.DestroyDB(path)
        self.db = leveldb.LevelDB(path)

    def rangeIter(self, start, end):
        yield from self.db.RangeIter(start, end)

    def get(self, key, default=None):
        try:
            return self.db.Get(key)
        except KeyError:
            return default

    def put(self, key, value):
        self.db.Put(key, value)

    def remove(self, key):
        self.db.Delete(key)

    def __contains__(self, key):
        try:
            self.db.Get(key)
            return True
        except KeyError:
            return False

    def close():
        # No close option in leveldb?
        pass


DB = InMemoryDb()
