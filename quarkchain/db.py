#!/usr/bin/python3
import time

import leveldb

from quarkchain.core import Constant, MinorBlock, RootBlock, RootBlockHeader, Transaction


class Db:

    MAX_TIMESTAMP = 2 ** 32 - 1

    def __putTx(self, tx, txHash, timestamp):
        self.put(b'tx_' + txHash, timestamp.to_bytes(4, byteorder="big") + tx.serialize())

    def __putTxToAccounts(self, tx, txHash, createTime, isPending=False):
        # The order of the transactions are
        # Pending tx, latest confirmed -> oldest confirmed
        pendingTxTimestampKey = bytes(4)
        if isPending:
            timestampKey = pendingTxTimestampKey
        else:
            inverseCreateTime = self.MAX_TIMESTAMP - createTime
            timestampKey = inverseCreateTime.to_bytes(4, byteorder="big")
        timestampValue = createTime.to_bytes(4, byteorder="big")

        addrSet = set()
        for txInput in tx.inList:
            t = self.getTx(txInput.hash)
            addr = t.outList[txInput.index].address
            addrSet.add(addr)
        for txOutput in tx.outList:
            addr = txOutput.address
            addrSet.add(addr)
        for addr in addrSet:
            self.put(b'addr_' + addr.serialize() + timestampKey + txHash, timestampValue)
            if not isPending:
                self.remove(b'addr_' + addr.serialize() + pendingTxTimestampKey + txHash)

    def putPendingTx(self, tx):
        createTime = int(time.time())
        txHash = tx.getHash()
        self.__putTx(tx, txHash, createTime)
        self.__putTxToAccounts(tx, txHash, int(time.time()), isPending=True)

    def removePendingTx(self, tx):
        txHash = tx.getHash()
        if self.get(b'txBlockHeader_' + txHash):
            # Don't delete confirmed tx
            return
        self.remove(b"tx_" + txHash)
        addrSet = set()
        for txInput in tx.inList:
            t = self.getTx(txInput.hash)
            addr = t.outList[txInput.index].address
            addrSet.add(addr)
        for txOutput in tx.outList:
            addr = txOutput.address
            addrSet.add(addr)
        for addr in addrSet:
            self.remove(b'addr_' + addr.serialize() + bytes(4) + txHash)

    def putConfirmedTx(self, tx, block, rootBlockHeader=None):
        '''
        'block' is a root chain block if the tx is a root chain coinbase tx since such tx
        isn't included in any minor block. Otherwise it is always a minor block.
        '''
        txHash = tx.getHash()
        self.__putTx(tx, txHash, block.header.createTime)
        self.put(b'txBlockHeader_' + txHash,
                 block.header.serialize())
        if rootBlockHeader is not None:
            self.put(b'txRootBlockHeader_' + txHash,
                     rootBlockHeader.serialize())
        for txIn in tx.inList:
            self.put(b'spent_' + txIn.serialize(), txHash)
        self.__putTxToAccounts(tx, txHash, block.header.createTime)

    def getTxAndTimestamp(self, txHash):
        '''
        The timestamp returned is tx creation time for pending tx
        or block.createTime for confirmed tx
        '''
        value = self.get(b'tx_' + txHash)
        if not value:
            return None
        timestamp = int.from_bytes(value[:4], byteorder="big")
        return (Transaction.deserialize(value[4:]), timestamp)

    def getTx(self, txHash):
        return self.getTxAndTimestamp(txHash)[0]

    def accountTxIter(self, address, limit=0):
        prefix = b'addr_'
        start = prefix + address.serialize()
        length = len(start)
        end = (int.from_bytes(start, byteorder="big") + 1).to_bytes(length, byteorder="big")
        done = 0
        for k, v in self.rangeIter(start, end):
            timestampStart = len(prefix) + Constant.ADDRESS_LENGTH
            timestampEnd = timestampStart + 4
            txHash = k[timestampEnd:]
            createTime = int.from_bytes(v, byteorder="big")
            yield (txHash, createTime)
            done += 1
            if limit > 0 and done >= limit:
                raise StopIteration()

    def getSpentTxHash(self, txIn):
        return self.get(b'spent_' + txIn.serialize())

    def getTxRootBlockHeader(self, txHash):
        return RootBlockHeader.deserialize(self.get(b'txRootBlockHeader_' + txHash))

    def getTxBlockHeader(self, txHash, headerClass):
        value = self.get(b'txBlockHeader_' + txHash)
        if not value:
            return None
        return headerClass.deserialize(value)

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
