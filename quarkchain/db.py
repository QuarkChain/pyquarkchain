#!/usr/bin/python3
import copy
import leveldb

from quarkchain.core import Constant, MinorBlock, RootBlock, RootBlockHeader, Transaction
from functools import lru_cache


class Db:

    MAX_TIMESTAMP = 2 ** 32 - 1

    def __putTxToAccounts(self, tx, txHash, createTime, consumedUtxoList=None):
        # The transactions are ordered from the latest to the oldest
        inverseCreateTime = self.MAX_TIMESTAMP - createTime
        timestampKey = inverseCreateTime.to_bytes(4, byteorder="big")
        timestampValue = createTime.to_bytes(4, byteorder="big")

        addrSet = set()
        if consumedUtxoList is None:
            # Slow path
            for txInput in tx.inList:
                t = self.getTx(txInput.hash)
                addr = t.outList[txInput.index].address
                addrSet.add(addr)
        else:
            # Fast path
            for consumedUtxo in consumedUtxoList:
                addrSet.add(consumedUtxo.address)

        for txOutput in tx.outList:
            addr = txOutput.address
            addrSet.add(addr)
        for addr in addrSet:
            self.put(b'addr_' + addr.serialize() + timestampKey + txHash, timestampValue)

    def putTx(self, tx, block, rootBlockHeader=None, txHash=None, consumedUtxoList=None):
        '''
        'block' is a root chain block if the tx is a root chain coinbase tx since such tx
        isn't included in any minor block. Otherwise it is always a minor block.
        '''
        txHash = tx.getHash() if txHash is None else txHash
        self.put(b'tx_' + txHash,
                 block.header.createTime.to_bytes(4, byteorder="big") + tx.serialize())
        self.put(b'txBlockHeader_' + txHash,
                 block.header.serialize())
        if rootBlockHeader is not None:
            self.put(b'txRootBlockHeader_' + txHash,
                     rootBlockHeader.serialize())
        for txIn in tx.inList:
            self.put(b'spent_' + txIn.serialize(), txHash)
        self.__putTxToAccounts(tx, txHash, block.header.createTime, consumedUtxoList)

    def getTxAndTimestamp(self, txHash):
        '''
        The timestamp returned is the createTime of the block that confirms the tx
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
            if 0 < limit <= done:
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

    def close(self):
        pass

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError("cannot find {}".format(key))
        return value


class InMemoryDb(Db):
    """ A simple in-memory key-value database
    TODO: Need to support range operation (e.g., leveldb)
    """

    def __init__(self):
        self.kv = dict()

    def rangeIter(self, start, end):
        keys = []
        for k in self.kv.keys():
            if k >= start and k <= end:
                keys.append(k)
        keys.sort()
        for k in keys:
            yield k, self.kv[k]

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
        for k, v in self.db.RangeIter(start, end):
            yield bytes(k), bytes(v)

    def get(self, key, default=None):
        try:
            return bytes(self.db.Get(key))
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

    def close(self):
        # No close option in leveldb?
        pass

    def __deepcopy__(self, memo):
        # LevelDB cannot be deep copied
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == "db":
                setattr(result, k, v)
                continue
            setattr(result, k, copy.deepcopy(v, memo))
        return result


@lru_cache(128)
def add1(b):
    v = int.from_bytes(b, byteorder="big")
    return (v + 1).to_bytes(4, byteorder="big")


@lru_cache(128)
def sub1(b):
    v = int.from_bytes(b, byteorder="big")
    return (v - 1).to_bytes(4, byteorder="big")


class RefcountedDb(Db):

    def __init__(self, db):
        self.db = db
        self.kv = None

    def get(self, key):
        return self.db.get(key)[4:]

    def getRefcount(self, key):
        try:
            return int.from_bytes(self.db.get(key)[:4], byteorder="big")
        except KeyError:
            return 0

    def rangeIter(self, start, end):
        for k, v in self.db.rangeIter(start, end):
            yield (k, v[4:])

    def put(self, key, value):
        existing = self.db.get(key)
        if existing is None:
            self.db.put(key, (1).to_bytes(4, byteorder="big") + value)
            return
        assert existing[4:] == value
        self.db.put(key, add1(existing[:4]) + value)

    def remove(self, key):
        existing = self.db.get(key)
        if existing[:4] == (1).to_bytes(4, byteorder="big"):
            self.db.remove(key)
        else:
            self.db.put(key, sub1(existing[:4]) + existing[4:])

    def commit(self):
        pass

    def _has_key(self, key):
        return key in self.db

    def __contains__(self, key):
        return self._has_key(key)


class ShardedDb(Db):

    def __init__(self, db, fullShardId):
        self.db = db
        self.fullShardId = fullShardId
        self.shardKey = fullShardId.to_bytes(4, byteorder="big")

    def get(self, key, default=None):
        return self.db.get(self.shardKey + key, default)

    def put(self, key, value):
        return self.db.put(self.shardKey + key, value)

    def remove(self, key):
        return self.db.remove(self.shardKey + key)

    def __contains__(self, key):
        return (self.shardKey + key) in self.db

    def rangeIter(self, start, end):
        for k, v in self.db.rangeIter(self.shardKey + start, self.shardKey + end):
            yield k[4:], v


class OverlayDb(Db):
    ''' Used for making temporary objects '''

    def __init__(self, db):
        self.db = db
        self.kv = None
        self.overlay = {}

    def get(self, key):
        if key in self.overlay:
            return self.overlay[key]
        return self.db.get(key)

    def put(self, key, value):
        self.overlay[key] = value

    def delete(self, key):
        self.overlay[key] = None

    def commit(self):
        pass

    def _has_key(self, key):
        if key in self.overlay:
            return self.overlay[key] is not None
        return key in self.db

    def __contains__(self, key):
        return self._has_key(key)


DB = InMemoryDb()
