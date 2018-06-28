#!/usr/bin/python3
import copy
import pathlib
import rocksdb
import shutil

from quarkchain.core import Constant, MinorBlock, RootBlock, RootBlockHeader, Transaction


class DbOpsMixin:
    """DEPRECATED. This was only used for the old UTXO based testnet."""

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
        """
        'block' is a root chain block if the tx is a root chain coinbase tx since such tx
        isn't included in any minor block. Otherwise it is always a minor block.
        """
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
        """
        The timestamp returned is the createTime of the block that confirms the tx
        """
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


class Db:

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError("cannot find {}".format(key))
        return value

    def close(self):
        pass


class InMemoryDb(Db, DbOpsMixin):
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
    def __init__(self, db_path, clean=False):
        self.db_path = db_path
        if clean:
            self._destroy()
        pathlib.Path(self.db_path).mkdir(parents=True, exist_ok=True)


        options = rocksdb.Options()
        options.create_if_missing = True
        options.max_open_files = 100000 # ubuntu 16.04 max files descriptors 524288
        options.write_buffer_size = 128 * 1024 * 1024  # 128 MiB
        options.max_write_buffer_number = 3
        options.target_file_size_base = 67108864
        options.compression = rocksdb.CompressionType.lz4_compression

        self._db = rocksdb.DB(db_path, options)

    def _destroy(self):
        shutil.rmtree(self.db_path, ignore_errors=True)

    def get(self, key, default=None):
        key = key.encode() if not isinstance(key, bytes) else key
        value = self._db.get(key)
        return default if value is None else value

    def multi_get(self, keys):
        keys = [k.encode() if not isinstance(k, bytes) else k for k in keys]
        return self._db.multi_get(keys) # returns a dict with keys as keys

    def put(self, key, value):
        key = key.encode() if not isinstance(key, bytes) else key
        value = bytes(value) if isinstance(value, bytearray) else value
        return self._db.put(key, value)

    def delete(self, key):
        key = key.encode() if not isinstance(key, bytes) else key
        return self._db.delete(key)

    def remove(self, key):
        return self.delete(key)

    def __contains__(self, key):
        key = key.encode() if not isinstance(key, bytes) else key
        return self._db.get(key) is not None

    def rangeIter(self, start, end):
        raise NotImplementedError

    def close(self):
        # No close() available for rocksdb
        # see https://github.com/twmht/python-rocksdb/issues/10
        pass

    def __deepcopy__(self, memo):
        raise NotImplementedError
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


class OverlayDb(Db):
    """ Used for making temporary objects in EvmState.ephemeral_clone()"""

    def __init__(self, db):
        self._db = db
        self.kv = None
        self.overlay = {}

    def get(self, key):
        if key in self.overlay:
            return self.overlay[key]
        return self._db.get(key)

    def put(self, key, value):
        self.overlay[key] = value

    def delete(self, key):
        self.overlay[key] = None

    def commit(self):
        pass

    def _has_key(self, key):
        if key in self.overlay:
            return self.overlay[key] is not None
        return self._db.get(key) is not None

    def __contains__(self, key):
        return self._has_key(key)


class ShardedDb(Db, DbOpsMixin):
    """DEPRECATED. TODO: remove these later, leaving in for not breaking legacy tests"""
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
