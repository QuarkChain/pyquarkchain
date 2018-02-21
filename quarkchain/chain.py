#!/usr/bin/python3

from quarkchain.genesis import create_genesis_blocks
from quarkchain.core import calculate_merkle_root, TransactionInput, Transaction, Code
from quarkchain.core import MinorBlock
import copy
import time
from collections import deque
from quarkchain.utils import check
import random


class UtxoValue:

    def __init__(self, address, quarkash, rootBlockHeader):
        self.address = address
        self.quarkash = quarkash
        # Root block that requires to confirm the UTXO
        self.rootBlockHeader = rootBlockHeader


class MinorBlockRewardCalcultor:

    def __init__(self, env):
        self.env = env

    def getBlockReward(self, chain):
        return self.env.config.MINOR_BLOCK_DEFAULT_REWARD


class ShardState:
    """  State of a shard, which includes
    - UTXO pool
    - minor blockchain
    - root blockchain and cross-shard transaction
    And we can perform state change either by append new block or roll back a block
    TODO: Support
    - cross-shard transaction
    - reshard by split
    """

    def __init__(self, env, genesisBlock, rootChain):
        self.env = env
        self.db = env.db
        self.genesisBlock = genesisBlock
        self.utxoPool = dict()
        self.chain = [genesisBlock.header]
        genesisRootBlock = rootChain.getGenesisBlock()
        # TODO: Check shard id or disable genesisBlock
        self.utxoPool[TransactionInput(genesisBlock.txList[0].getHash(), 0)] = UtxoValue(
            genesisBlock.txList[0].outList[0].address,
            genesisBlock.txList[0].outList[0].quarkash,
            genesisRootBlock.header)
        self.db.putTx(genesisBlock.txList[0], rootBlockHeader=genesisRootBlock)

        self.branch = self.genesisBlock.header.branch
        self.rootChain = rootChain
        self.diffCalc = self.env.config.MINOR_DIFF_CALCULATOR
        self.diffHashFunc = self.env.config.DIFF_HASH_FUNC
        self.rewardCalc = MinorBlockRewardCalcultor(env)

        grCoinbaseTx = rootChain.getGenesisBlock().coinbaseTx
        if self.branch.isInShard(grCoinbaseTx.outList[0].address.fullShardId):
            self.utxoPool[TransactionInput(grCoinbaseTx.getHash(), 0)] = UtxoValue(
                grCoinbaseTx.outList[0].address,
                grCoinbaseTx.outList[0].quarkash,
                genesisRootBlock.header)
            self.db.putTx(grCoinbaseTx, rootBlockHeader=genesisRootBlock)

        self.txQueue = deque()

    def __checkTx(self, tx, utxoPool):
        if len(tx.inList) == 0:
            return -1

        # Make sure all tx ids from inputs:
        # - are unique; and
        # - exist in utxo pool; and
        # - depend before rootBlockHeader (inclusive)
        txInputSet = set()
        txInputQuarkash = 0
        senderList = []
        rootBlockHeader = self.chain[0]
        for txInput in tx.inList:
            if txInput in txInputSet:
                raise RuntimeError("transaction input cannot be used twice")
            if txInput not in utxoPool:
                raise RuntimeError("transaction input hash doesn't exist in UTXO pool")
            if utxoPool[txInput].rootBlockHeader.height > rootBlockHeader.height:
                rootBlockHeader = utxoPool[txInput].rootBlockHeader
            txInputSet.add(txInput)
            txInputQuarkash = utxoPool[txInput].quarkash
            senderList.append(utxoPool[txInput].address.recipient)

        # Check signature
        if not tx.verifySignature(senderList):
            raise RuntimeError("incorrect signature")

        # Check if the sum of output is smaller than or equal to the input
        txOutputQuarkash = 0
        for txOut in tx.outList:
            txOutputQuarkash += txOut.quarkash
        if txOutputQuarkash > txInputQuarkash:
            raise RuntimeError("output quarkash cannot exceed input one")

        return (txInputQuarkash - txOutputQuarkash, rootBlockHeader)

    def __doPerformTx(self, tx, rootBlockHeader, utxoPool):
        for txInput in tx.inList:
            del utxoPool[txInput]

        txHash = tx.getHash()
        for idx, txOutput in enumerate(tx.outList):
            if not self.branch.isInShard(txOutput.address.fullShardId):
                continue
            utxoPool[TransactionInput(txHash, idx)] = UtxoValue(
                txOutput.address,
                txOutput.quarkash,
                rootBlockHeader)

        self.db.putTx(tx, rootBlockHeader=rootBlockHeader, txHash=txHash)

    def __performTx(self, tx, rootBlockHeader, utxoPool=None):
        """ Perform a transacton atomically.
        Return -1 if the transaction is invalid or
               >= 0 for the transaction fee if the transaction successfully executed.
        """

        utxoPool = self.utxoPool if utxoPool is None else utxoPool

        txFee, prevRootBlockHeader = self.__checkTx(tx, utxoPool)

        if prevRootBlockHeader.height > rootBlockHeader.height:
            raise RuntimeError("root block header's height is too small")

        self.__doPerformTx(tx, rootBlockHeader, utxoPool)
        return txFee

    def __rollBackTx(self, tx):
        txHash = tx.getHash()
        for i in range(len(tx.outList)):
            # Don't roll back cross-shard TX
            if not self.branch.isInShard(tx.outList[i].address.fullShardId):
                continue
            del self.utxoPool[TransactionInput(txHash, i)]

        for txInput in tx.inList:
            prevTx = self.db.getTx(txInput.hash)
            rootBlockHeader = self.db.getTxRootBlockHeader(txInput.hash)
            self.utxoPool[txInput] = UtxoValue(
                prevTx.outList[txInput.index].address,
                prevTx.outList[txInput.index].quarkash,
                rootBlockHeader)
        return None

    def appendBlock(self, block):
        """  Append a block.  This would perform validation check with local
        UTXO pool and perform state change atomically
        Return None upon success, otherwise return a string with error message
        """

        # TODO: May check if the block is already in db (and thus already
        # validated)

        if block.header.hashPrevMinorBlock != self.chain[-1].getHash():
            return "prev hash mismatch"

        if block.header.height != self.chain[-1].height + 1:
            return "height mismatch"

        if block.header.branch != self.branch:
            return "branch mismatch"

        if block.header.createTime <= self.chain[-1].createTime:
            return "incorrect create time tip time {}, new block time {}".format(
                block.header.createTime, self.chain[-1].createTime)

        # Make sure merkle tree is valid
        merkleHash = calculate_merkle_root(block.txList)
        if merkleHash != block.header.hashMerkleRoot:
            return "incorrect merkle root"

        # Check the first transaction of the block
        if len(block.txList) == 0:
            return "coinbase tx must exist"

        if len(block.txList[0].inList) != 0:
            return "coinbase tx's input must be empty"

        # TODO: Support multiple outputs in the coinbase tx
        if len(block.txList[0].outList) != 1:
            return "coinbase tx's output must be one"

        if not self.branch.isInShard(block.txList[0].outList[0].address.fullShardId):
            return "coinbase output address must be in the shard"

        # Check difficulty
        if not self.env.config.SKIP_MINOR_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == 0:
                diff = self.getNextBlockDifficulty(block.header.createTime)
                metric = diff * int.from_bytes(block.header.getHash(), byteorder="big")
                if metric >= 2 ** 256:
                    return "incorrect difficulty"
            elif block.txList[0].outList[0].address.recipient != self.env.config.TESTNET_MASTER_ACCOUNT.recipient:
                return "incorrect master to create the block"

        if not self.branch.isInShard(block.txList[0].outList[0].address.fullShardId):
            return "coinbase output must be in local shard"

        if block.txList[0].code != Code.createMinorBlockCoinbaseCode(block.header.height, block.header.branch):
            return "incorrect coinbase code"

        # Check whether the root header is in the root chain
        rootBlockHeader = self.rootChain.getBlockHeaderByHash(
            block.header.hashPrevRootBlock)
        if rootBlockHeader is None:
            return "cannot find root block for the minor block"

        if rootBlockHeader.height < self.rootChain.getBlockHeaderByHash(self.chain[-1].hashPrevRootBlock).height:
            return "prev root block height must be non-decreasing"

        txDoneList = []
        totalFee = 0
        for tx in block.txList[1:]:
            try:
                fee = self.__performTx(tx, rootBlockHeader)
            except Exception as e:
                for rTx in reversed(txDoneList):
                    rollBackResult = self.__rollBackTx(rTx)
                    assert(rollBackResult is None)
                return str(e)
            totalFee += fee
            txDoneList.append(tx)

        # The rest fee goes to root block
        if not self.env.config.SKIP_MINOR_COINBASE_CHECK and \
                block.txList[0].outList[0].quarkash > totalFee // 2 + self.rewardCalc.getBlockReward(self):
            for rTx in reversed(txDoneList):
                rollBackResult = self.__rollBackTx(rTx)
                assert(rollBackResult is None)
            return "coinbase reward is greater than block reward + fee"

        txHash = block.txList[0].getHash()
        for idx, txOutput in enumerate(block.txList[0].outList):
            self.utxoPool[TransactionInput(txHash, idx)] = UtxoValue(
                txOutput.address,
                txOutput.quarkash,
                rootBlockHeader)

        self.db.putTx(block.txList[0], rootBlockHeader)
        self.db.putMinorBlock(block)
        self.chain.append(block.header)

        # TODO: invalidate consumed tx in txQueue
        return None

    def printUtxoPool(self):
        for k, v in self.utxoPool.items():
            print("%s, %s, %s" % (k.hash.hex(), k.index, v.quarkash))

    def rollBackTip(self):
        if len(self.chain) == 1:
            return "Cannot roll back genesis block"

        blockHeader = self.chain[-1]
        block = self.db.getMinorBlockByHash(blockHeader.getHash())
        del self.chain[-1]
        for rTx in reversed(block.txList[1:]):
            rollBackResult = self.__rollBackTx(rTx)
            assert(rollBackResult is None)

        txHash = block.txList[0].getHash()
        for idx in range(len(block.txList[0].outList)):
            del self.utxoPool[TransactionInput(txHash, idx)]

        return None

        # Don't need to remove db data

    def tip(self):
        """ Return the header of the tail of the shard
        """
        return self.chain[-1]

    def addCrossShardUtxo(self, txInput, utxoValue):
        assert(txInput not in self.utxoPool)
        self.utxoPool[txInput] = utxoValue

    def removeCrossShardUtxo(self, txInput):
        del self.utxoPool[txInput]

    def getBlockHeaderByHeight(self, height):
        return self.chain[height]

    def getGenesisBlock(self):
        return self.genesisBlock

    def getBalance(self, recipient):
        balance = 0
        for k, v in self.utxoPool.items():
            if v.address.recipient != recipient:
                continue

            balance += v.quarkash
        return balance

    def getNextBlockDifficulty(self, createTime):
        return self.diffCalc.calculateDiff(self, createTime)

    def getNextBlockReward(self):
        return self.rewardCalc.getBlockReward(self)

    def createBlockToAppend(self, createTime=None, address=None):
        """ Create an empty block to append
        """
        createTime = int(time.time()) if createTime is None else createTime
        return self.tip().createBlockToAppend(
            createTime=createTime,
            address=address,
            difficulty=self.getNextBlockDifficulty(createTime),
            quarkash=self.getNextBlockReward())

    def createBlockToMine(self, createTime=None, address=None):
        """ Create a block to append and include TXs to maximize rewards
        """
        block = self.createBlockToAppend(
            createTime=createTime, address=address)
        utxoPool = copy.copy(self.utxoPool)
        totalTxFee = 0
        invalidTxList = []
        for tx in self.txQueue:
            if len(block.txList) >= self.env.config.TRANSACTION_LIMIT_PER_BLOCK:
                break

            try:
                txFee, rootBlockHeader = self.__checkTx(tx, utxoPool)
            except Exception as e:
                # TODO: C++ style erase while iterating?
                invalidTxList.append(tx)
                continue

            totalTxFee += txFee
            self.__doPerformTx(tx, rootBlockHeader, utxoPool)
            block.addTx(tx)
        for tx in invalidTxList:
            self.txQueue.remove(tx)
        block.txList[0].outList[0].quarkash += totalTxFee
        return block.finalizeMerkleRoot()

    def addTransactionToQueue(self, transaction):
        # TODO: limit transaction queue size

        self.txQueue.append(transaction)

    def getUtxoPool(self):
        # TODO: May just return a copy
        return self.utxoPool


class MinorChainManager:

    def __init__(self, env):
        self.env = env
        self.db = env.db
        self.rootChain = None
        self.blockPool = dict()  # hash to block header

        tmp, self.genesisBlockList = create_genesis_blocks(env)

        for mBlock in self.genesisBlockList:
            mHash = mBlock.header.getHash()
            self.db.put(b'mblock_' + mHash, mBlock.serialize())
            self.blockPool[mHash] = mBlock.header

    def setRootChain(self, rootChain):
        assert(self.rootChain is None)
        self.rootChain = rootChain

    def checkValidationByHash(self, h):
        return h in self.blockPool

    def getBlockHeader(self, h):
        return self.blockPool.get(h)

    def getBlock(self, h):
        data = self.db.get(h)
        if data is None:
            return None
        return MinorBlock.deserialize(data)

    def getGenesisBlock(self, shardId):
        return self.genesisBlockList[shardId]

    def addNewBlock(self, block):
        # TODO: validate the block
        blockHash = block.header.getHash()
        self.blockPool[blockHash] = block.header
        self.db.put(b'mblock_' + blockHash, block.serialize())
        self.db.put(b'mblockCoinbaseTx_' + blockHash,
                    block.txList[0].serialize())
        return None

    def getBlockCoinbaseTx(self, blockHash):
        return Transaction.deserialize(self.db.get(b'mblockCoinbaseTx_' + blockHash))

    def getBlockCoinbaseQuarkash(self, blockHash):
        return self.getBlockCoinbaseTx(blockHash).outList[0].quarkash


def get_minor_block_coinbase_tx(db, blockHash):
    return Transaction.deserialize(db.get(b'mblockCoinbaseTx_' + blockHash))


def get_minor_block_coinbase_quarkash(db, blockHash):
    return get_minor_block_coinbase_tx(db, blockHash).outList[0].quarkash


class RootChain:

    def __init__(self, env, genesisBlock=None):
        self.env = env
        self.db = env.db
        self.blockPool = dict()

        # Create genesis block if not exist
        block = genesisBlock
        if block is None:
            block, tmp = create_genesis_blocks(env)

        h = block.header.getHash()
        if b'rblock_' + h not in self.db:
            self.db.put(b'rblock_' + h, block.serialize())
        self.blockPool[h] = block.header
        self.genesisBlock = block
        self.chain = [block.header]
        self.diffCalc = self.env.config.ROOT_DIFF_CALCULATOR
        self.diffHashFunc = self.env.config.DIFF_HASH_FUNC

    def loadFromDb(self):
        # TODO
        pass

    def tip(self):
        return self.chain[-1]

    def getGenesisBlock(self):
        return self.genesisBlock

    def containBlockByHash(self, h):
        return h in self.blockPool

    def getBlockHeaderByHash(self, h):
        return self.blockPool.get(h, None)

    def getBlockHeaderByHeight(self, height):
        return self.chain[height]

    def rollBack(self):
        if len(self.chain) == 1:
            return "cannot roll back genesis block"
        del self.blockPool[self.chain[-1].getHash()]
        del self.chain[-1]
        return None

    def __checkCoinbaseTx(self, tx, height):
        if len(tx.inList) != 0:
            return False

        if tx.code != Code.createRootBlockCoinbaseCode(height):
            return False

        # We only support one output for coinbase tx
        if len(tx.outList) != 1:
            return False

        return True

    def __getBlockCoinbaseTx(self, blockHash):
        return get_minor_block_coinbase_tx(self.db, blockHash)

    def __getBlockCoinbaseQuarkash(self, blockHash):
        return get_minor_block_coinbase_quarkash(self.db, blockHash)

    def appendBlock(self, block, uncommittedMinorBlockHeaderQueueList):
        """ Append new block.
        There are a couple of optimizations can be done here:
        - the root block could only contain minor block header hashes as long as the shards fully validate the headers
        - the header (or hashes) are un-ordered as long as they contains valid sub-chains from previous root block
        """

        if block.header.hashPrevBlock != self.chain[-1].getHash():
            return "previous hash block mismatch"

        if block.header.height != len(self.chain):
            return "height mismatch"

        if block.header.createTime <= self.chain[-1].createTime:
            return "incorrect create time tip time {}, new block time {}".format(
                block.header.createTime, self.chain[-1].createTime)

        if block.header.hashCoinbaseTx != block.coinbaseTx.getHash():
            return "coinbase tx hash mismatch"

        if not self.__checkCoinbaseTx(block.coinbaseTx, block.header.height):
            return "incorrect coinbase tx"

        blockHash = block.header.getHash()

        # Check the merkle tree
        merkleHash = calculate_merkle_root(block.minorBlockHeaderList)
        if merkleHash != block.header.hashMerkleRoot:
            return "incorrect merkle root"

        # Check difficulty
        if not self.env.config.SKIP_ROOT_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == 0:
                diff = self.getNextBlockDifficulty(block.header.createTime)
                metric = diff * int.from_bytes(blockHash, byteorder="big")
                if metric >= 2 ** 256:
                    return "insufficient difficulty"
            elif block.coinbaseTx.outList[0].address.recipient != self.env.config.TESTNET_MASTER_ACCOUNT.recipient:
                return "incorrect master to create the block"

        # Check whether all minor blocks are ordered, validated (and linked to previous block)
        # Find the last block of previous block
        shardId = 0
        newQueueList = []
        q = copy.copy(uncommittedMinorBlockHeaderQueueList[shardId])
        blockCountInShard = 0
        totalMinorCoinbase = 0
        for mHeader in block.minorBlockHeaderList:
            if mHeader.branch.getShardId() != shardId:
                if mHeader.branch.getShardId() != shardId + 1:
                    return "shard id must be ordered"
                if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                    return "fail to prove progress"
                newQueueList.append(q)
                shardId += 1
                q = copy.copy(uncommittedMinorBlockHeaderQueueList[shardId])
                blockCountInShard = 0

            if len(q) == 0 or q.popleft() != mHeader:
                return "minor block doesn't link to previous minor block"
            blockCountInShard += 1
            totalMinorCoinbase += self.__getBlockCoinbaseQuarkash(
                mHeader.getHash())

        if shardId != block.header.shardInfo.getShardSize() - 1 and self.env.config.PROOF_OF_PROGRESS_BLOCKS != 0:
            return "fail to prove progress"
        if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
            return "fail to prove progress"
        newQueueList.append(q)

        # Check the coinbase value is valid (we allow burning coins)
        if block.coinbaseTx.outList[0].quarkash > totalMinorCoinbase:
            return "incorrect coinbase quarkash"

        # Add the block hash to block header to memory pool and add the block
        # to db
        self.blockPool[blockHash] = block.header
        self.chain.append(block.header)
        self.db.putRootBlock(block, rBlockHash=blockHash)

        # Set new uncommitted blocks
        for shardId in range(min(block.header.shardInfo.getShardSize(), len(newQueueList))):
            uncommittedMinorBlockHeaderQueueList[shardId] = \
                newQueueList[shardId]

        return None

    def getNextBlockDifficulty(self, timeSec):
        return self.diffCalc.calculateDiff(self, timeSec)


class QuarkChain:

    def __init__(self, env):
        self.minorChainManager = MinorChainManager(env)
        self.rootChain = RootChain(env)
        self.minorChainManager.setRootChain(self.rootChain)


class QuarkChainState:
    """ TODO: Support reshard
    """

    def __init__(self, env):
        self.env = env
        self.db = env.db
        rBlock, mBlockList = create_genesis_blocks(env)
        self.rootChain = RootChain(env, rBlock)
        self.shardList = [ShardState(env, mBlock, self.rootChain)
                          for mBlock in mBlockList]
        self.blockToCrossShardUtxoMap = dict()
        self.uncommittedMinorBlockHeaderQueueList = [
            deque() for shard in self.shardList]

    def __addCrossShardTxFrom(self, mBlock, rBlock):
        shardSize = len(self.shardList)
        for tx in mBlock.txList[1:]:
            txHash = tx.getHash()
            for idx, txOutput in enumerate(tx.outList):
                shardId = txOutput.address.fullShardId & (shardSize - 1)
                if shardId == mBlock.header.branch.getShardId():
                    continue
                self.shardList[shardId].addCrossShardUtxo(
                    TransactionInput(txHash, idx),
                    UtxoValue(
                        txOutput.address,
                        txOutput.quarkash,
                        rBlock.header))

    def __removeCrossShardTxFrom(self, mBlock):
        shardSize = len(self.shardList)
        for tx in mBlock.txList[1:]:
            txHash = tx.getHash()
            for idx, txOutput in enumerate(tx.outList):
                shardId = txOutput.address.fullShardId & (shardSize - 1)
                if shardId == mBlock.header.branch.getShardId():
                    continue
                self.shardList[shardId].removeCrossShardUtxo(
                    TransactionInput(txHash, idx))

    def appendMinorBlock(self, mBlock):
        if mBlock.header.branch.getShardSize() != len(self.shardList):
            return "minor block shard size is too large"

        appendResult = self.shardList[
            mBlock.header.branch.getShardId()].appendBlock(mBlock)
        if appendResult is not None:
            return appendResult

        self.uncommittedMinorBlockHeaderQueueList[
            mBlock.header.branch.getShardId()].append(mBlock.header)
        return None

    def rollBackMinorBlock(self, shardId):
        """ Roll back a minor block of a shard.
        The minor block must not be commited by root blocks.
        """
        if shardId > len(self.shardList):
            return "shard id is too large"

        if len(self.uncommittedMinorBlockHeaderQueueList[shardId]) == 0:
            """ Root block already commits the minor blocks.
            Need to roll back root block before rolling back the minor block.
            """
            return "the minor block is commited by root block"
        shard = self.shardList[shardId]
        check(self.uncommittedMinorBlockHeaderQueueList[
              shardId].pop() == shard.tip())
        return shard.rollBackTip()

    def getShardTip(self, shardId):
        if shardId > len(self.shardList):
            raise RuntimeError("shard id not exist")

        shard = self.shardList[shardId]
        return shard.tip()

    def getShardSize(self):
        return len(self.shardList)

    def appendRootBlock(self, rBlock):
        """ Append a root block to rootChain
        """
        appendResult = self.rootChain.appendBlock(
            rBlock, self.uncommittedMinorBlockHeaderQueueList)
        if appendResult is not None:
            return appendResult

        for mHeader in rBlock.minorBlockHeaderList:
            mBlock = self.db.getMinorBlockByHash(mHeader.getHash())
            self.__addCrossShardTxFrom(mBlock, rBlock)

        return None

        # TODO: Add root block coinbase tx

    def rollBackRootBlock(self):
        """ Roll back a root block in rootChain
        """
        rBlockHeader = self.rootChain.tip()
        rBlockHash = rBlockHeader.getHash()
        rBlock = self.db.getRootBlockByHash(rBlockHash)
        for uncommittedQueue in self.uncommittedMinorBlockHeaderQueueList:
            if len(uncommittedQueue) == 0:
                continue

            mHeader = uncommittedQueue[-1]
            if mHeader.hashPrevRootBlock == rBlockHash:
                # Cannot roll back the root block since it is being used.
                return "the root block is used by uncommitted minor blocks"

        result = self.rootChain.rollBack()
        if result is not None:
            return result

        for mHeader in reversed(rBlock.minorBlockHeaderList):
            self.uncommittedMinorBlockHeaderQueueList[
                mHeader.branch.getShardId()].appendleft(mHeader)
            self.__removeCrossShardTxFrom(
                self.db.getMinorBlockByHash(mHeader.getHash()))

        return None

        # TODO: Remove root block coinbase tx

    def rollBackRootChainTo(self, rBlockHeader):
        """ Roll back the root chain to a specific block header
        Return None upon success or error message upon failure
        """

        # TODO: Optimize with pqueue
        blockHash = rBlockHeader.getHash()
        if self.rootChain.getBlockHeaderByHash(blockHash) is None:
            return "cannot find the root block in root chain"

        while self.rootChain.tip() != rBlockHeader:
            # Roll back minor blocks
            for shardId, q in enumerate(self.uncommittedMinorBlockHeaderQueueList):
                while len(q) > 0 and q[-1].height > rBlockHeader.height:
                    check(self.rollBackMinorBlock(shardId) is None)
            check(self.rollBackRootBlock() is None)
        return None

    def getMinorBlockHeaderByHeight(self, shardId, height):
        return self.shardList[shardId].getBlockHeaderByHeight(height)

    def getRootBlockHeaderByHeight(self, height):
        return self.rootChain.getBlockHeaderByHeight(height)

    def getGenesisMinorBlock(self, shardId):
        return self.shardList[shardId].getGenesisBlock()

    def getGenesisRootBlock(self):
        return self.rootChain.getGenesisBlock()

    def getRootBlockTip(self):
        return self.rootChain.tip()

    def getMinorBlockTip(self, shardId):
        return self.shardList[shardId].tip()

    def copy(self):
        """ Return a copy of the state.
        TODO: Optimize copy
        """
        return copy.deepcopy(self)

    def getBalance(self, recipient):
        balance = 0
        for shard in self.shardList:
            balance += shard.getBalance(recipient)

        return balance

    def getNextMinorBlockDifficulty(self, shardId, createTime=None):
        if shardId >= len(self.shardList):
            raise RuntimeError("invalid shard id")

        createTime = int(time.time()) if createTime is None else createTime
        shard = self.shardList[shardId]
        return shard.getNextBlockDifficulty(createTime)

    def getNextRootBlockDifficulty(self, createTime=None):
        createTime = int(time.time()) if createTime is None else createTime
        return self.rootChain.getNextBlockDifficulty(createTime)

    def createMinorBlockToAppend(self, shardId, createTime=None, address=None):
        if shardId >= len(self.shardList):
            raise RuntimeError("invalid shard id")
        return self.shardList[shardId].createBlockToAppend(
            createTime=createTime, address=address)

    def createRootBlockToAppend(self, createTime=None, address=None):
        createTime = int(time.time()) if createTime is None else createTime
        diff = self.getNextRootBlockDifficulty(createTime)
        return self.rootChain.tip().createBlockToAppend(
            createTime=createTime, difficulty=diff, address=address)

    def createRootBlockToMine(self, createTime=None, address=None):
        rBlock = self.createRootBlockToAppend(
            createTime=createTime, address=address)
        totalReward = 0
        for q in self.uncommittedMinorBlockHeaderQueueList:
            for mHeader in q:
                rBlock.addMinorBlockHeader(mHeader)
                totalReward += get_minor_block_coinbase_quarkash(
                    self.db,
                    mHeader.getHash())

        return rBlock.finalize(quarkash=totalReward)

    def createMinorBlockToMine(self, shardId, createTime=None, address=None):
        if shardId >= len(self.shardList):
            raise RuntimeError("invalid shard id")
        return self.shardList[shardId].createBlockToMine(
            createTime=createTime, address=address)

    def getNextMinorBlockReward(self, shardId):
        if shardId >= len(self.shardList):
            raise RuntimeError("invalid shard id")

        return self.shardList[shardId].getNextBlockReward()

    def getNextRootBlockReward(self):
        totalReward = 0
        for q in self.uncommittedMinorBlockHeaderQueueList:
            for mHeader in q:
                totalReward += get_minor_block_coinbase_quarkash(
                    self.db, mHeader.getHash())
        return totalReward

    def addTransactionToQueue(self, shardId, transaction):
        if shardId > len(self.shardList):
            raise RuntimeError("invalid shard id")

        self.shardList[shardId].addTransactionToQueue(transaction)

    def findBestBlockToMine(self,
                            includeRoot=True,
                            shardMaskList=[],
                            createTime=None,
                            address=None,
                            randomizeOutput=True):
        """ Find the best block (reward / diff) to mine
        Return None if no such block is found
        """
        createTime = int(time.time()) if createTime is None else createTime
        if includeRoot:
            blockId = 0
            maxEco = self.getNextRootBlockReward() / self.getNextRootBlockDifficulty(createTime)
        else:
            blockId = None
            maxEco = None

        # TODO: Apply shard mask
        dupEcoCount = 1
        blockHeight = 0
        for shardId, shard in enumerate(self.shardList):
            eco = shard.getNextBlockReward() / shard.getNextBlockDifficulty(createTime)
            if maxEco is None or eco > maxEco or \
                    (eco == maxEco and blockId > 0 and blockHeight > shard.tip().height):
                blockId = shardId + 1
                maxEco = eco
                dupEcoCount = 1
                blockHeight = shard.tip().height
            elif eco == maxEco and randomizeOutput:
                # The current block with max eco has smaller height, mine the block first
                # This should be only used during bootstrap.
                if blockId > 0 and blockHeight < shard.tip().height:
                    continue
                dupEcoCount += 1
                if random.random() < 1 / dupEcoCount:
                    blockId = shardId + 1
                    maxEco = eco

        if blockId == 0:
            # Double check if we meet proof-of-progress
            for shardId, q in enumerate(self.uncommittedMinorBlockHeaderQueueList):
                if len(q) < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                    return (False, self.createMinorBlockToMine(
                        shardId, createTime=createTime, address=address))
            return (True, self.createRootBlockToMine(
                createTime=createTime, address=address))
        else:
            return (False, self.createMinorBlockToMine(
                blockId - 1, createTime=createTime, address=address))

    def getUtxoPool(self, shardId):
        return self.shardList[shardId].getUtxoPool()
