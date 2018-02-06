#!/usr/bin/python3

from quarkchain.genesis import create_genesis_blocks
from quarkchain.core import calculate_merkle_root, RootBlock, MinorBlock, Transaction


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
        self.db.put(b'mblockCoinbaseTx_' + blockHash, block.txList[0].serialize())
        return True

    def getBlockCoinbaseTx(self, blockHash):
        return Transaction.deserialize(self.db.get(b'mblockCoinbaseTx_' + blockHash))

    def getBlockCoinbaseQuarkash(self, blockHash):
        return self.getBlockCoinbaseTx(blockHash).outList[0].quarkash


class RootChain:

    def __init__(self, env):
        self.env = env
        self.db = env.db
        self.minorChainManager = None
        self.blockPool = dict()

        # Create genesis block if not exist
        block, tmp = create_genesis_blocks(env)
        h = block.header.getHash()
        if b'rblock_' + h not in self.db:
            self.db.put(b'rblock_' + h, block.serialize())
        self.blockPool[h] = block.header
        self.tip = block
        self.genesisBlock = block

    def setMinorChainManager(self, manager):
        assert(self.minorChainManager is None)
        self.minorChainManager = manager

    def loadFromDb(self):
        # TODO
        pass

    def tip(self):
        return self.tip

    def getGenesisBlock(self):
        return self.genesisBlock

    def addNewBlock(self, block):
        """ Add new block.  The block doesn't not necessarily be appended to
        the end of the chain.  However, if the block is the longest, then
        will be updated
        There are a couple of optimizations can be done here:
        - the root block could only contain minor block header hashes as long as the shards fully validate the headers
        - the header (or hashes) are un-ordered as long as they contains valid sub-chains from previous root block
        """

        # Check whether the block is already added
        blockHash = block.header.getHash()
        if blockHash in self.blockPool:
            return True

        # Check whether previous block is in the pool
        if block.header.hashPrevBlock not in self.blockPool:
            return False
        prevBlock = RootBlock.deserialize(self.db.get(
            b'rblock_' + block.header.hashPrevBlock))

        # Check the merkle tree
        merkleHash = calculate_merkle_root(block.minorBlockHeaderList)
        if merkleHash != block.header.hashMerkleRoot:
            return False

        # Check difficulty
        if not self.env.config.SKIP_ROOT_DIFFICULTY_CHECK:
            # TOOD: Implement difficulty
            return False

        # Check whether all minor blocks are validated
        for mheader in block.minorBlockHeaderList:
            if not self.minorChainManager.checkValidationByHash(mheader.getHash()):
                return False
            # Check shard size matches
            if mheader.branch.getShardSize() != block.header.shardInfo.getShardSize():
                return False

        # Check whether all minor blocks are ordered (and linked to previous block)
        # Find the last block of previous block
        shardId = 0
        lastBlockHashList = []
        prevHeader = prevBlock.minorBlockHeaderList[0]
        for mheader in prevBlock.minorBlockHeaderList:
            if shardId != mheader.branch.getShardId():
                assert(shardId + 1 == mheader.branch.getShardId())
                lastBlockHashList.append(prevHeader.getHash())
            prevHeader = mheader
        lastBlockHashList.append(prevBlock.minorBlockHeaderList[-1].getHash())
        assert(len(lastBlockHashList) ==
               prevBlock.header.shardInfo.getShardSize())

        shardId = 0
        prevHeader = block.minorBlockHeaderList[0]
        blockCountInShard = 1
        if prevHeader.branch.getShardId() != 0:
            return False
        if prevHeader.hashPrevMinorBlock != lastBlockHashList[0]:
            return False

        totalMinorCoinbase = self.minorChainManager.getBlockCoinbaseQuarkash(block.minorBlockHeaderList[0].getHash())
        for mheader in block.minorBlockHeaderList[1:]:
            totalMinorCoinbase += self.minorChainManager.getBlockCoinbaseQuarkash(mheader.getHash())
            if mheader.branch.getShardId() == shardId:
                # Check if all minor blocks are linked in the shard
                if mheader.hashPrevMinorBlock != prevHeader.getHash():
                    return False
                blockCountInShard += 1
            elif mheader.branch.getShardId() != shardId + 1:
                # Shard id is unordered
                return False
            else:
                if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                    return False
                # New shard is found in the list
                shardId = mheader.branch.getShardId()
                if mheader.hashPrevMinorBlock != lastBlockHashList[shardId]:
                    return False

            prevHeader = mheader
        if shardId != block.header.shardInfo.getShardSize() - 1:
            return False
        if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
            return False

        # Check the coinbase value is valid (we allow burning coins)
        if block.header.coinbaseValue > totalMinorCoinbase:
            return False

        # Add the block hash to block header to memory pool and add the block
        # to db
        self.blockPool[blockHash] = block.header
        self.db.put(b"rblock_" + blockHash, block.serialize())

        if block.header.height > self.tip.header.height:
            # Switch tip
            self.tip = block
            # TODO switch shard tips
        return True


class QuarkChain:

    def __init__(self, env):
        self.minorChainManager = MinorChainManager(env)
        self.rootChain = RootChain(env)
        self.minorChainManager.setRootChain(self.rootChain)
        self.rootChain.setMinorChainManager(self.minorChainManager)
