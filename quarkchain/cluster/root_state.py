import time

from quarkchain.cluster.genesis import create_genesis_blocks, create_genesis_evm_list
from quarkchain.config import NetworkId
from quarkchain.core import RootBlock, MinorBlockHeader
from quarkchain.core import calculate_merkle_root, Serializable, PrependedSizeListSerializer
from quarkchain.utils import Logger


class LastMinorBlockHeaderList(Serializable):
    FIELDS = [
        ("headerList", PrependedSizeListSerializer(4, MinorBlockHeader))
    ]

    def __init__(self, headerList):
        self.headerList = headerList


class RootDb:
    """ Storage for all validated root blocks and minor blocks

    On initialization it will try to recover the recent blocks (maxNumBlocksToRecover) of the best chain
    from local database.
    Block referenced by "tipHash" is skipped because it's consistency is not guaranteed within the cluster.
    Note that we only recover the blocks on the best chain than including the forking blocks because
    we don't save "tipHash"s for the forks and thus their consistency state is hard to reason about.
    They can always be downloaded again from peers if they ever became the best chain.

    """

    def __init__(self, db, maxNumBlocksToRecover):
        # TODO: evict old blocks from memory
        self.db = db
        self.maxNumBlocksToRecover = maxNumBlocksToRecover
        # TODO: May store locally to save memory space (e.g., with LRU cache)
        self.mHashSet = set()
        self.rHeaderPool = dict()
        self.tipHeader = None

        self.__recoverFromDb()

    def __recoverFromDb(self):
        ''' Recover the best chain from local database.
        '''
        Logger.info("Recovering root chain from local database...")

        if b"tipHash" not in self.db:
            return None

        # The block referenced by tipHash might not have been fully propagated within the cluster
        # when the cluster was down, but its parent is guaranteed to have been accepted by all shards.
        # Therefore we use its parent as the new tip.
        rHash = self.db.get(b"tipHash")
        rBlock = RootBlock.deserialize(self.db.get(b"rblock_" + rHash))
        while rBlock.header.height >= 1 and len(self.rHeaderPool) < self.maxNumBlocksToRecover:
            rHash = rBlock.header.hashPrevBlock
            rBlock = RootBlock.deserialize(self.db.get(b"rblock_" + rHash))

            if self.tipHeader is None:
                self.tipHeader = rBlock.header

            self.rHeaderPool[rHash] = rBlock.header
            for mHeader in rBlock.minorBlockHeaderList:
                self.mHashSet.add(mHeader.getHash())

    def getTipHeader(self):
        return self.tipHeader

    # ------------------------- Root block db operations --------------------------------
    def putRootBlock(self, rootBlock, lastMinorBlockHeaderList, rootBlockHash=None):
        if rootBlockHash is None:
            rootBlockHash = rootBlock.header.getHash()

        lastList = LastMinorBlockHeaderList(headerList=lastMinorBlockHeaderList)
        self.db.put(b"rblock_" + rootBlockHash, rootBlock.serialize())
        self.db.put(b"lastlist_" + rootBlockHash, lastList.serialize())
        self.rHeaderPool[rootBlockHash] = rootBlock.header

    def updateTipHash(self, blockHash):
        self.db.put(b"tipHash", blockHash)

    def getRootBlockByHash(self, h, consistencyCheck=True):
        if consistencyCheck and h not in self.rHeaderPool:
            return None

        rawBlock = self.db.get(b"rblock_" + h, None)
        if not rawBlock:
            return None
        return RootBlock.deserialize(rawBlock)

    def getRootBlockHeaderByHash(self, h, consistencyCheck=True):
        header = self.rHeaderPool.get(h, None)
        if not header and not consistencyCheck:
            block = self.getRootBlockByHash(h, False)
            if block:
                header = block.header
        return header

    def getRootBlockLastMinorBlockHeaderList(self, h):
        if h not in self.rHeaderPool:
            return None
        return LastMinorBlockHeaderList.deserialize(self.db.get(b"lastlist_" + h)).headerList

    def containRootBlockByHash(self, h):
        return h in self.rHeaderPool

    def putRootBlockIndex(self, block):
        self.db.put(b"ri_%d" % block.header.height, block.header.getHash())

    def getRootBlockByHeight(self, height):
        key = b"ri_%d" % height
        if key not in self.db:
            return None
        blockHash = self.db.get(key)
        return self.getRootBlockByHash(blockHash, False)

    # ------------------------- Minor block db operations --------------------------------
    def containMinorBlockByHash(self, h):
        return h in self.mHashSet

    def putMinorBlockHash(self, mHash):
        self.db.put(b"mheader_" + mHash, b'')
        self.mHashSet.add(mHash)

    # ------------------------- Common operations -----------------------------------------
    def put(self, key, value):
        self.db.put(key, value)

    def get(self, key, default=None):
        return self.db.get(key, default)

    def __getitem__(self, key):
        return self[key]


class RootState:
    """ State of root
    """

    def __init__(self, env):
        self.env = env
        self.diffCalc = self.env.config.ROOT_DIFF_CALCULATOR
        self.diffHashFunc = self.env.config.DIFF_HASH_FUNC
        self.rawDb = env.db
        self.db = RootDb(self.rawDb, env.config.MAX_ROOT_BLOCK_IN_MEMORY)

        persistedTip = self.db.getTipHeader()
        if persistedTip:
            self.tip = persistedTip
            Logger.info("Recovered root state with tip height {}".format(self.tip.height))
        else:
            self.__createGenesisBlocks()
            Logger.info("Created genesis root block")

    def __createGenesisBlocks(self):
        evmList = create_genesis_evm_list(env=self.env)
        genesisRootBlock0, genesisRootBlock1, gMinorBlockList0, gMinorBlockList1 = create_genesis_blocks(
            env=self.env, evmList=evmList)

        self.db.putRootBlock(genesisRootBlock0, [b.header for b in gMinorBlockList0])
        self.db.putRootBlockIndex(genesisRootBlock0)
        self.db.putRootBlock(genesisRootBlock1, [b.header for b in gMinorBlockList1])
        self.db.putRootBlockIndex(genesisRootBlock1)
        self.tip = genesisRootBlock1.header
        for b in gMinorBlockList0:
            self.addValidatedMinorBlockHash(b.header.getHash())
        for b in gMinorBlockList1:
            self.addValidatedMinorBlockHash(b.header.getHash())

    def getTipBlock(self):
        return self.db.getRootBlockByHash(self.tip.getHash())

    def addValidatedMinorBlockHash(self, h):
        self.db.putMinorBlockHash(h)

    def getNextBlockDifficulty(self, createTime=None):
        if createTime is None:
            createTime = max(self.tip.createTime + 1, int(time.time()))
        return self.diffCalc.calculateDiffWithParent(self.tip, createTime)

    def createBlockToMine(self, mHeaderList, address, createTime=None):
        if createTime is None:
            createTime = max(self.tip.createTime + 1, int(time.time()))
        difficulty = self.diffCalc.calculateDiffWithParent(self.tip, createTime)
        block = self.tip.createBlockToAppend(createTime=createTime, address=address, difficulty=difficulty)
        block.minorBlockHeaderList = mHeaderList

        coinbaseAmount = 0
        for header in mHeaderList:
            coinbaseAmount += header.coinbaseAmount

        coinbaseAmount = coinbaseAmount // 2
        return block.finalize(quarkash=coinbaseAmount, coinbaseAddress=address)

    def validateBlockHeader(self, blockHeader, blockHash=None):
        ''' Validate the block header.
        '''
        if blockHeader.height <= 1:
            raise ValueError("unexpected height")

        if not self.db.containRootBlockByHash(blockHeader.hashPrevBlock):
            raise ValueError("previous hash block mismatch")
        prevBlockHeader = self.db.getRootBlockHeaderByHash(blockHeader.hashPrevBlock)

        if prevBlockHeader.height + 1 != blockHeader.height:
            raise ValueError("incorrect block height")

        if blockHeader.createTime <= prevBlockHeader.createTime:
            raise ValueError("incorrect create time tip time {}, new block time {}".format(
                blockHeader.createTime, prevBlockHeader.createTime))

        if blockHash is None:
            blockHash = blockHeader.getHash()

        # Check difficulty
        if not self.env.config.SKIP_ROOT_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == NetworkId.MAINNET:
                diff = self.diffCalc.calculateDiffWithParent(prevBlockHeader, blockHeader.createTime)
                if diff != blockHeader.difficulty:
                    raise ValueError("incorrect difficulty")
                metric = diff * int.from_bytes(blockHash, byteorder="big")
                if metric >= 2 ** 256:
                    raise ValueError("insufficient difficulty")
            elif blockHeader.coinbaseAddress.recipient != self.env.config.TESTNET_MASTER_ACCOUNT.recipient:
                raise ValueError("incorrect master to create the block")

        return blockHash

    def __isSameChain(self, longerBlockHeader, shorterBlockHeader):
        if shorterBlockHeader.height > longerBlockHeader.height:
            return False

        header = longerBlockHeader
        for i in range(longerBlockHeader.height - shorterBlockHeader.height):
            header = self.db.getRootBlockHeaderByHash(header.hashPrevBlock)
        return header == shorterBlockHeader

    def validateBlock(self, block, blockHash=None):
        if not self.db.containRootBlockByHash(block.header.hashPrevBlock):
            raise ValueError("previous hash block mismatch")
        prevLastMinorBlockHeaderList = self.db.getRootBlockLastMinorBlockHeaderList(block.header.hashPrevBlock)

        blockHash = self.validateBlockHeader(block.header, blockHash)

        # Check the merkle tree
        merkleHash = calculate_merkle_root(block.minorBlockHeaderList)
        if merkleHash != block.header.hashMerkleRoot:
            raise ValueError("incorrect merkle root")

        # Check whether all minor blocks are ordered, validated (and linked to previous block)
        shardId = 0
        prevHeader = prevLastMinorBlockHeaderList[0]
        lastMinorBlockHeaderList = []
        blockCountInShard = 0
        for idx, mHeader in enumerate(block.minorBlockHeaderList):
            if mHeader.branch.getShardId() != shardId:
                if mHeader.branch.getShardId() != shardId + 1:
                    raise ValueError("shard id must be ordered")
                if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                    raise ValueError("fail to prove progress")
                if mHeader.createTime > block.header.createTime:
                    raise ValueError("minor block create time is too large {}>{}".format(
                        mHeader.createTime, block.header.createTime))
                if not self.__isSameChain(
                        self.db.getRootBlockHeaderByHash(block.header.hashPrevBlock),
                        self.db.getRootBlockHeaderByHash(prevHeader.hashPrevRootBlock)):
                    raise ValueError("minor block's prev root block must be in the same chain")

                lastMinorBlockHeaderList.append(block.minorBlockHeaderList[idx - 1])
                shardId += 1
                blockCountInShard = 0
                prevHeader = prevLastMinorBlockHeaderList[shardId]

            if not self.db.containMinorBlockByHash(mHeader.getHash()):
                raise ValueError("minor block is not validated. {}-{}".format(
                    mHeader.branch.getShardId(), mHeader.height))

            if mHeader.hashPrevMinorBlock != prevHeader.getHash():
                raise ValueError("minor block doesn't link to previous minor block")
            blockCountInShard += 1
            prevHeader = mHeader
            # TODO: Add coinbase

        if shardId != block.header.shardInfo.getShardSize() - 1 and self.env.config.PROOF_OF_PROGRESS_BLOCKS != 0:
            raise ValueError("fail to prove progress")
        if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
            raise ValueError("fail to prove progress")
        if mHeader.createTime > block.header.createTime:
            raise ValueError("minor block create time is too large {}>{}".format(
                mHeader.createTime, block.header.createTime))
        if not self.__isSameChain(
                self.db.getRootBlockHeaderByHash(block.header.hashPrevBlock),
                self.db.getRootBlockHeaderByHash(mHeader.hashPrevRootBlock)):
            raise ValueError("minor block's prev root block must be in the same chain")
        lastMinorBlockHeaderList.append(mHeader)

        return blockHash, lastMinorBlockHeaderList

    def __rewriteBlockIndexTo(self, block):
        ''' Find the common ancestor in the current chain and rewrite index till block '''
        while block.header.height >= 0:
            origBlock = self.db.getRootBlockByHeight(block.header.height)
            if origBlock and origBlock.header == block.header:
                break
            self.db.putRootBlockIndex(block)
            block = self.db.getRootBlockByHash(block.header.hashPrevBlock)

    def addBlock(self, block, blockHash=None):
        """ Add new block.
        return True if a longest block is added, False otherwise
        There are a couple of optimizations can be done here:
        - the root block could only contain minor block header hashes as long as the shards fully validate the headers
        - the header (or hashes) are un-ordered as long as they contains valid sub-chains from previous root block
        """
        blockHash, lastMinorBlockHeaderList = self.validateBlock(block, blockHash)

        self.db.putRootBlock(block, lastMinorBlockHeaderList, rootBlockHash=blockHash)

        if self.tip.height < block.header.height:
            self.tip = block.header
            self.db.updateTipHash(blockHash)
            self.__rewriteBlockIndexTo(block)
            return True
        return False

    # -------------------------------- Root block db related operations ------------------------------
    def getRootBlockByHash(self, h):
        return self.db.getRootBlockByHash(h)

    def containRootBlockByHash(self, h):
        return self.db.containRootBlockByHash(h)

    def getRootBlockHeaderByHash(self, h):
        return self.db.getRootBlockHeaderByHash(h)

    def getRootBlockByHeight(self, height):
        return self.db.getRootBlockByHeight(height)

    # --------------------------------- Minor block db related operations ----------------------------
    def isMinorBlockValidated(self, h):
        return self.db.containMinorBlockByHash(h)
