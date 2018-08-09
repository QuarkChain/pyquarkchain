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

        self.__recover_from_db()

    def __recover_from_db(self):
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
                self.mHashSet.add(mHeader.get_hash())

    def get_tip_header(self):
        return self.tipHeader

    # ------------------------- Root block db operations --------------------------------
    def put_root_block(self, rootBlock, lastMinorBlockHeaderList, rootBlockHash=None):
        if rootBlockHash is None:
            rootBlockHash = rootBlock.header.get_hash()

        lastList = LastMinorBlockHeaderList(headerList=lastMinorBlockHeaderList)
        self.db.put(b"rblock_" + rootBlockHash, rootBlock.serialize())
        self.db.put(b"lastlist_" + rootBlockHash, lastList.serialize())
        self.rHeaderPool[rootBlockHash] = rootBlock.header

    def update_tip_hash(self, blockHash):
        self.db.put(b"tipHash", blockHash)

    def get_root_block_by_hash(self, h, consistencyCheck=True):
        if consistencyCheck and h not in self.rHeaderPool:
            return None

        rawBlock = self.db.get(b"rblock_" + h, None)
        if not rawBlock:
            return None
        return RootBlock.deserialize(rawBlock)

    def get_root_block_header_by_hash(self, h, consistencyCheck=True):
        header = self.rHeaderPool.get(h, None)
        if not header and not consistencyCheck:
            block = self.get_root_block_by_hash(h, False)
            if block:
                header = block.header
        return header

    def get_root_block_last_minor_block_header_list(self, h):
        if h not in self.rHeaderPool:
            return None
        return LastMinorBlockHeaderList.deserialize(self.db.get(b"lastlist_" + h)).headerList

    def contain_root_block_by_hash(self, h):
        return h in self.rHeaderPool

    def put_root_block_index(self, block):
        self.db.put(b"ri_%d" % block.header.height, block.header.get_hash())

    def get_root_block_by_height(self, height):
        key = b"ri_%d" % height
        if key not in self.db:
            return None
        blockHash = self.db.get(key)
        return self.get_root_block_by_hash(blockHash, False)

    # ------------------------- Minor block db operations --------------------------------
    def contain_minor_block_by_hash(self, h):
        return h in self.mHashSet

    def put_minor_block_hash(self, mHash):
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

        persistedTip = self.db.get_tip_header()
        if persistedTip:
            self.tip = persistedTip
            Logger.info("Recovered root state with tip height {}".format(self.tip.height))
        else:
            self.__create_genesis_blocks()
            Logger.info("Created genesis root block")

    def __create_genesis_blocks(self):
        evmList = create_genesis_evm_list(env=self.env)
        genesisRootBlock0, genesisRootBlock1, gMinorBlockList0, gMinorBlockList1 = create_genesis_blocks(
            env=self.env, evmList=evmList)

        self.db.put_root_block(genesisRootBlock0, [b.header for b in gMinorBlockList0])
        self.db.put_root_block_index(genesisRootBlock0)
        self.db.put_root_block(genesisRootBlock1, [b.header for b in gMinorBlockList1])
        self.db.put_root_block_index(genesisRootBlock1)
        self.tip = genesisRootBlock1.header
        for b in gMinorBlockList0:
            self.add_validated_minor_block_hash(b.header.get_hash())
        for b in gMinorBlockList1:
            self.add_validated_minor_block_hash(b.header.get_hash())

    def get_tip_block(self):
        return self.db.get_root_block_by_hash(self.tip.get_hash())

    def add_validated_minor_block_hash(self, h):
        self.db.put_minor_block_hash(h)

    def get_next_block_difficulty(self, createTime=None):
        if createTime is None:
            createTime = max(self.tip.createTime + 1, int(time.time()))
        return self.diffCalc.calculate_diff_with_parent(self.tip, createTime)

    def create_block_to_mine(self, mHeaderList, address, createTime=None):
        if createTime is None:
            createTime = max(self.tip.createTime + 1, int(time.time()))
        difficulty = self.diffCalc.calculate_diff_with_parent(self.tip, createTime)
        block = self.tip.create_block_to_append(createTime=createTime, address=address, difficulty=difficulty)
        block.minorBlockHeaderList = mHeaderList

        coinbaseAmount = 0
        for header in mHeaderList:
            coinbaseAmount += header.coinbaseAmount

        coinbaseAmount = coinbaseAmount // 2
        return block.finalize(quarkash=coinbaseAmount, coinbaseAddress=address)

    def validate_block_header(self, blockHeader, blockHash=None):
        ''' Validate the block header.
        '''
        if blockHeader.height <= 1:
            raise ValueError("unexpected height")

        if not self.db.contain_root_block_by_hash(blockHeader.hashPrevBlock):
            raise ValueError("previous hash block mismatch")
        prevBlockHeader = self.db.get_root_block_header_by_hash(blockHeader.hashPrevBlock)

        if prevBlockHeader.height + 1 != blockHeader.height:
            raise ValueError("incorrect block height")

        if blockHeader.createTime <= prevBlockHeader.createTime:
            raise ValueError("incorrect create time tip time {}, new block time {}".format(
                blockHeader.createTime, prevBlockHeader.createTime))

        if blockHash is None:
            blockHash = blockHeader.get_hash()

        # Check difficulty
        if not self.env.config.SKIP_ROOT_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == NetworkId.MAINNET:
                diff = self.diffCalc.calculate_diff_with_parent(prevBlockHeader, blockHeader.createTime)
                if diff != blockHeader.difficulty:
                    raise ValueError("incorrect difficulty")
                metric = diff * int.from_bytes(blockHash, byteorder="big")
                if metric >= 2 ** 256:
                    raise ValueError("insufficient difficulty")
            elif blockHeader.coinbaseAddress.recipient != self.env.config.TESTNET_MASTER_ACCOUNT.recipient:
                raise ValueError("incorrect master to create the block")

        return blockHash

    def __is_same_chain(self, longerBlockHeader, shorterBlockHeader):
        if shorterBlockHeader.height > longerBlockHeader.height:
            return False

        header = longerBlockHeader
        for i in range(longerBlockHeader.height - shorterBlockHeader.height):
            header = self.db.get_root_block_header_by_hash(header.hashPrevBlock)
        return header == shorterBlockHeader

    def validate_block(self, block, blockHash=None):
        if not self.db.contain_root_block_by_hash(block.header.hashPrevBlock):
            raise ValueError("previous hash block mismatch")
        prevLastMinorBlockHeaderList = self.db.get_root_block_last_minor_block_header_list(block.header.hashPrevBlock)

        blockHash = self.validate_block_header(block.header, blockHash)

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
            if mHeader.branch.get_shard_id() != shardId:
                if mHeader.branch.get_shard_id() != shardId + 1:
                    raise ValueError("shard id must be ordered")
                if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                    raise ValueError("fail to prove progress")
                if mHeader.createTime > block.header.createTime:
                    raise ValueError("minor block create time is too large {}>{}".format(
                        mHeader.createTime, block.header.createTime))
                if not self.__is_same_chain(
                        self.db.get_root_block_header_by_hash(block.header.hashPrevBlock),
                        self.db.get_root_block_header_by_hash(prevHeader.hashPrevRootBlock)):
                    raise ValueError("minor block's prev root block must be in the same chain")

                lastMinorBlockHeaderList.append(block.minorBlockHeaderList[idx - 1])
                shardId += 1
                blockCountInShard = 0
                prevHeader = prevLastMinorBlockHeaderList[shardId]

            if not self.db.contain_minor_block_by_hash(mHeader.get_hash()):
                raise ValueError("minor block is not validated. {}-{}".format(
                    mHeader.branch.get_shard_id(), mHeader.height))

            if mHeader.hashPrevMinorBlock != prevHeader.get_hash():
                raise ValueError("minor block doesn't link to previous minor block")
            blockCountInShard += 1
            prevHeader = mHeader
            # TODO: Add coinbase

        if shardId != block.header.shardInfo.get_shard_size() - 1 and self.env.config.PROOF_OF_PROGRESS_BLOCKS != 0:
            raise ValueError("fail to prove progress")
        if blockCountInShard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
            raise ValueError("fail to prove progress")
        if mHeader.createTime > block.header.createTime:
            raise ValueError("minor block create time is too large {}>{}".format(
                mHeader.createTime, block.header.createTime))
        if not self.__is_same_chain(
                self.db.get_root_block_header_by_hash(block.header.hashPrevBlock),
                self.db.get_root_block_header_by_hash(mHeader.hashPrevRootBlock)):
            raise ValueError("minor block's prev root block must be in the same chain")
        lastMinorBlockHeaderList.append(mHeader)

        return blockHash, lastMinorBlockHeaderList

    def __rewrite_block_index_to(self, block):
        ''' Find the common ancestor in the current chain and rewrite index till block '''
        while block.header.height >= 0:
            origBlock = self.db.get_root_block_by_height(block.header.height)
            if origBlock and origBlock.header == block.header:
                break
            self.db.put_root_block_index(block)
            block = self.db.get_root_block_by_hash(block.header.hashPrevBlock)

    def add_block(self, block, blockHash=None):
        """ Add new block.
        return True if a longest block is added, False otherwise
        There are a couple of optimizations can be done here:
        - the root block could only contain minor block header hashes as long as the shards fully validate the headers
        - the header (or hashes) are un-ordered as long as they contains valid sub-chains from previous root block
        """
        blockHash, lastMinorBlockHeaderList = self.validate_block(block, blockHash)

        self.db.put_root_block(block, lastMinorBlockHeaderList, rootBlockHash=blockHash)

        if self.tip.height < block.header.height:
            self.tip = block.header
            self.db.update_tip_hash(blockHash)
            self.__rewrite_block_index_to(block)
            return True
        return False

    # -------------------------------- Root block db related operations ------------------------------
    def get_root_block_by_hash(self, h):
        return self.db.get_root_block_by_hash(h)

    def contain_root_block_by_hash(self, h):
        return self.db.contain_root_block_by_hash(h)

    def get_root_block_header_by_hash(self, h):
        return self.db.get_root_block_header_by_hash(h)

    def get_root_block_by_height(self, height):
        return self.db.get_root_block_by_height(height)

    # --------------------------------- Minor block db related operations ----------------------------
    def is_minor_block_validated(self, h):
        return self.db.contain_minor_block_by_hash(h)
