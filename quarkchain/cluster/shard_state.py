import random
import time

from collections import deque

from quarkchain.cluster.core import RootBlock, MinorBlock, CrossShardTransactionList, CrossShardTransactionDeposit
from quarkchain.cluster.genesis import create_genesis_minor_block, create_genesis_root_block
from quarkchain.cluster.rpc import ShardStats
from quarkchain.config import NetworkId
from quarkchain.core import calculate_merkle_root, Address, Branch, Code, Constant, Transaction
from quarkchain.evm.state import State as EvmState
from quarkchain.evm.messages import apply_transaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.evm.transaction_queue import TransactionQueue
from quarkchain.evm import opcodes
from quarkchain.reward import ConstMinorBlockRewardCalcultor
from quarkchain.utils import Logger, check


class ExpiryQueue:
    ''' A queue only keeps the elements added in the past ttl seconds '''

    def __init__(self, ttlSec):
        self.__queue = deque()
        self.__ttl = ttlSec

    def __removeExpiredElements(self):
        current = time.time()
        while len(self.__queue) > 0 and self.__queue[0][0] < current:
            self.__queue.popleft()

    def append(self, e):
        self.__removeExpiredElements()
        self.__queue.append((time.time() + self.__ttl, e))

    def __iter__(self):
        self.__removeExpiredElements()
        for t, e in self.__queue:
            yield e

    def __getitem__(self, index):
        self.__removeExpiredElements()
        return self.__queue[index]

    def __len__(self):
        self.__removeExpiredElements()
        return len(self.__queue)

    def __str__(self):
        self.__removeExpiredElements()
        return str(self.__queue)


class ExpiryCounter:

    def __init__(self, windowSec):
        self.window = windowSec
        self.queue = ExpiryQueue(windowSec)

    def increment(self, value):
        self.queue.append(value)

    def getCount(self):
        return sum(self.queue)

    def getCountPerSecond(self):
        return self.getCount() / self.window


class ShardDb:
    def __init__(self, db, branch):
        self.db = db
        self.branch = branch
        # TODO:  iterate db to recover pools and set
        self.mHeaderPool = dict()
        self.mMetaPool = dict()
        self.xShardSet = set()
        self.rHeaderPool = dict()
        self.rMinorHeaderPool = dict()
        self.txCounter60s = ExpiryCounter(60)

        self.__recoverFromDb()

    def __getLastMinorBlockInRootBlock(self, rootBlock):
        lHeader = None
        for mHeader in rootBlock.minorBlockHeaderList:
            if mHeader.branch != self.branch:
                continue

            if lHeader is None or mHeader.height > lHeader.height:
                lHeader = mHeader

        check(lHeader is not None)
        return lHeader

    def __recoverFromDb(self):
        Logger.info("Recovering shard state from local database...")

        # recover mHeaderPool, mMetaPool
        prefix = b"mblock_"
        start = prefix
        end = prefix + b"\xff" * 32
        numBytesToSkip = len(prefix)
        for k, v in self.db.rangeIter(start, end):
            blockHash = k[numBytesToSkip:]
            block = MinorBlock.deserialize(v)
            Logger.debug("Recovering shard block height {}".format(block.header.height))
            self.mHeaderPool[blockHash] = block.header
            self.mMetaPool[blockHash] = block.meta

        # recover xShardSet
        prefix = b"xShard_"
        start = prefix
        end = prefix + b"\xff" * 32
        numBytesToSkip = len(prefix)
        Logger.info("Recovering xShardSet ...")
        for k, v in self.db.rangeIter(start, end):
            self.xShardSet.add(k[numBytesToSkip:])
            Logger.debug("Recovered xShardSet height {}".format(k))

        # recover rHeaderPool
        prefix = b"rblock_"
        start = prefix
        end = prefix + b"\xff" * 32
        numBytesToSkip = len(prefix)
        Logger.info("Recovering rHeaderPool ...")
        for k, v in self.db.rangeIter(start, end):
            rootBlock = RootBlock.deserialize(v)
            self.rHeaderPool[k[numBytesToSkip:]] = rootBlock.header
            # TODO: genesis root block should contain minor header in future
            if rootBlock.header.height != 0:
                self.rMinorHeaderPool[k[numBytesToSkip:]] = self.__getLastMinorBlockInRootBlock(rootBlock)
            Logger.debug("Recovered root header height {} hash {}".format(
                rootBlock.header.height, k[numBytesToSkip:]))

    def pruneForks(self, rHeader, mHeader):
        ''' When recovering from local database, we can only guarantee the consistency of the best chain.
        Forking blocks can be in inconsistent state and thus should be pruned from the database
        so that they can be retried in the future.
        '''
        rHeaderPool = {rHeader.getHash(): rHeader}
        while rHeader.height > 0:
            prevHash = rHeader.hashPrevBlock
            prevHeader = self.rHeaderPool[prevHash]
            rHeaderPool[prevHash] = prevHeader
            rHeader = prevHeader
        self.rHeaderPool = rHeaderPool

        mHeaderPool = {mHeader.getHash(): mHeader}
        while mHeader.height > 0:
            prevHash = mHeader.hashPrevMinorBlock
            prevHeader = self.mHeaderPool[prevHash]
            mHeaderPool[prevHash] = prevHeader
            mHeader = prevHeader
        self.mHeaderPool = mHeaderPool

    # ------------------------- Root block db operations --------------------------------
    def putRootBlock(self, rootBlock, rMinorHeader, rootBlockHash=None):
        ''' rMinorHeader: the minor header of the shard in the root block with largest height
        '''
        if rootBlockHash is None:
            rootBlockHash = rootBlock.header.getHash()

        self.db.put(b"rblock_" + rootBlockHash, rootBlock.serialize())
        self.rHeaderPool[rootBlockHash] = rootBlock.header
        self.rMinorHeaderPool[rootBlockHash] = rMinorHeader

    def getRootBlockByHash(self, h):
        if h not in self.rHeaderPool:
            return None
        return RootBlock.deserialize(self.db.get(b"rblock_" + h))

    def getRootBlockHeaderByHash(self, h):
        return self.rHeaderPool.get(h, None)

    def containRootBlockByHash(self, h):
        return h in self.rHeaderPool

    def getLastMinorBlockInRootBlock(self, h):
        if h not in self.rHeaderPool:
            return None
        return self.rMinorHeaderPool.get(h)

    # ------------------------- Minor block db operations --------------------------------
    def putMinorBlock(self, mBlock, evmState, mBlockHash=None):
        if mBlockHash is None:
            mBlockHash = mBlock.header.getHash()

        self.db.put(b"mblock_" + mBlockHash, mBlock.serialize())
        self.mHeaderPool[mBlockHash] = mBlock.header
        self.mMetaPool[mBlockHash] = mBlock.meta

        self.txCounter60s.increment(len(mBlock.txList))

    def getMinorBlockHeaderByHash(self, h):
        return self.mHeaderPool.get(h, None)

    def getMinorBlockEvmRootHashByHash(self, h):
        if h not in self.mHeaderPool:
            return None
        check(h in self.mMetaPool)
        meta = self.mMetaPool[h]
        return meta.hashEvmStateRoot

    def getMinorBlockMetaByHash(self, h):
        return self.mMetaPool.get(h, None)

    def getMinorBlockByHash(self, h):
        if h not in self.mHeaderPool:
            return None
        return MinorBlock.deserialize(self.db.get(b"mblock_" + h))

    def containMinorBlockByHash(self, h):
        return h in self.mHeaderPool

    def putMinorBlockIndex(self, block):
        self.db.put(b"mi_%d" % block.header.height, block.header.getHash())

    def removeMinorBlockIndex(self, block):
        self.db.remove(b"mi_%d" % block.header.height)

    def getMinorBlockByHeight(self, height):
        key = b"mi_%d" % height
        if key not in self.db:
            return None
        blockHash = self.db.get(key)
        return self.getMinorBlockByHash(blockHash)

    # ------------------------- Transaction db operations --------------------------------
    def putTransactionIndex(self, txHash, blockHeight, index):
        self.db.put(b"txindex_" + txHash,
                    blockHeight.to_bytes(4, "big") + index.to_bytes(4, "big"))

    def removeTransactionIndex(self, txHash):
        self.db.remove(b"txindex_" + txHash)

    def getTransactionByHash(self, txHash):
        result = self.db.get(b"txindex_" + txHash, None)
        if not result:
            return None, None
        check(len(result) == 8)
        blockHeight = int.from_bytes(result[:4], "big")
        index = int.from_bytes(result[4:], "big")
        return self.getMinorBlockByHeight(blockHeight), index

    def putTransactionIndexFromBlock(self, minorBlock):
        for i, tx in enumerate(minorBlock.txList):
            self.putTransactionIndex(tx.getHash(), minorBlock.header.height, i)

    def removeTransactionIndexFromBlock(self, minorBlock):
        for tx in minorBlock.txList:
            self.removeTransactionIndex(tx.getHash())

    # -------------------------- Cross-shard tx operations ----------------------------
    def putMinorBlockXshardTxList(self, h, txList: CrossShardTransactionList):
        self.xShardSet.add(h)
        self.db.put(b"xShard_" + h, txList.serialize())

    def getMinorBlockXshardTxList(self, h) -> CrossShardTransactionList:
        return CrossShardTransactionList.deserialize(self.db.get(b"xShard_" + h))

    def containRemoteMinorBlockHash(self, h):
        return h in self.xShardSet

    # ------------------------- Common operations -----------------------------------------
    def put(self, key, value):
        self.db.put(key, value)

    def get(self, key, default=None):

        return self.db.get(key, default)

    def __getitem__(self, key):
        return self[key]


class ShardState:
    """  State of a shard, which includes
    - evm state
    - minor blockchain
    - root blockchain and cross-shard transaction
    TODO: Support
    - reshard by split
    """

    def __init__(self, env, shardId, db=None):
        self.env = env
        self.shardId = shardId
        self.diffCalc = self.env.config.MINOR_DIFF_CALCULATOR
        self.diffHashFunc = self.env.config.DIFF_HASH_FUNC
        self.rewardCalc = ConstMinorBlockRewardCalcultor(env)
        self.rawDb = db if db is not None else env.db
        self.branch = Branch.create(env.config.SHARD_SIZE, shardId)
        self.db = ShardDb(self.rawDb, self.branch)
        self.txQueue = TransactionQueue()
        self.initialized = False

        # assure ShardState is in good shape after constructor returns though we still
        # rely on master calling initFromRootBlock to bring the cluster into consistency
        self.__createGenesisBlocks(shardId)

    def initFromRootBlock(self, rootBlock):
        ''' Master will send its root chain tip when it connects to slaves.
        Shards will initialize its state based on the root block.
        '''
        def __getHeaderTipFromRootBlock(branch):
            headerTip = None
            for mHeader in rootBlock.minorBlockHeaderList:
                if mHeader.branch == branch:
                    check(headerTip is None or headerTip.height + 1 == mHeader.height)
                    headerTip = mHeader
            check(headerTip is not None)
            return headerTip

        check(not self.initialized)
        self.initialized = True

        Logger.info("Initializing shard state from root height {} hash {}".format(
            rootBlock.header.height, rootBlock.header.getHash().hex()))

        if rootBlock.header.height == 0:
            Logger.info("Created genesis block")
            return

        # It's possible that the root block is unknown to this shard since
        # the shard could crash before the root block arrived
        # TODO: the ShardState should have necessary data to add the root block,
        # this may fail if data are not properly flushed/synced to persistent storage (e.g., OS crash)
        if not self.db.containRootBlockByHash(rootBlock.header.getHash()):
            self.addRootBlock(rootBlock)

        shardSize = rootBlock.header.shardInfo.getShardSize()
        self.branch = Branch.create(shardSize, self.shardId)
        self.rootTip = rootBlock.header
        self.headerTip = __getHeaderTipFromRootBlock(self.branch)
        self.metaTip = self.db.getMinorBlockMetaByHash(self.headerTip.getHash())
        self.confirmedHeaderTip = self.headerTip
        self.confirmedMetaTip = self.metaTip
        self.evmState = self.__createEvmState()
        self.evmState.trie.root_hash = self.metaTip.hashEvmStateRoot
        check(self.db.getMinorBlockEvmRootHashByHash(self.headerTip.getHash()) == self.metaTip.hashEvmStateRoot)

        self.db.pruneForks(self.rootTip, self.headerTip)

        self.__rewriteBlockIndexTo(self.db.getMinorBlockByHash(self.headerTip.getHash()))

    def __createEvmState(self):
        return EvmState(env=self.env.evmEnv, db=self.rawDb)

    def __createGenesisBlocks(self, shardId):
        genesisRootBlock = create_genesis_root_block(self.env)
        self.evmState = self.__createEvmState()
        genesisMinorBlock = create_genesis_minor_block(
            env=self.env,
            shardId=shardId,
            hashRootBlock=genesisRootBlock.header.getHash(),
            evmState=self.evmState)

        check(genesisMinorBlock.header.branch == self.branch)
        self.db.putMinorBlock(genesisMinorBlock, self.evmState)
        self.db.putMinorBlockIndex(genesisMinorBlock)
        self.db.putRootBlock(genesisRootBlock, genesisMinorBlock.header)

        self.rootTip = genesisRootBlock.header
        # Tips that are confirmed by root
        self.confirmedHeaderTip = genesisMinorBlock.header
        self.confirmedMetaTip = genesisMinorBlock.header
        # Tips that are unconfirmed by root
        self.headerTip = genesisMinorBlock.header
        self.metaTip = genesisMinorBlock.meta

    def __validateTx(self, tx: Transaction) -> EvmTransaction:
        # UTXOs are not supported now
        if len(tx.inList) != 0:
            raise RuntimeError("input list must be empty")
        if len(tx.outList) != 0:
            raise RuntimeError("output list must be empty")
        if len(tx.signList) != 0:
            raise RuntimeError("sign list must be empty")

        # Check OP code
        if len(tx.code.code) == 0:
            raise RuntimeError("empty op code")
        if not tx.code.isEvm():
            raise RuntimeError("only evm transaction is supported now")

        evmTx = tx.code.getEvmTransaction()
        if evmTx.networkId != self.env.config.NETWORK_ID:
            raise RuntimeError("evm tx network id mismatch")
        if self.branch.value != evmTx.branchValue:
            raise RuntimeError("evm tx is not in the shard")
        if evmTx.getWithdraw() < 0:
            raise RuntimeError("withdraw must be non-negative")
        if evmTx.getWithdraw() != 0:
            if len(evmTx.withdrawTo) != Constant.ADDRESS_LENGTH:
                raise ValueError("withdraw to address length is incorrect")
            withdrawTo = Address.deserialize(evmTx.withdrawTo)
            if self.branch.isInShard(withdrawTo.fullShardId):
                raise ValueError("withdraw address must not in the shard")
        # TODO: Neighborhood and xshard gas limit check
        return evmTx

    def addTx(self, tx: Transaction):
        try:
            evmTx = self.__validateTx(tx)
            self.txQueue.add_transaction(evmTx)
            return True
        except Exception as e:
            Logger.warningEverySec("Failed to add transaction: {}".format(e), 1)
            return False

    def __getEvmStateForNewBlock(self, block, ephemeral=True):
        if ephemeral:
            state = self.evmState.ephemeral_clone()
        else:
            state = self.__createEvmState()

        state = self.__createEvmState()
        state.trie.root_hash = self.db.getMinorBlockEvmRootHashByHash(block.header.hashPrevMinorBlock)
        state.txindex = 0
        state.gas_used = 0
        state.bloom = 0
        state.receipts = []
        state.timestamp = block.header.createTime
        state.gas_limit = block.meta.evmGasLimit  # TODO
        state.block_number = block.header.height
        state.recent_uncles[state.block_number] = []  # TODO [x.hash for x in block.uncles]
        # TODO: Create a account with shard info if the account is not created
        state.block_coinbase = block.meta.coinbaseAddress.recipient
        state.block_difficulty = block.header.difficulty
        state.block_reward = 0
        state.prev_headers = []                          # TODO: state.add_block_header(block.header)
        return state

    def __isSameMinorChain(self, longerBlockHeader, shorterBlockHeader):
        if shorterBlockHeader.height > longerBlockHeader.height:
            return False

        header = longerBlockHeader
        for i in range(longerBlockHeader.height - shorterBlockHeader.height):
            header = self.db.getMinorBlockHeaderByHash(header.hashPrevMinorBlock)
        return header == shorterBlockHeader

    def __isSameRootChain(self, longerBlockHeader, shorterBlockHeader):
        if shorterBlockHeader.height > longerBlockHeader.height:
            return False

        header = longerBlockHeader
        for i in range(longerBlockHeader.height - shorterBlockHeader.height):
            header = self.db.getRootBlockHeaderByHash(header.hashPrevBlock)
        return header == shorterBlockHeader

    def __validateBlock(self, block):
        ''' Validate a block before running evm transactions
        '''
        if not self.db.containMinorBlockByHash(block.header.hashPrevMinorBlock):
            # TODO:  May put the block back to queue
            raise ValueError("prev block not found")
        prevHeader = self.db.getMinorBlockHeaderByHash(block.header.hashPrevMinorBlock)

        if block.header.height != prevHeader.height + 1:
            raise ValueError("height mismatch")

        if block.header.branch != self.branch:
            raise ValueError("branch mismatch")

        if block.header.createTime <= prevHeader.createTime:
            raise ValueError("incorrect create time tip time {}, new block time {}".format(
                block.header.createTime, self.chain[-1].createTime))

        if block.header.hashMeta != block.meta.getHash():
            raise ValueError("Hash of meta mismatch")

        if len(block.meta.extraData) > self.env.config.BLOCK_EXTRA_DATA_SIZE_LIMIT:
            raise ValueError("extraData in block is too large")

        # Make sure merkle tree is valid
        merkleHash = calculate_merkle_root(block.txList)
        if merkleHash != block.meta.hashMerkleRoot:
            raise ValueError("incorrect merkle root")

        # Check the first transaction of the block
        if not self.branch.isInShard(block.meta.coinbaseAddress.fullShardId):
            raise ValueError("coinbase output address must be in the shard")

        # Check difficulty
        if not self.env.config.SKIP_MINOR_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == NetworkId.MAINNET:
                diff = self.diffCalc.calculateDiffWithParent(prevHeader, block.header.createTime)
                if diff != block.header.difficulty:
                    raise ValueError("incorrect difficulty")
                metric = diff * int.from_bytes(block.header.getHash(), byteorder="big")
                if metric >= 2 ** 256:
                    raise ValueError("insufficient difficulty")
            elif block.meta.coinbaseAddress.recipient != self.env.config.TESTNET_MASTER_ACCOUNT.recipient:
                raise ValueError("incorrect master to create the block")

        if not self.branch.isInShard(block.meta.coinbaseAddress.fullShardId):
            raise ValueError("coinbase output must be in local shard")

        # Check whether the root header is in the root chain
        rootBlockHeader = self.db.getRootBlockHeaderByHash(block.header.hashPrevRootBlock)
        if rootBlockHeader is None:
            raise ValueError("cannot find root block for the minor block")

        if rootBlockHeader.height < self.db.getRootBlockHeaderByHash(prevHeader.hashPrevRootBlock).height:
            raise ValueError("prev root block height must be non-decreasing")

        prevConfirmedMinorBlock = self.db.getLastMinorBlockInRootBlock(block.header.hashPrevRootBlock)
        if not self.__isSameMinorChain(prevHeader, prevConfirmedMinorBlock):
            raise ValueError("prev root block's minor block is not in the same chain as the minor block")

        if not self.__isSameRootChain(self.db.getRootBlockHeaderByHash(block.header.hashPrevRootBlock),
                                      self.db.getRootBlockHeaderByHash(prevHeader.hashPrevRootBlock)):
            raise ValueError("prev root blocks are not on the same chain")

    def runBlock(self, block, evmState=None):
        if evmState is None:
            evmState = self.__getEvmStateForNewBlock(block, ephemeral=False)
        rootBlockHeader = self.db.getRootBlockHeaderByHash(block.header.hashPrevRootBlock)
        prevHeader = self.db.getMinorBlockHeaderByHash(block.header.hashPrevMinorBlock)

        self.__runCrossShardTxList(
            evmState=evmState,
            descendantRootHeader=rootBlockHeader,
            ancestorRootHeader=self.db.getRootBlockHeaderByHash(prevHeader.hashPrevRootBlock))

        for idx, tx in enumerate(block.txList):
            try:
                evmTx = self.__validateTx(tx)
                apply_transaction(evmState, evmTx)
            except Exception as e:
                Logger.debugException()
                Logger.debug("failed to process Tx {}, idx {}, reason {}".format(
                    tx.getHash().hex(), idx, e))
                raise e

        # Put only half of block fee to coinbase address
        check(evmState.get_balance(evmState.block_coinbase) >= evmState.block_fee)
        evmState.delta_balance(evmState.block_coinbase, -evmState.block_fee // 2)

        # Update actual root hash
        evmState.commit()
        return evmState

    def __isMinorBlockLinkedToRootTip(self, mBlock):
        """ Determine whether a minor block is a descendant of a minor block confirmed by root tip
        """
        if mBlock.header.height <= self.confirmedHeaderTip.height:
            return False

        header = mBlock.header
        for i in range(mBlock.header.height - self.confirmedHeaderTip.height):
            header = self.db.getMinorBlockHeaderByHash(header.hashPrevMinorBlock)

        return header == self.confirmedHeaderTip

    def __rewriteBlockIndexTo(self, minorBlock):
        ''' Find the common ancestor in the current chain and rewrite index till minorblock '''
        newChain = []
        oldChain = []

        # minorBlock height could be lower than the curren tip
        # we should revert all the blocks above minorBlock height
        height = minorBlock.header.height + 1
        while True:
            origBlock = self.db.getMinorBlockByHeight(height)
            if not origBlock:
                break
            oldChain.append(origBlock)
            height += 1

        block = minorBlock
        # Find common ancestor and record the blocks that needs to be updated
        while block.header.height >= 0:
            origBlock = self.db.getMinorBlockByHeight(block.header.height)
            if origBlock and origBlock.header == block.header:
                break
            newChain.append(block)
            if origBlock:
                oldChain.append(origBlock)
            block = self.db.getMinorBlockByHash(block.header.hashPrevMinorBlock)

        for block in oldChain:
            self.db.removeTransactionIndexFromBlock(block)
            self.db.removeMinorBlockIndex(block)
        for block in newChain:
            self.db.putTransactionIndexFromBlock(block)
            self.db.putMinorBlockIndex(block)

    def addBlock(self, block):
        """  Add a block to local db.  Perform validate and update tip accordingly
        Returns None if block is already added.
        Returns a list of CrossShardTransactionDeposit from block.
        Raises on any error.
        """
        if self.db.containMinorBlockByHash(block.header.getHash()):
            return None

        # Throw exception if fail to run
        self.__validateBlock(block)
        evmState = self.runBlock(block)

        # ------------------------ Validate ending result of the block --------------------
        if block.meta.hashEvmStateRoot != evmState.trie.root_hash:
            raise ValueError("State root mismatch: header %s computed %s" %
                             (block.meta.hashEvmStateRoot.hex(), evmState.trie.root_hash.hex()))

        if evmState.gas_used != block.meta.evmGasUsed:
            raise ValueError("Gas used mismatch: header %d computed %d" %
                             (block.meta.evmGasUsed, evmState.gas_used))

        # The rest fee goes to root block
        if evmState.block_fee // 2 != block.header.coinbaseAmount:
            raise ValueError("Coinbase reward incorrect")
        # TODO: Check evm receipt and bloom

        # TODO: Add block reward to coinbase
        # self.rewardCalc.getBlockReward(self):
        self.db.putMinorBlock(block, evmState)

        # Update tip if a block is appended or a fork is longer (with the same ancestor confirmed by root block tip)
        # or they are equal length but the root height confirmed by the block is longer
        updateTip = False
        if not self.__isSameRootChain(self.rootTip, self.db.getRootBlockHeaderByHash(block.header.hashPrevRootBlock)):
            # Don't update tip if the block depends on a root block that is not rootTip or rootTip's ancestor
            updateTip = False
        elif block.header.hashPrevMinorBlock == self.headerTip.getHash():
            updateTip = True
        elif self.__isMinorBlockLinkedToRootTip(block):
            if block.header.height > self.headerTip.height:
                updateTip = True
            elif block.header.height == self.headerTip.height:
                updateTip = self.db.getRootBlockHeaderByHash(block.header.hashPrevRootBlock).height > \
                    self.db.getRootBlockHeaderByHash(self.headerTip.hashPrevRootBlock).height

        if updateTip:
            self.__rewriteBlockIndexTo(block)
            self.evmState = evmState
            self.headerTip = block.header
            self.metaTip = block.meta

        check(self.__isSameRootChain(self.rootTip, self.db.getRootBlockHeaderByHash(self.headerTip.hashPrevRootBlock)))

        return evmState.xshard_list

    def getTip(self):
        return self.db.getMinorBlockByHash(self.headerTip.getHash())

    def tip(self):
        ''' Called in diff.py '''
        return self.headerTip

    def finalizeAndAddBlock(self, block):
        block.finalize(evmState=self.runBlock(block))
        self.addBlock(block)

    def getBlockHeaderByHeight(self, height):
        pass

    def getBalance(self, recipient):
        return self.evmState.get_balance(recipient)

    def getTransactionCount(self, recipient):
        return self.evmState.get_nonce(recipient)

    def getNextBlockDifficulty(self, createTime=None):
        if not createTime:
            createTime = max(int(time.time()), self.headerTip.createTime + 1)
        return self.diffCalc.calculateDiffWithParent(self.headerTip, createTime)

    def getNextBlockReward(self):
        return self.rewardCalc.getBlockReward(self)

    def getNextBlockCoinbaseAmount(self):
        # TODO: add block reward
        block = self.createBlockToMine()
        # Add back the transactions that have been popped into block
        # This actually lowers the priority of the popped tx if there are other tx with same gas price
        # TODO: get a better data structure for txQueue
        for tx in block.txList:
            check(self.addTx(tx))
        return block.header.coinbaseAmount

    def getUnconfirmedHeadersCoinbaseAmount(self):
        amount = 0
        header = self.headerTip
        for i in range(header.height - self.confirmedHeaderTip.height):
            amount += header.coinbaseAmount
            header = self.db.getMinorBlockHeaderByHash(header.hashPrevMinorBlock)
        check(header == self.confirmedHeaderTip)
        return amount

    def getUnconfirmedHeaderList(self):
        ''' height in ascending order '''
        headerList = []
        header = self.headerTip
        for i in range(header.height - self.confirmedHeaderTip.height):
            headerList.append(header)
            header = self.db.getMinorBlockHeaderByHash(header.hashPrevMinorBlock)
        check(header == self.confirmedHeaderTip)
        headerList.reverse()
        return headerList

    def __addArtificialTx(self, block, evmState, artificialTxConfig):
        for i in range(artificialTxConfig.numTxPerBlock):
            if random.randint(1, 100) <= artificialTxConfig.xShardTxPercent:
                # x-shard tx
                toShard = random.randint(0, self.env.config.SHARD_SIZE - 1)
                if toShard == self.branch.getShardId():
                    continue
                withdrawTo = evmState.block_coinbase + toShard.to_bytes(4, "big")
                evmTx = EvmTransaction(
                    branchValue=self.branch.value,
                    nonce=evmState.get_nonce(evmState.block_coinbase),
                    gasprice=1,
                    startgas=500000,
                    to=evmState.block_coinbase,
                    value=0,
                    data=b'',
                    withdrawSign=1,
                    withdraw=random.randint(1, 10000) * self.env.config.QUARKSH_TO_JIAOZI,
                    withdrawTo=withdrawTo,
                    networkId=self.env.config.NETWORK_ID)
            else:
                evmTx = EvmTransaction(
                    branchValue=self.branch.value,
                    nonce=evmState.get_nonce(evmState.block_coinbase),
                    gasprice=1,
                    startgas=21000,
                    to=evmState.block_coinbase,
                    value=random.randint(1, 10000) * self.env.config.QUARKSH_TO_JIAOZI,
                    data=b'',
                    withdrawSign=1,
                    withdraw=0,
                    withdrawTo=b'',
                    networkId=self.env.config.NETWORK_ID)

            evmTx.sign(key=self.env.config.GENESIS_KEY)
            try:
                apply_transaction(evmState, evmTx)
                block.addTx(Transaction(code=Code.createEvmCode(evmTx)))
            except Exception as e:
                Logger.errorException()
                return

    def createBlockToMine(self, createTime=None, address=None, includeTx=True, artificialTxConfig=None):
        """ Create a block to append and include TXs to maximize rewards
        """
        if not createTime:
            createTime = max(int(time.time()), self.headerTip.createTime + 1)
        difficulty = self.getNextBlockDifficulty(createTime)
        block = self.getTip().createBlockToAppend(
            createTime=createTime,
            address=address,
            difficulty=difficulty,
        )
        block.header.hashPrevRootBlock = self.rootTip.getHash()

        evmState = self.__getEvmStateForNewBlock(block)
        prevHeader = self.headerTip

        ancestorRootHeader = self.db.getRootBlockHeaderByHash(prevHeader.hashPrevRootBlock)
        check(self.__isSameRootChain(self.rootTip, ancestorRootHeader))

        self.__runCrossShardTxList(
            evmState=evmState,
            descendantRootHeader=self.rootTip,
            ancestorRootHeader=ancestorRootHeader)

        while evmState.gas_used < evmState.gas_limit:
            evmTx = self.txQueue.pop_transaction(
                max_gas=evmState.gas_limit - evmState.gas_used,
            )
            if evmTx is None:
                break

            try:
                apply_transaction(evmState, evmTx)
                block.addTx(Transaction(code=Code.createEvmCode(evmTx)))
            except Exception as e:
                Logger.errorException()

        if artificialTxConfig:
            self.__addArtificialTx(block, evmState, artificialTxConfig)

        # Put only half of block fee to coinbase address
        check(evmState.get_balance(evmState.block_coinbase) >= evmState.block_fee)
        evmState.delta_balance(evmState.block_coinbase, -evmState.block_fee // 2)

        # Update actual root hash
        evmState.commit()

        block.finalize(evmState=evmState)
        return block

    def getBlockByHash(self, h):
        ''' Return an validated block.  Return None if no such block exists in db
        '''
        return self.db.getMinorBlockByHash(h)

    def containBlockByHash(self, h):
        return self.db.containMinorBlockByHash(h)

    def getPendingTxSize(self):
        return self.transactionPool.size()

    #
    # ============================ Cross-shard transaction handling =============================
    #
    def addCrossShardTxListByMinorBlockHash(self, h, txList: CrossShardTransactionList):
        ''' Add a cross shard tx list from remote shard
        The list should be validated by remote shard, however,
        it is better to diagnose some bugs in peer shard if we could check
        - x-shard gas limit exceeded
        - it is a neighor of current shard following our routing rule
        '''
        self.db.putMinorBlockXshardTxList(h, txList)

    def addRootBlock(self, rBlock):
        ''' Add a root block.
        Make sure all cross shard tx lists of remote shards confirmed by the root block are in local db.
        '''
        if not self.db.containRootBlockByHash(rBlock.header.hashPrevBlock):
            raise ValueError("cannot find previous root block in pool")

        shardHeader = None
        for mHeader in rBlock.minorBlockHeaderList:
            h = mHeader.getHash()
            if mHeader.branch == self.branch:
                if not self.db.containMinorBlockByHash(h):
                    raise ValueError("cannot find minor block in local shard")
                if shardHeader is None or shardHeader.height < mHeader.height:
                    shardHeader = mHeader
                continue

            if not self.__isNeighbor(mHeader.branch):
                continue

            if not self.db.containRemoteMinorBlockHash(h):
                raise ValueError("cannot find xShard tx list")

        # shardHeader cannot be None since PROOF_OF_PROGRESS should be positive
        check(shardHeader is not None)

        self.db.putRootBlock(rBlock, shardHeader)
        check(self.__isSameRootChain(rBlock.header,
                                     self.db.getRootBlockHeaderByHash(shardHeader.hashPrevRootBlock)))

        if rBlock.header.height > self.rootTip.height:
            # Switch to the longest root block
            self.rootTip = rBlock.header
            self.confirmedHeaderTip = shardHeader
            self.confirmedMetaTip = self.db.getMinorBlockMetaByHash(shardHeader.getHash())

            origBlock = self.db.getMinorBlockByHeight(shardHeader.height)
            if not origBlock or origBlock.header != shardHeader:
                self.__rewriteBlockIndexTo(self.db.getMinorBlockByHash(shardHeader.getHash()))
                # TODO: shardHeader might not be the tip of the longest chain
                # need to switch to the tip of the longest chain
                self.headerTip = shardHeader
                self.metaTip = self.db.getMinorBlockMetaByHash(self.headerTip.getHash())
            return True

        check(self.__isSameRootChain(self.rootTip,
                                     self.db.getRootBlockHeaderByHash(self.headerTip.hashPrevRootBlock)))
        return False

    def __isNeighbor(self, remoteBranch):
        # TODO: Apply routing rule to determine neighors that could directly send x-shard tx
        return True

    def __getCrossShardTxListByRootBlockHash(self, h):
        rBlock = self.db.getRootBlockByHash(h)
        txList = []
        for mHeader in rBlock.minorBlockHeaderList:
            if mHeader.branch == self.branch:
                continue

            if not self.__isNeighbor(mHeader.branch):
                continue

            h = mHeader.getHash()
            txList.extend(self.db.getMinorBlockXshardTxList(h).txList)

        # Apply root block coinbase
        if self.branch.isInShard(rBlock.header.coinbaseAddress.fullShardId):
            txList.append(CrossShardTransactionDeposit(
                address=rBlock.header.coinbaseAddress,
                amount=rBlock.header.coinbaseAmount,
                gasPrice=0))
        return txList

    def __runCrossShardTxList(self, evmState, descendantRootHeader, ancestorRootHeader):
        rHeader = descendantRootHeader
        while rHeader != ancestorRootHeader:
            if rHeader.height == ancestorRootHeader.height:
                raise ValueError(
                    "incorrect ancestor root header: expected {}, actual {}",
                    rHeader.getHash().hex(),
                    ancestorRootHeader.getHash().hex())
            if evmState.gas_used == evmState.gas_limit:
                raise ValueError("gas consumed by cross-shard tx exceeding limit")

            txList = self.__getCrossShardTxListByRootBlockHash(rHeader.getHash())
            for tx in txList:
                evmState.delta_balance(tx.address.recipient, tx.amount)
                evmState.gas_used = min(evmState.gas_used + opcodes.GTXXSHARDCOST, evmState.gas_limit)
                evmState.block_fee += opcodes.GTXXSHARDCOST * tx.gasPrice
                evmState.delta_balance(evmState.block_coinbase, opcodes.GTXXSHARDCOST * tx.gasPrice)

            rHeader = self.db.getRootBlockHeaderByHash(rHeader.hashPrevBlock)

            # TODO: Check x-shard gas used is within limit
            # TODO: Refill local x-shard gas

    def containRemoteMinorBlockHash(self, h):
        return self.db.containRemoteMinorBlockHash(h)

    def getShardStats(self) -> ShardStats:
        return ShardStats(
            branch=self.branch,
            height=self.headerTip.height,
            timestamp=self.headerTip.createTime,
            txCount60s=self.db.txCounter60s.getCount(),
            pendingTxCount=len(self.txQueue),
        )
