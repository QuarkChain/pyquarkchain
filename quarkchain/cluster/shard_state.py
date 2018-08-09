import time
from collections import deque
from typing import Optional, Tuple

from quarkchain.cluster.genesis import create_genesis_blocks, create_genesis_evm_list
from quarkchain.cluster.rpc import ShardStats, TransactionDetail
from quarkchain.config import NetworkId
from quarkchain.core import calculate_merkle_root, Address, Branch, Code, Transaction
from quarkchain.core import (
    mk_receipt_sha,
    CrossShardTransactionList, CrossShardTransactionDeposit,
    RootBlock, MinorBlock, MinorBlockHeader, MinorBlockMeta,
    TransactionReceipt,
)
from quarkchain.evm import opcodes
from quarkchain.evm.messages import apply_transaction, validate_transaction
from quarkchain.evm.state import State as EvmState
from quarkchain.evm.transaction_queue import TransactionQueue
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.reward import ConstMinorBlockRewardCalcultor
from quarkchain.utils import Logger, check


class ExpiryQueue:
    """ A queue only keeps the elements added in the past ttl seconds """

    def __init__(self, ttlSec):
        self.__queue = deque()
        self.__ttl = ttlSec

    def __remove_expired_elements(self):
        current = time.time()
        while len(self.__queue) > 0 and self.__queue[0][0] < current:
            self.__queue.popleft()

    def append(self, e):
        self.__remove_expired_elements()
        self.__queue.append((time.time() + self.__ttl, e))

    def __iter__(self):
        self.__remove_expired_elements()
        for t, e in self.__queue:
            yield e

    def __getitem__(self, index):
        self.__remove_expired_elements()
        return self.__queue[index]

    def __len__(self):
        self.__remove_expired_elements()
        return len(self.__queue)

    def __str__(self):
        self.__remove_expired_elements()
        return str(self.__queue)


class ExpiryCounter:

    def __init__(self, windowSec):
        self.window = windowSec
        self.queue = ExpiryQueue(windowSec)

    def increment(self, value):
        self.queue.append(value)

    def get_count(self):
        return sum(self.queue)

    def get_count_per_second(self):
        return self.get_count() / self.window


class TransactionHistoryMixin:

    def __encode_address_transaction_key(self, address, height, index, crossShard):
        crossShardByte = b"\x00" if crossShard else b"\x01"
        return b"addr_" + address.serialize() + height.to_bytes(4, "big") + crossShardByte + index.to_bytes(4, "big")

    def put_confirmed_cross_shard_transaction_deposit_list(self, minorBlockHash, crossShardTransactionDepositList):
        """Stores a mapping from minor block to the list of CrossShardTransactionDeposit confirmed"""
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return

        l = CrossShardTransactionList(crossShardTransactionDepositList)
        self.db.put(b"xr_" + minorBlockHash, l.serialize())

    def __get_confirmed_cross_shard_transaction_deposit_list(self, mBlockHash):
        data = self.db.get(b"xr_" + mBlockHash, None)
        if not data:
            return []
        return CrossShardTransactionList.deserialize(data).txList

    def __update_transaction_history_index(self, tx, blockHeight, index, func):
        evmTx = tx.code.get_evm_transaction()
        addr = Address(evmTx.sender, evmTx.fromFullShardId)
        key = self.__encode_address_transaction_key(addr, blockHeight, index, False)
        func(key, b"")
        # "to" can be empty for smart contract deployment
        if evmTx.to and self.branch.is_in_shard(evmTx.toFullShardId):
            addr = Address(evmTx.to, evmTx.toFullShardId)
            key = self.__encode_address_transaction_key(addr, blockHeight, index, False)
            func(key, b"")

    def put_transaction_history_index(self, tx, blockHeight, index):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index(tx, blockHeight, index, lambda k, v: self.db.put(k, v))

    def remove_transaction_history_index(self, tx, blockHeight, index):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index(tx, blockHeight, index, lambda k, v: self.db.remove(k))

    def __update_transaction_history_index_from_block(self, minorBlock, func):
        xShardReceiveTxList = self.__get_confirmed_cross_shard_transaction_deposit_list(minorBlock.header.get_hash())
        for i, tx in enumerate(xShardReceiveTxList):
            if tx.txHash == bytes(32):  # coinbase reward for root block miner
                continue
            key = self.__encode_address_transaction_key(tx.toAddress, minorBlock.header.height, i, True)
            func(key, b"")

    def put_transaction_history_index_from_block(self, minorBlock):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index_from_block(minorBlock, lambda k, v: self.db.put(k, v))

    def remove_transaction_history_index_from_block(self, minorBlock):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index_from_block(minorBlock, lambda k, v: self.db.remove(k))

    def get_transactions_by_address(self, address, start=b"", limit=10):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        serializedAddress = address.serialize()
        end = b"addr_" + serializedAddress
        originalStart = (int.from_bytes(end, byteorder="big") + 1).to_bytes(len(end), byteorder="big")
        next = end
        # reset start to the latest if start is not valid
        if not start or start > originalStart:
            start = originalStart

        txList = []
        for k, v in self.db.reversed_range_iter(start, end):
            limit -= 1
            if limit < 0:
                break
            height = int.from_bytes(k[5 + 24:5 + 24 + 4], "big")
            crossShard = int(k[5 + 24 + 4]) == 0
            index = int.from_bytes(k[5 + 24 + 4 + 1:], "big")
            if crossShard:  # cross shard receive
                mBlock = self.get_minor_block_by_height(height)
                xShardReceiveTxList = self.__get_confirmed_cross_shard_transaction_deposit_list(mBlock.header.get_hash())
                tx = xShardReceiveTxList[index]  # tx is CrossShardTransactionDeposit
                txList.append(
                    TransactionDetail(
                        tx.txHash,
                        tx.fromAddress,
                        tx.toAddress,
                        tx.value,
                        height,
                        mBlock.header.createTime,
                        True,
                    )
                )
            else:
                mBlock = self.get_minor_block_by_height(height)
                receipt = mBlock.get_receipt(self.db, index)
                tx = mBlock.txList[index]  # tx is Transaction
                evmTx = tx.code.get_evm_transaction()
                txList.append(
                    TransactionDetail(
                        tx.get_hash(),
                        Address(evmTx.sender, evmTx.fromFullShardId),
                        Address(evmTx.to, evmTx.toFullShardId) if evmTx.to else None,
                        evmTx.value,
                        height,
                        mBlock.header.createTime,
                        receipt.success == b"\x01",
                    )
                )
            next = (int.from_bytes(k, byteorder="big") - 1).to_bytes(len(k), byteorder="big")

        return txList, next


class ShardDbOperator(TransactionHistoryMixin):
    def __init__(self, db, env, branch):
        self.env = env
        self.db = db
        self.branch = branch
        # TODO:  iterate db to recover pools and set
        self.mHeaderPool = dict()
        self.mMetaPool = dict()
        self.xShardSet = set()
        self.rHeaderPool = dict()
        self.rMinorHeaderPool = dict()

        # height -> set(minor block hash) for counting wasted blocks
        self.heightToMinorBlockHashes = dict()

    def __get_last_minor_block_in_root_block(self, rootBlock):
        lHeader = None
        for mHeader in rootBlock.minorBlockHeaderList:
            if mHeader.branch != self.branch:
                continue

            if lHeader is None or mHeader.height > lHeader.height:
                lHeader = mHeader

        check(lHeader is not None)
        return lHeader

    def recover_state(self, rHeader, mHeader):
        """ When recovering from local database, we can only guarantee the consistency of the best chain.
        Forking blocks can be in inconsistent state and thus should be pruned from the database
        so that they can be retried in the future.
        """
        rHash = rHeader.get_hash()
        while len(self.rHeaderPool) < self.env.config.MAX_ROOT_BLOCK_IN_MEMORY:
            block = RootBlock.deserialize(self.db.get(b"rblock_" + rHash))
            self.rMinorHeaderPool[rHash] = self.__get_last_minor_block_in_root_block(block)
            self.rHeaderPool[rHash] = block.header
            if block.header.height <= 0:
                break
            rHash = block.header.hashPrevBlock

        mHash = mHeader.get_hash()
        while len(self.mHeaderPool) < self.env.config.MAX_MINOR_BLOCK_IN_MEMORY:
            block = MinorBlock.deserialize(self.db.get(b"mblock_" + mHash))
            self.mHeaderPool[mHash] = block.header
            self.mMetaPool[mHash] = block.meta
            if block.header.height <= 0:
                break
            mHash = block.header.hashPrevMinorBlock

        Logger.info("[{}] recovered {} minor blocks and {} root blocks".format(
            self.branch.get_shard_id(), len(self.mHeaderPool), len(self.rHeaderPool)))

    # ------------------------- Root block db operations --------------------------------
    def put_root_block(self, rootBlock, rMinorHeader, rootBlockHash=None):
        """ rMinorHeader: the minor header of the shard in the root block with largest height
        """
        if rootBlockHash is None:
            rootBlockHash = rootBlock.header.get_hash()

        self.db.put(b"rblock_" + rootBlockHash, rootBlock.serialize())
        self.rHeaderPool[rootBlockHash] = rootBlock.header
        self.rMinorHeaderPool[rootBlockHash] = rMinorHeader

    def get_root_block_by_hash(self, h):
        if h not in self.rHeaderPool:
            return None
        return RootBlock.deserialize(self.db.get(b"rblock_" + h))

    def get_root_block_header_by_hash(self, h):
        return self.rHeaderPool.get(h, None)

    def contain_root_block_by_hash(self, h):
        return h in self.rHeaderPool

    def get_last_minor_block_in_root_block(self, h):
        if h not in self.rHeaderPool:
            return None
        return self.rMinorHeaderPool.get(h)

    # ------------------------- Minor block db operations --------------------------------
    def put_minor_block(self, mBlock, xShardReceiveTxList):
        mBlockHash = mBlock.header.get_hash()

        self.db.put(b"mblock_" + mBlockHash, mBlock.serialize())
        self.put_total_tx_count(mBlock)

        self.mHeaderPool[mBlockHash] = mBlock.header
        self.mMetaPool[mBlockHash] = mBlock.meta

        self.heightToMinorBlockHashes.setdefault(mBlock.header.height, set()).add(mBlock.header.get_hash())

        self.put_confirmed_cross_shard_transaction_deposit_list(mBlockHash, xShardReceiveTxList)

    def put_total_tx_count(self, mBlock):
        prevCount = 0
        if mBlock.header.height > 2:
            prevCount = self.get_total_tx_count(mBlock.header.hashPrevMinorBlock)
        count = prevCount + len(mBlock.txList)
        self.db.put(b"txCount_"+ mBlock.header.get_hash(), count.to_bytes(4, "big"))

    def get_total_tx_count(self, mBlockHash):
        countBytes = self.db.get(b"txCount_" + mBlockHash, None)
        if not countBytes:
            return 0
        return int.from_bytes(countBytes, "big")

    def get_minor_block_header_by_hash(self, h, consistencyCheck=True):
        header = self.mHeaderPool.get(h, None)
        if not header and not consistencyCheck:
            block = self.get_minor_block_by_hash(h, False)
            header = block.header
        return header

    def get_minor_block_evm_root_hash_by_hash(self, h):
        if h not in self.mHeaderPool:
            return None
        check(h in self.mMetaPool)
        meta = self.mMetaPool[h]
        return meta.hashEvmStateRoot

    def get_minor_block_meta_by_hash(self, h):
        return self.mMetaPool.get(h, None)

    def get_minor_block_by_hash(self, h, consistencyCheck=True):
        if consistencyCheck and h not in self.mHeaderPool:
            return None
        return MinorBlock.deserialize(self.db.get(b"mblock_" + h))

    def contain_minor_block_by_hash(self, h):
        return h in self.mHeaderPool

    def put_minor_block_index(self, block):
        self.db.put(b"mi_%d" % block.header.height, block.header.get_hash())

    def remove_minor_block_index(self, block):
        self.db.remove(b"mi_%d" % block.header.height)

    def get_minor_block_by_height(self, height):
        key = b"mi_%d" % height
        if key not in self.db:
            return None
        blockHash = self.db.get(key)
        return self.get_minor_block_by_hash(blockHash, False)

    def get_block_count_by_height(self, height):
        """ Return the total number of blocks with the given height"""
        return len(self.heightToMinorBlockHashes.setdefault(height, set()))

    # ------------------------- Transaction db operations --------------------------------
    def put_transaction_index(self, tx, blockHeight, index):
        txHash = tx.get_hash()
        self.db.put(b"txindex_" + txHash,
                    blockHeight.to_bytes(4, "big") + index.to_bytes(4, "big"))

        self.put_transaction_history_index(tx, blockHeight, index)

    def remove_transaction_index(self, tx, blockHeight, index):
        txHash = tx.get_hash()
        self.db.remove(b"txindex_" + txHash)

        self.remove_transaction_history_index(tx, blockHeight, index)

    def contain_transaction_hash(self, txHash):
        key = b"txindex_" + txHash
        return key in self.db

    def get_transaction_by_hash(self, txHash) -> Tuple[Optional[MinorBlock], Optional[int]]:
        result = self.db.get(b"txindex_" + txHash, None)
        if not result:
            return None, None
        check(len(result) == 8)
        blockHeight = int.from_bytes(result[:4], "big")
        index = int.from_bytes(result[4:], "big")
        return self.get_minor_block_by_height(blockHeight), index

    def put_transaction_index_from_block(self, minorBlock):
        for i, tx in enumerate(minorBlock.txList):
            self.put_transaction_index(tx, minorBlock.header.height, i)

        self.put_transaction_history_index_from_block(minorBlock)

    def remove_transaction_index_from_block(self, minorBlock):
        for i, tx in enumerate(minorBlock.txList):
            self.remove_transaction_index(tx, minorBlock.header.height, i)

        self.remove_transaction_history_index_from_block(minorBlock)

    # -------------------------- Cross-shard tx operations ----------------------------
    def put_minor_block_xshard_tx_list(self, h, txList: CrossShardTransactionList):
        # self.xShardSet.add(h)
        self.db.put(b"xShard_" + h, txList.serialize())

    def get_minor_block_xshard_tx_list(self, h) -> CrossShardTransactionList:
        return CrossShardTransactionList.deserialize(self.db.get(b"xShard_" + h))

    def contain_remote_minor_block_hash(self, h):
        key = b"xShard_" + h
        return key in self.db

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
    - root blockchain and cross-shard transactiond
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
        self.db = ShardDbOperator(self.rawDb, self.env, self.branch)
        self.txQueue = TransactionQueue()  # queue of EvmTransaction
        self.txDict = dict()  # hash -> Transaction for explorer
        self.initialized = False

        # assure ShardState is in good shape after constructor returns though we still
        # rely on master calling init_from_root_block to bring the cluster into consistency
        self.__create_genesis_blocks(shardId)

    def init_from_root_block(self, rootBlock):
        """ Master will send its root chain tip when it connects to slaves.
        Shards will initialize its state based on the root block.
        """
        def __get_header_tip_from_root_block(branch):
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
            rootBlock.header.height, rootBlock.header.get_hash().hex()))

        if rootBlock.header.height <= 1:
            Logger.info("Created genesis block")
            return

        shardSize = rootBlock.header.shardInfo.get_shard_size()
        check(self.branch == Branch.create(shardSize, self.shardId))
        self.rootTip = rootBlock.header
        self.headerTip = __get_header_tip_from_root_block(self.branch)

        self.db.recover_state(self.rootTip, self.headerTip)
        Logger.info("[{}] done recovery from db. shard tip {} {} root tip {} {}".format(
            self.branch.get_shard_id(), self.headerTip.height, self.headerTip.get_hash().hex(),
            self.rootTip.height, self.rootTip.get_hash().hex()))

        self.metaTip = self.db.get_minor_block_meta_by_hash(self.headerTip.get_hash())
        self.confirmedHeaderTip = self.headerTip
        self.confirmedMetaTip = self.metaTip
        self.evmState = self.__create_evm_state()
        self.evmState.trie.root_hash = self.metaTip.hashEvmStateRoot
        check(self.db.get_minor_block_evm_root_hash_by_hash(self.headerTip.get_hash()) == self.metaTip.hashEvmStateRoot)

        self.__rewrite_block_index_to(self.db.get_minor_block_by_hash(self.headerTip.get_hash()), add_txBackToQueue=False)

    def __create_evm_state(self):
        return EvmState(env=self.env.evmEnv, db=self.rawDb)

    def __create_genesis_blocks(self, shardId):
        evmList = create_genesis_evm_list(env=self.env, dbMap={self.shardId: self.rawDb})
        genesisRootBlock0, genesisRootBlock1, gMinorBlockList0, gMinorBlockList1 = create_genesis_blocks(
            env=self.env, evmList=evmList)

        # Add x-shard list to db
        for mBlock1, evmState in zip(gMinorBlockList1, evmList):
            if mBlock1.header.branch.get_shard_id() == shardId:
                continue
            self.add_cross_shard_tx_list_by_minor_block_hash(
                mBlock1.header.get_hash(), CrossShardTransactionList(txList=[]))

        # Local helper variables
        genesisMinorBlock0 = gMinorBlockList0[self.shardId]
        genesisMinorBlock1 = gMinorBlockList1[self.shardId]
        check(genesisMinorBlock1.header.branch.get_shard_id() == self.shardId)

        check(genesisMinorBlock0.header.branch == self.branch)
        self.evmState = evmList[self.shardId]
        self.db.put_minor_block(genesisMinorBlock0, [])
        self.db.put_minor_block_index(genesisMinorBlock0)
        self.db.put_minor_block(genesisMinorBlock1, [])
        self.db.put_minor_block_index(genesisMinorBlock1)
        self.db.put_root_block(genesisRootBlock0, genesisMinorBlock0.header)
        self.db.put_root_block(genesisRootBlock1, genesisMinorBlock1.header)

        self.rootTip = genesisRootBlock1.header
        # Tips that are confirmed by root
        self.confirmedHeaderTip = genesisMinorBlock1.header
        self.confirmedMetaTip = genesisMinorBlock1.header
        # Tips that are unconfirmed by root
        self.headerTip = genesisMinorBlock1.header
        self.metaTip = genesisMinorBlock1.meta

    def __validate_tx(self, tx: Transaction, evmState, fromAddress=None) -> EvmTransaction:
        """fromAddress will be set for execute_tx"""
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
        if not tx.code.is_evm():
            raise RuntimeError("only evm transaction is supported now")

        evmTx = tx.code.get_evm_transaction()

        if fromAddress:
            check(evmTx.fromFullShardId == fromAddress.fullShardId)
            nonce = evmState.get_nonce(fromAddress.recipient)
            # have to create a new evmTx as nonce is immutable
            evmTx = EvmTransaction(
                nonce, evmTx.gasprice, evmTx.startgas, evmTx.to, evmTx.value, evmTx.data,
                fromFullShardId=evmTx.fromFullShardId, toFullShardId=evmTx.toFullShardId, networkId=evmTx.networkId)
            evmTx.sender = fromAddress.recipient

        evmTx.set_shard_size(self.branch.get_shard_size())

        if evmTx.networkId != self.env.config.NETWORK_ID:
            raise RuntimeError("evm tx network id mismatch. expect {} but got {}".format(
                self.env.config.NETWORK_ID, evmTx.networkId))

        if evmTx.from_shard_id() != self.branch.get_shard_id():
            raise RuntimeError("evm tx from_shard_id mismatch. expect {} but got {}".format(
                self.branch.get_shard_id(), evmTx.from_shard_id(),
            ))

        # This will check signature, nonce, balance, gas limit
        validate_transaction(evmState, evmTx)

        # TODO: Neighborhood and xshard gas limit check
        return evmTx

    def add_tx(self, tx: Transaction):
        if len(self.txQueue) > self.env.config.TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD:
            # exceeding tx queue size limit
            return False

        if self.db.contain_transaction_hash(tx.get_hash()):
            return False

        txHash = tx.get_hash()
        if txHash in self.txDict:
            return False

        evmState = self.evmState.ephemeral_clone()
        evmState.gas_used = 0
        try:
            evmTx = self.__validate_tx(tx, evmState)
            self.txQueue.add_transaction(evmTx)
            self.txDict[txHash] = tx
            return True
        except Exception as e:
            Logger.warningEverySec("Failed to add transaction: {}".format(e), 1)
            return False

    def execute_tx(self, tx: Transaction, fromAddress) -> Optional[bytes]:
        state = self.evmState.ephemeral_clone()
        state.gas_used = 0
        try:
            evmTx = self.__validate_tx(tx, state, fromAddress)
            success, output = apply_transaction(state, evmTx, tx_wrapper_hash=bytes(32))
            return output if success else None
        except Exception as e:
            Logger.warningEverySec("failed to apply transaction: {}".format(e), 1)
            return None

    def __get_evm_state_for_new_block(self, block, ephemeral=True):
        state = self.__create_evm_state()
        if ephemeral:
            state = state.ephemeral_clone()
        state.trie.root_hash = self.db.get_minor_block_evm_root_hash_by_hash(block.header.hashPrevMinorBlock)
        state.timestamp = block.header.createTime
        state.gas_limit = block.meta.evmGasLimit  # TODO
        state.block_number = block.header.height
        state.recent_uncles[state.block_number] = []  # TODO [x.hash for x in block.uncles]
        # TODO: Create a account with shard info if the account is not created
        # Right now the fullShardId for coinbase actually comes from the first tx that got applied
        state.block_coinbase = block.meta.coinbaseAddress.recipient
        state.block_difficulty = block.header.difficulty
        state.block_reward = 0
        state.prev_headers = []  # TODO: state.add_block_header(block.header)
        return state

    def __is_same_minor_chain(self, longerBlockHeader, shorterBlockHeader):
        if shorterBlockHeader.height > longerBlockHeader.height:
            return False

        header = longerBlockHeader
        for i in range(longerBlockHeader.height - shorterBlockHeader.height):
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)
        return header == shorterBlockHeader

    def __is_same_root_chain(self, longerBlockHeader, shorterBlockHeader):
        if shorterBlockHeader.height > longerBlockHeader.height:
            return False

        header = longerBlockHeader
        for i in range(longerBlockHeader.height - shorterBlockHeader.height):
            header = self.db.get_root_block_header_by_hash(header.hashPrevBlock)
        return header == shorterBlockHeader

    def __validate_block(self, block):
        """ Validate a block before running evm transactions
        """
        if block.header.height <= 1:
            raise ValueError("unexpected height")

        if not self.db.contain_minor_block_by_hash(block.header.hashPrevMinorBlock):
            # TODO:  May put the block back to queue
            raise ValueError("[{}] prev block not found, block height {} prev hash {}".format(self.branch.get_shard_id(), block.header.height, block.header.hashPrevMinorBlock.hex()))
        prevHeader = self.db.get_minor_block_header_by_hash(block.header.hashPrevMinorBlock)

        if block.header.height != prevHeader.height + 1:
            raise ValueError("height mismatch")

        if block.header.branch != self.branch:
            raise ValueError("branch mismatch")

        if block.header.createTime <= prevHeader.createTime:
            raise ValueError("incorrect create time tip time {}, new block time {}".format(
                block.header.createTime, self.chain[-1].createTime))

        if block.header.hashMeta != block.meta.get_hash():
            raise ValueError("Hash of meta mismatch")

        if len(block.meta.extraData) > self.env.config.BLOCK_EXTRA_DATA_SIZE_LIMIT:
            raise ValueError("extraData in block is too large")

        # Make sure merkle tree is valid
        merkleHash = calculate_merkle_root(block.txList)
        if merkleHash != block.meta.hashMerkleRoot:
            raise ValueError("incorrect merkle root")

        # Check the first transaction of the block
        if not self.branch.is_in_shard(block.meta.coinbaseAddress.fullShardId):
            raise ValueError("coinbase output address must be in the shard")

        # Check difficulty
        if not self.env.config.SKIP_MINOR_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == NetworkId.MAINNET:
                diff = self.diffCalc.calculate_diff_with_parent(prevHeader, block.header.createTime)
                if diff != block.header.difficulty:
                    raise ValueError("incorrect difficulty")
                metric = diff * int.from_bytes(block.header.get_hash(), byteorder="big")
                if metric >= 2 ** 256:
                    raise ValueError("insufficient difficulty")
            elif block.meta.coinbaseAddress.recipient != self.env.config.TESTNET_MASTER_ACCOUNT.recipient:
                raise ValueError("incorrect master to create the block")

        if not self.branch.is_in_shard(block.meta.coinbaseAddress.fullShardId):
            raise ValueError("coinbase output must be in local shard")

        # Check whether the root header is in the root chain
        rootBlockHeader = self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock)
        if rootBlockHeader is None:
            raise ValueError("cannot find root block for the minor block")

        if rootBlockHeader.height < self.db.get_root_block_header_by_hash(prevHeader.hashPrevRootBlock).height:
            raise ValueError("prev root block height must be non-decreasing")

        prevConfirmedMinorBlock = self.db.get_last_minor_block_in_root_block(block.header.hashPrevRootBlock)
        if not self.__is_same_minor_chain(prevHeader, prevConfirmedMinorBlock):
            raise ValueError("prev root block's minor block is not in the same chain as the minor block")

        if not self.__is_same_root_chain(self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock),
                                      self.db.get_root_block_header_by_hash(prevHeader.hashPrevRootBlock)):
            raise ValueError("prev root blocks are not on the same chain")

    def run_block(self, block, evmState=None, evmTxIncluded=None, xShardReceiveTxList=None):
        if evmTxIncluded is None:
            evmTxIncluded = []
        if xShardReceiveTxList is None:
            xShardReceiveTxList = []
        if evmState is None:
            evmState = self.__get_evm_state_for_new_block(block, ephemeral=False)
        rootBlockHeader = self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock)
        prevHeader = self.db.get_minor_block_header_by_hash(block.header.hashPrevMinorBlock)

        xShardReceiveTxList.extend(self.__run_cross_shard_tx_list(
            evmState=evmState,
            descendantRootHeader=rootBlockHeader,
            ancestorRootHeader=self.db.get_root_block_header_by_hash(prevHeader.hashPrevRootBlock)))

        for idx, tx in enumerate(block.txList):
            try:
                evmTx = self.__validate_tx(tx, evmState)
                evmTx.set_shard_size(self.branch.get_shard_size())
                apply_transaction(evmState, evmTx, tx.get_hash())
                evmTxIncluded.append(evmTx)
            except Exception as e:
                Logger.debugException()
                Logger.debug("failed to process Tx {}, idx {}, reason {}".format(
                    tx.get_hash().hex(), idx, e))
                raise e

        # Put only half of block fee to coinbase address
        check(evmState.get_balance(evmState.block_coinbase) >= evmState.block_fee)
        evmState.delta_balance(evmState.block_coinbase, -evmState.block_fee // 2)

        # Update actual root hash
        evmState.commit()
        return evmState

    def __is_minor_block_linked_to_root_tip(self, mBlock):
        """ Determine whether a minor block is a descendant of a minor block confirmed by root tip
        """
        if mBlock.header.height <= self.confirmedHeaderTip.height:
            return False

        header = mBlock.header
        for i in range(mBlock.header.height - self.confirmedHeaderTip.height):
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)

        return header == self.confirmedHeaderTip

    def __rewrite_block_index_to(self, minorBlock, add_txBackToQueue=True):
        """ Find the common ancestor in the current chain and rewrite index till minorblock """
        newChain = []
        oldChain = []

        # minorBlock height could be lower than the current tip
        # we should revert all the blocks above minorBlock height
        height = minorBlock.header.height + 1
        while True:
            origBlock = self.db.get_minor_block_by_height(height)
            if not origBlock:
                break
            oldChain.append(origBlock)
            height += 1

        block = minorBlock
        # Find common ancestor and record the blocks that needs to be updated
        while block.header.height >= 0:
            origBlock = self.db.get_minor_block_by_height(block.header.height)
            if origBlock and origBlock.header == block.header:
                break
            newChain.append(block)
            if origBlock:
                oldChain.append(origBlock)
            block = self.db.get_minor_block_by_hash(block.header.hashPrevMinorBlock)

        for block in oldChain:
            self.db.remove_transaction_index_from_block(block)
            self.db.remove_minor_block_index(block)
            if add_txBackToQueue:
                self.__add_transactions_from_block(block)
        for block in newChain:
            self.db.put_transaction_index_from_block(block)
            self.db.put_minor_block_index(block)
            self.__remove_transactions_from_block(block)

    def __add_transactions_from_block(self, block):
        for tx in block.txList:
            self.txDict[tx.get_hash()] = tx
            self.txQueue.add_transaction(tx.code.get_evm_transaction())

    def __remove_transactions_from_block(self, block):
        evmTxList = []
        for tx in block.txList:
            self.txDict.pop(tx.get_hash(), None)
            evmTxList.append(tx.code.get_evm_transaction())
        self.txQueue = self.txQueue.diff(evmTxList)

    def add_block(self, block):
        """  Add a block to local db.  Perform validate and update tip accordingly
        Returns None if block is already added.
        Returns a list of CrossShardTransactionDeposit from block.
        Raises on any error.
        """
        startTime = time.time()
        if self.headerTip.height - block.header.height > 700:
            Logger.info("[{}] drop old block {} << {}".format(block.header.height, self.headerTip.height))
            return None
        if self.db.contain_minor_block_by_hash(block.header.get_hash()):
            return None

        evmTxIncluded = []
        xShardReceiveTxList = []
        # Throw exception if fail to run
        self.__validate_block(block)
        evmState = self.run_block(block, evmTxIncluded=evmTxIncluded, xShardReceiveTxList=xShardReceiveTxList)

        # ------------------------ Validate ending result of the block --------------------
        if block.meta.hashEvmStateRoot != evmState.trie.root_hash:
            raise ValueError("State root mismatch: header %s computed %s" %
                             (block.meta.hashEvmStateRoot.hex(), evmState.trie.root_hash.hex()))

        receiptRoot = mk_receipt_sha(evmState.receipts, evmState.db)
        if block.meta.hashEvmReceiptRoot != receiptRoot:
            raise ValueError("Receipt root mismatch: header {} computed {}".format(
                block.meta.hashEvmReceiptRoot.hex(), receiptRoot.hex()
            ))

        if evmState.gas_used != block.meta.evmGasUsed:
            raise ValueError("Gas used mismatch: header %d computed %d" %
                             (block.meta.evmGasUsed, evmState.gas_used))

        if evmState.xshard_receive_gas_used != block.meta.evmCrossShardReceiveGasUsed:
            raise ValueError("X-shard gas used mismatch: header %d computed %d" %
                             (block.meta.evmCrossShardReceiveGasUsed, evmState.xshard_receive_gas_used))

        # The rest fee goes to root block
        if evmState.block_fee // 2 != block.header.coinbaseAmount:
            raise ValueError("Coinbase reward incorrect")
        # TODO: Check evm bloom

        # TODO: Add block reward to coinbase
        # self.rewardCalc.get_block_reward(self):
        self.db.put_minor_block(block, xShardReceiveTxList)

        # Update tip if a block is appended or a fork is longer (with the same ancestor confirmed by root block tip)
        # or they are equal length but the root height confirmed by the block is longer
        updateTip = False
        if not self.__is_same_root_chain(self.rootTip, self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock)):
            # Don't update tip if the block depends on a root block that is not rootTip or rootTip's ancestor
            updateTip = False
        elif block.header.hashPrevMinorBlock == self.headerTip.get_hash():
            updateTip = True
        elif self.__is_minor_block_linked_to_root_tip(block):
            if block.header.height > self.headerTip.height:
                updateTip = True
            elif block.header.height == self.headerTip.height:
                updateTip = self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock).height > \
                            self.db.get_root_block_header_by_hash(self.headerTip.hashPrevRootBlock).height

        if updateTip:
            self.__rewrite_block_index_to(block)
            self.evmState = evmState
            self.headerTip = block.header
            self.metaTip = block.meta

        check(self.__is_same_root_chain(self.rootTip, self.db.get_root_block_header_by_hash(self.headerTip.hashPrevRootBlock)))
        endTime = time.time()
        Logger.debug("Add block took {} seconds for {} tx".format(
            endTime - startTime, len(block.txList)
        ))
        return evmState.xshard_list

    def get_tip(self):
        return self.db.get_minor_block_by_hash(self.headerTip.get_hash())

    def tip(self):
        """ Called in diff.py """
        return self.headerTip

    def finalize_and_add_block(self, block):
        block.finalize(evmState=self.run_block(block))
        self.add_block(block)

    def get_block_header_by_height(self, height):
        pass

    def get_balance(self, recipient):
        return self.evmState.get_balance(recipient)

    def get_transaction_count(self, recipient):
        return self.evmState.get_nonce(recipient)

    def get_code(self, recipient):
        return self.evmState.get_code(recipient)

    def get_next_block_difficulty(self, createTime=None):
        if not createTime:
            createTime = max(int(time.time()), self.headerTip.createTime + 1)
        return self.diffCalc.calculate_diff_with_parent(self.headerTip, createTime)

    def get_next_block_reward(self):
        return self.rewardCalc.get_block_reward(self)

    def get_next_block_coinbase_amount(self):
        # TODO: add block reward
        # TODO: the current calculation is bogus and just serves as a placeholder.
        coinbase = 0
        for txWrapper in self.txQueue.peek():
            tx = txWrapper.tx
            coinbase += tx.gasprice * tx.startgas

        if self.rootTip.get_hash() != self.headerTip.hashPrevRootBlock:
            txs = self.__get_cross_shard_tx_list_by_root_block_hash(self.rootTip.get_hash())
            for tx in txs:
                coinbase += tx.gasPrice * opcodes.GTXXSHARDCOST

        return coinbase

    def get_unconfirmed_headers_coinbase_amount(self):
        amount = 0
        header = self.headerTip
        for i in range(header.height - self.confirmedHeaderTip.height):
            amount += header.coinbaseAmount
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)
        check(header == self.confirmedHeaderTip)
        return amount

    def get_unconfirmed_header_list(self):
        """ height in ascending order """
        headerList = []
        header = self.headerTip
        for i in range(header.height - self.confirmedHeaderTip.height):
            headerList.append(header)
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)
        check(header == self.confirmedHeaderTip)
        headerList.reverse()
        return headerList

    def __add_transactions_to_fund_loadtest_accounts(self, block, evmState):
        height = block.header.height
        startIndex = (height - 2) * 500
        shardMask = self.branch.get_shard_size() - 1
        for i in range(500):
            index = startIndex + i
            if index >= len(self.env.config.LOADTEST_ACCOUNTS):
                return
            address, key = self.env.config.LOADTEST_ACCOUNTS[index]
            fromFullShardId = self.env.config.GENESIS_ACCOUNT.fullShardId & (~shardMask) | self.branch.get_shard_id()
            toFullShardId = address.fullShardId & (~shardMask) | self.branch.get_shard_id()
            gas = 21000
            evmTx = EvmTransaction(
                nonce=evmState.get_nonce(evmState.block_coinbase),
                gasprice=3 * (10 ** 9),
                startgas=gas,
                to=address.recipient,
                value=10 * (10 ** 18),
                data=b'',
                fromFullShardId=fromFullShardId,
                toFullShardId=toFullShardId,
                networkId=self.env.config.NETWORK_ID,
            )
            evmTx.sign(key=self.env.config.GENESIS_KEY)
            evmTx.set_shard_size(self.branch.get_shard_size())
            try:
                # tx_wrapper_hash is not needed for in-shard tx
                apply_transaction(evmState, evmTx, tx_wrapper_hash=bytes(32))
                block.add_tx(Transaction(code=Code.create_evm_code(evmTx)))
            except Exception as e:
                Logger.errorException()
                return

    def create_block_to_mine(self, createTime=None, address=None, gasLimit=None):
        """ Create a block to append and include TXs to maximize rewards
        """
        startTime = time.time()
        if not createTime:
            createTime = max(int(time.time()), self.headerTip.createTime + 1)
        difficulty = self.get_next_block_difficulty(createTime)
        block = self.get_tip().create_block_to_append(
            createTime=createTime,
            address=address,
            difficulty=difficulty,
        )

        evmState = self.__get_evm_state_for_new_block(block)

        if gasLimit is not None:
            # Set gasLimit.  Since gas limit is auto adjusted between blocks, this is for test purpose only.
            evmState.gas_limit = gasLimit

        prevHeader = self.headerTip
        ancestorRootHeader = self.db.get_root_block_header_by_hash(prevHeader.hashPrevRootBlock)
        check(self.__is_same_root_chain(self.rootTip, ancestorRootHeader))

        # cross-shard receive must be handled before including tx from txQueue
        block.header.hashPrevRootBlock = self.__include_cross_shard_tx_list(
            evmState=evmState,
            descendantRootHeader=self.rootTip,
            ancestorRootHeader=ancestorRootHeader).get_hash()

        # fund load test accounts
        self.__add_transactions_to_fund_loadtest_accounts(block, evmState)

        popedTxs = []
        while evmState.gas_used < evmState.gas_limit:
            evmTx = self.txQueue.pop_transaction(
                max_gas=evmState.gas_limit - evmState.gas_used,
            )
            if evmTx is None:
                break
            evmTx.set_shard_size(self.branch.get_shard_size())
            try:
                tx = Transaction(code=Code.create_evm_code(evmTx))
                apply_transaction(evmState, evmTx, tx.get_hash())
                block.add_tx(tx)
                popedTxs.append(evmTx)
            except Exception as e:
                Logger.warningEverySec("Failed to include transaction: {}".format(e), 1)
                tx = Transaction(code=Code.create_evm_code(evmTx))
                self.txDict.pop(tx.get_hash(), None)

        # We don't want to drop the transactions if the mined block failed to be appended
        for evmTx in popedTxs:
            self.txQueue.add_transaction(evmTx)

        # Put only half of block fee to coinbase address
        check(evmState.get_balance(evmState.block_coinbase) >= evmState.block_fee)
        evmState.delta_balance(evmState.block_coinbase, -evmState.block_fee // 2)

        # Update actual root hash
        evmState.commit()

        block.finalize(evmState=evmState)

        endTime= time.time()
        Logger.debug("Create block to mine took {} seconds for {} tx".format(
            endTime - startTime, len(block.txList)
        ))
        return block

    def get_block_by_hash(self, h):
        """ Return an validated block.  Return None if no such block exists in db
        """
        return self.db.get_minor_block_by_hash(h)

    def contain_block_by_hash(self, h):
        return self.db.contain_minor_block_by_hash(h)

    def get_pending_tx_size(self):
        return self.transactionPool.size()

    #
    # ============================ Cross-shard transaction handling =============================
    #
    def add_cross_shard_tx_list_by_minor_block_hash(self, h, txList: CrossShardTransactionList):
        """ Add a cross shard tx list from remote shard
        The list should be validated by remote shard, however,
        it is better to diagnose some bugs in peer shard if we could check
        - x-shard gas limit exceeded
        - it is a neighor of current shard following our routing rule
        """
        self.db.put_minor_block_xshard_tx_list(h, txList)

    def add_root_block(self, rBlock):
        """ Add a root block.
        Make sure all cross shard tx lists of remote shards confirmed by the root block are in local db.
        """
        if not self.db.contain_root_block_by_hash(rBlock.header.hashPrevBlock):
            raise ValueError("cannot find previous root block in pool")

        shardHeader = None
        for mHeader in rBlock.minorBlockHeaderList:
            h = mHeader.get_hash()
            if mHeader.branch == self.branch:
                if not self.db.contain_minor_block_by_hash(h):
                    raise ValueError("cannot find minor block in local shard")
                if shardHeader is None or shardHeader.height < mHeader.height:
                    shardHeader = mHeader
                continue

            if not self.__is_neighbor(mHeader.branch):
                continue

            if not self.db.contain_remote_minor_block_hash(h):
                raise ValueError("cannot find xShard tx list for {}-{} {}".format(
                    mHeader.branch.get_shard_id(), mHeader.height, h.hex()))

        # shardHeader cannot be None since PROOF_OF_PROGRESS should be positive
        check(shardHeader is not None)

        self.db.put_root_block(rBlock, shardHeader)
        check(self.__is_same_root_chain(rBlock.header,
                                     self.db.get_root_block_header_by_hash(shardHeader.hashPrevRootBlock)))

        if rBlock.header.height > self.rootTip.height:
            # Switch to the longest root block
            self.rootTip = rBlock.header
            self.confirmedHeaderTip = shardHeader
            self.confirmedMetaTip = self.db.get_minor_block_meta_by_hash(shardHeader.get_hash())

            origHeaderTip = self.headerTip
            origBlock = self.db.get_minor_block_by_height(shardHeader.height)
            if not origBlock or origBlock.header != shardHeader:
                self.__rewrite_block_index_to(self.db.get_minor_block_by_hash(shardHeader.get_hash()))
                # TODO: shardHeader might not be the tip of the longest chain
                # need to switch to the tip of the longest chain
                self.headerTip = shardHeader
                self.metaTip = self.db.get_minor_block_meta_by_hash(self.headerTip.get_hash())
                Logger.info("[{}] (root confirms a fork) shard tip reset from {} to {} by root block {}".format(
                    self.branch.get_shard_id(), origHeaderTip.height, self.headerTip.height, rBlock.header.height))
            else:
                # the current headerTip might point to a root block on a fork with rBlock
                # we need to scan back until finding a minor block pointing to the same root chain rBlock is on.
                # the worst case would be that we go all the way back to origBlock (shardHeader)
                while not self.__is_same_root_chain(
                        self.rootTip, self.db.get_root_block_header_by_hash(self.headerTip.hashPrevRootBlock)):
                    self.headerTip = self.db.get_minor_block_header_by_hash(self.headerTip.hashPrevMinorBlock)
                if self.headerTip != origHeaderTip:
                    Logger.info("[{}] shard tip reset from {} to {} by root block {}".format(
                        self.branch.get_shard_id(), origHeaderTip.height, self.headerTip.height, rBlock.header.height))
            return True

        check(self.__is_same_root_chain(self.rootTip,
                                     self.db.get_root_block_header_by_hash(self.headerTip.hashPrevRootBlock)))
        return False

    def __is_neighbor(self, remoteBranch):
        # TODO: Apply routing rule to determine neighors that could directly send x-shard tx
        return True

    def __get_cross_shard_tx_list_by_root_block_hash(self, h):
        rBlock = self.db.get_root_block_by_hash(h)
        txList = []
        for mHeader in rBlock.minorBlockHeaderList:
            if mHeader.branch == self.branch:
                continue

            if not self.__is_neighbor(mHeader.branch):
                continue

            h = mHeader.get_hash()
            txList.extend(self.db.get_minor_block_xshard_tx_list(h).txList)

        # Apply root block coinbase
        if self.branch.is_in_shard(rBlock.header.coinbaseAddress.fullShardId):
            txList.append(CrossShardTransactionDeposit(
                txHash=bytes(32),
                fromAddress=Address.create_empty_account(0),
                toAddress=rBlock.header.coinbaseAddress,
                value=rBlock.header.coinbaseAmount,
                gasPrice=0))
        return txList

    def __run_one_cross_shard_tx_list_by_root_block_hash(self, rHash, evmState):
        txList = self.__get_cross_shard_tx_list_by_root_block_hash(rHash)

        for tx in txList:
            evmState.delta_balance(tx.toAddress.recipient, tx.value)
            evmState.gas_used = min(
                evmState.gas_used + (opcodes.GTXXSHARDCOST if tx.gasPrice != 0 else 0),
                evmState.gas_limit)
            evmState.block_fee += opcodes.GTXXSHARDCOST * tx.gasPrice
            evmState.delta_balance(evmState.block_coinbase, opcodes.GTXXSHARDCOST * tx.gasPrice)
        evmState.xshard_receive_gas_used = evmState.gas_used

        return txList

    def __include_cross_shard_tx_list(self, evmState, descendantRootHeader, ancestorRootHeader):
        """ Include cross-shard transaction as much as possible by confirming root header as much as possible
        """
        if descendantRootHeader == ancestorRootHeader:
            return ancestorRootHeader

        # Find all unconfirmed root headers
        rHeader = descendantRootHeader
        headerList = []
        while rHeader != ancestorRootHeader:
            check(rHeader.height > ancestorRootHeader.height)
            headerList.append(rHeader)
            rHeader = self.db.get_root_block_header_by_hash(rHeader.hashPrevBlock)

        # Add root headers.  Return if we run out of gas.
        for rHeader in reversed(headerList):
            self.__run_one_cross_shard_tx_list_by_root_block_hash(rHeader.get_hash(), evmState)
            if evmState.gas_used == evmState.gas_limit:
                return rHeader

        return descendantRootHeader

    def __run_cross_shard_tx_list(self, evmState, descendantRootHeader, ancestorRootHeader):
        txList = []
        rHeader = descendantRootHeader
        while rHeader != ancestorRootHeader:
            if rHeader.height == ancestorRootHeader.height:
                raise ValueError(
                    "incorrect ancestor root header: expected {}, actual {}",
                    rHeader.get_hash().hex(),
                    ancestorRootHeader.get_hash().hex())
            if evmState.gas_used > evmState.gas_limit:
                raise ValueError("gas consumed by cross-shard tx exceeding limit")

            oneTxList = self.__run_one_cross_shard_tx_list_by_root_block_hash(rHeader.get_hash(), evmState)
            txList.extend(oneTxList)

            # Move to next root block header
            rHeader = self.db.get_root_block_header_by_hash(rHeader.hashPrevBlock)

        check(evmState.gas_used <= evmState.gas_limit)
        # TODO: Refill local x-shard gas
        return txList

    def contain_remote_minor_block_hash(self, h):
        return self.db.contain_remote_minor_block_hash(h)

    def get_transaction_by_hash(self, h):
        """ Returns (block, index) where index is the position of tx in the block """
        block, index = self.db.get_transaction_by_hash(h)
        if block:
            return block, index
        if h in self.txDict:
            block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            block.txList.append(self.txDict[h])
            return block, 0
        return None, None

    def get_transaction_receipt(self, h) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        block, index = self.db.get_transaction_by_hash(h)
        if not block:
            return None
        receipt = block.get_receipt(self.evmState.db, index)
        if receipt.contractAddress != Address.create_empty_account(0):
            address = receipt.contractAddress
            check(address.fullShardId == self.evmState.get_full_shard_id(address.recipient))
        return block, index, receipt

    def get_transaction_list_by_address(self, address, start, limit):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        if start == bytes(1):  # get pending tx
            txList = []
            for orderableTx in self.txQueue.txs + self.txQueue.aside:
                tx = orderableTx.tx
                if Address(tx.sender, tx.fromFullShardId) == address:
                    txList.append(TransactionDetail(
                        Transaction(code=Code.create_evm_code(tx)).get_hash(),
                        address,
                        Address(tx.to, tx.toFullShardId) if tx.to else None,
                        tx.value,
                        blockHeight=0,
                        timestamp=0,
                        success=False,
                    ))
            return txList, b""

        return self.db.get_transactions_by_address(address, start, limit)

    def get_shard_stats(self) -> ShardStats:
        cutoff = self.headerTip.createTime - 60
        block = self.db.get_minor_block_by_hash(self.headerTip.get_hash())
        txCount = 0
        blockCount = 0
        staleBlockCount = 0
        lastBlockTime = 0
        while block.header.height > 0 and block.header.createTime > cutoff:
            txCount += len(block.txList)
            blockCount += 1
            staleBlockCount += max(0, (self.db.get_block_count_by_height(block.header.height) - 1))
            block = self.db.get_minor_block_by_hash(block.header.hashPrevMinorBlock)
            if lastBlockTime == 0:
                lastBlockTime = self.headerTip.createTime - block.header.createTime

        check(staleBlockCount >= 0)
        return ShardStats(
            branch=self.branch,
            height=self.headerTip.height,
            timestamp=self.headerTip.createTime,
            txCount60s=txCount,
            pendingTxCount=len(self.txQueue),
            totalTxCount=self.db.get_total_tx_count(self.headerTip.get_hash()),
            blockCount60s=blockCount,
            staleBlockCount60s=staleBlockCount,
            lastBlockTime=lastBlockTime,
        )
