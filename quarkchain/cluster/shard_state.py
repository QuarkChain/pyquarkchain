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

    def __encode_address_transaction_key(self, address, height, index, cross_shard):
        cross_shard_byte = b"\x00" if cross_shard else b"\x01"
        return b"addr_" + address.serialize() + height.to_bytes(4, "big") + cross_shard_byte + index.to_bytes(4, "big")

    def put_confirmed_cross_shard_transaction_deposit_list(self, minor_block_hash, cross_shard_transaction_deposit_list):
        """Stores a mapping from minor block to the list of CrossShardTransactionDeposit confirmed"""
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return

        l = CrossShardTransactionList(cross_shard_transaction_deposit_list)
        self.db.put(b"xr_" + minor_block_hash, l.serialize())

    def __get_confirmed_cross_shard_transaction_deposit_list(self, minor_block_hash):
        data = self.db.get(b"xr_" + minor_block_hash, None)
        if not data:
            return []
        return CrossShardTransactionList.deserialize(data).txList

    def __update_transaction_history_index(self, tx, block_height, index, func):
        evm_tx = tx.code.get_evm_transaction()
        addr = Address(evm_tx.sender, evm_tx.fromFullShardId)
        key = self.__encode_address_transaction_key(addr, block_height, index, False)
        func(key, b"")
        # "to" can be empty for smart contract deployment
        if evm_tx.to and self.branch.is_in_shard(evm_tx.toFullShardId):
            addr = Address(evm_tx.to, evm_tx.toFullShardId)
            key = self.__encode_address_transaction_key(addr, block_height, index, False)
            func(key, b"")

    def put_transaction_history_index(self, tx, block_height, index):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index(tx, block_height, index, lambda k, v: self.db.put(k, v))

    def remove_transaction_history_index(self, tx, block_height, index):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index(tx, block_height, index, lambda k, v: self.db.remove(k))

    def __update_transaction_history_index_from_block(self, minor_block, func):
        x_shard_receive_tx_list = self.__get_confirmed_cross_shard_transaction_deposit_list(minor_block.header.get_hash())
        for i, tx in enumerate(x_shard_receive_tx_list):
            if tx.txHash == bytes(32):  # coinbase reward for root block miner
                continue
            key = self.__encode_address_transaction_key(tx.toAddress, minor_block.header.height, i, True)
            func(key, b"")

    def put_transaction_history_index_from_block(self, minor_block):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index_from_block(minor_block, lambda k, v: self.db.put(k, v))

    def remove_transaction_history_index_from_block(self, minor_block):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index_from_block(minor_block, lambda k, v: self.db.remove(k))

    def get_transactions_by_address(self, address, start=b"", limit=10):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        serialized_address = address.serialize()
        end = b"addr_" + serialized_address
        original_start = (int.from_bytes(end, byteorder="big") + 1).to_bytes(len(end), byteorder="big")
        next = end
        # reset start to the latest if start is not valid
        if not start or start > original_start:
            start = original_start

        tx_list = []
        for k, v in self.db.reversed_range_iter(start, end):
            limit -= 1
            if limit < 0:
                break
            height = int.from_bytes(k[5 + 24:5 + 24 + 4], "big")
            cross_shard = int(k[5 + 24 + 4]) == 0
            index = int.from_bytes(k[5 + 24 + 4 + 1:], "big")
            if cross_shard:  # cross shard receive
                m_block = self.get_minor_block_by_height(height)
                x_shard_receive_tx_list = self.__get_confirmed_cross_shard_transaction_deposit_list(m_block.header.get_hash())
                tx = x_shard_receive_tx_list[index]  # tx is CrossShardTransactionDeposit
                tx_list.append(
                    TransactionDetail(
                        tx.txHash,
                        tx.fromAddress,
                        tx.toAddress,
                        tx.value,
                        height,
                        m_block.header.createTime,
                        True,
                    )
                )
            else:
                m_block = self.get_minor_block_by_height(height)
                receipt = m_block.get_receipt(self.db, index)
                tx = m_block.txList[index]  # tx is Transaction
                evm_tx = tx.code.get_evm_transaction()
                tx_list.append(
                    TransactionDetail(
                        tx.get_hash(),
                        Address(evm_tx.sender, evm_tx.fromFullShardId),
                        Address(evm_tx.to, evm_tx.toFullShardId) if evm_tx.to else None,
                        evm_tx.value,
                        height,
                        m_block.header.createTime,
                        receipt.success == b"\x01",
                    )
                )
            next = (int.from_bytes(k, byteorder="big") - 1).to_bytes(len(k), byteorder="big")

        return tx_list, next


class ShardDbOperator(TransactionHistoryMixin):
    def __init__(self, db, env, branch):
        self.env = env
        self.db = db
        self.branch = branch
        # TODO:  iterate db to recover pools and set
        self.m_header_pool = dict()
        self.m_meta_pool = dict()
        self.x_shard_set = set()
        self.r_header_pool = dict()
        self.r_minor_header_pool = dict()

        # height -> set(minor block hash) for counting wasted blocks
        self.heightToMinorBlockHashes = dict()

    def __get_last_minor_block_in_root_block(self, root_block):
        l_header = None
        for m_header in root_block.minorBlockHeaderList:
            if m_header.branch != self.branch:
                continue

            if l_header is None or m_header.height > l_header.height:
                l_header = m_header

        check(l_header is not None)
        return l_header

    def recover_state(self, r_header, m_header):
        """ When recovering from local database, we can only guarantee the consistency of the best chain.
        Forking blocks can be in inconsistent state and thus should be pruned from the database
        so that they can be retried in the future.
        """
        r_hash = r_header.get_hash()
        while len(self.r_header_pool) < self.env.config.MAX_ROOT_BLOCK_IN_MEMORY:
            block = RootBlock.deserialize(self.db.get(b"rblock_" + r_hash))
            self.r_minor_header_pool[r_hash] = self.__get_last_minor_block_in_root_block(block)
            self.r_header_pool[r_hash] = block.header
            if block.header.height <= 0:
                break
            r_hash = block.header.hashPrevBlock

        m_hash = m_header.get_hash()
        while len(self.m_header_pool) < self.env.config.MAX_MINOR_BLOCK_IN_MEMORY:
            block = MinorBlock.deserialize(self.db.get(b"mblock_" + m_hash))
            self.m_header_pool[m_hash] = block.header
            self.m_meta_pool[m_hash] = block.meta
            if block.header.height <= 0:
                break
            m_hash = block.header.hashPrevMinorBlock

        Logger.info("[{}] recovered {} minor blocks and {} root blocks".format(
            self.branch.get_shard_id(), len(self.m_header_pool), len(self.r_header_pool)))

    # ------------------------- Root block db operations --------------------------------
    def put_root_block(self, root_block, r_minor_header, root_block_hash=None):
        """ rMinorHeader: the minor header of the shard in the root block with largest height
        """
        if root_block_hash is None:
            root_block_hash = root_block.header.get_hash()

        self.db.put(b"rblock_" + root_block_hash, root_block.serialize())
        self.r_header_pool[root_block_hash] = root_block.header
        self.r_minor_header_pool[root_block_hash] = r_minor_header

    def get_root_block_by_hash(self, h):
        if h not in self.r_header_pool:
            return None
        return RootBlock.deserialize(self.db.get(b"rblock_" + h))

    def get_root_block_header_by_hash(self, h):
        return self.r_header_pool.get(h, None)

    def contain_root_block_by_hash(self, h):
        return h in self.r_header_pool

    def get_last_minor_block_in_root_block(self, h):
        if h not in self.r_header_pool:
            return None
        return self.r_minor_header_pool.get(h)

    # ------------------------- Minor block db operations --------------------------------
    def put_minor_block(self, m_block, x_shard_receive_tx_list):
        m_block_hash = m_block.header.get_hash()

        self.db.put(b"mblock_" + m_block_hash, m_block.serialize())
        self.put_total_tx_count(m_block)

        self.m_header_pool[m_block_hash] = m_block.header
        self.m_meta_pool[m_block_hash] = m_block.meta

        self.heightToMinorBlockHashes.setdefault(m_block.header.height, set()).add(m_block.header.get_hash())

        self.put_confirmed_cross_shard_transaction_deposit_list(m_block_hash, x_shard_receive_tx_list)

    def put_total_tx_count(self, m_block):
        prev_count = 0
        if m_block.header.height > 2:
            prev_count = self.get_total_tx_count(m_block.header.hashPrevMinorBlock)
        count = prev_count + len(m_block.txList)
        self.db.put(b"txCount_" + m_block.header.get_hash(), count.to_bytes(4, "big"))

    def get_total_tx_count(self, m_block_hash):
        count_bytes = self.db.get(b"txCount_" + m_block_hash, None)
        if not count_bytes:
            return 0
        return int.from_bytes(count_bytes, "big")

    def get_minor_block_header_by_hash(self, h, consistency_check=True):
        header = self.m_header_pool.get(h, None)
        if not header and not consistency_check:
            block = self.get_minor_block_by_hash(h, False)
            header = block.header
        return header

    def get_minor_block_evm_root_hash_by_hash(self, h):
        if h not in self.m_header_pool:
            return None
        check(h in self.m_meta_pool)
        meta = self.m_meta_pool[h]
        return meta.hashEvmStateRoot

    def get_minor_block_meta_by_hash(self, h):
        return self.m_meta_pool.get(h, None)

    def get_minor_block_by_hash(self, h, consistency_check=True):
        if consistency_check and h not in self.m_header_pool:
            return None
        return MinorBlock.deserialize(self.db.get(b"mblock_" + h))

    def contain_minor_block_by_hash(self, h):
        return h in self.m_header_pool

    def put_minor_block_index(self, block):
        self.db.put(b"mi_%d" % block.header.height, block.header.get_hash())

    def remove_minor_block_index(self, block):
        self.db.remove(b"mi_%d" % block.header.height)

    def get_minor_block_by_height(self, height):
        key = b"mi_%d" % height
        if key not in self.db:
            return None
        block_hash = self.db.get(key)
        return self.get_minor_block_by_hash(block_hash, False)

    def get_block_count_by_height(self, height):
        """ Return the total number of blocks with the given height"""
        return len(self.heightToMinorBlockHashes.setdefault(height, set()))

    # ------------------------- Transaction db operations --------------------------------
    def put_transaction_index(self, tx, block_height, index):
        tx_hash = tx.get_hash()
        self.db.put(b"txindex_" + tx_hash,
                    block_height.to_bytes(4, "big") + index.to_bytes(4, "big"))

        self.put_transaction_history_index(tx, block_height, index)

    def remove_transaction_index(self, tx, block_height, index):
        tx_hash = tx.get_hash()
        self.db.remove(b"txindex_" + tx_hash)

        self.remove_transaction_history_index(tx, block_height, index)

    def contain_transaction_hash(self, tx_hash):
        key = b"txindex_" + tx_hash
        return key in self.db

    def get_transaction_by_hash(self, tx_hash) -> Tuple[Optional[MinorBlock], Optional[int]]:
        result = self.db.get(b"txindex_" + tx_hash, None)
        if not result:
            return None, None
        check(len(result) == 8)
        block_height = int.from_bytes(result[:4], "big")
        index = int.from_bytes(result[4:], "big")
        return self.get_minor_block_by_height(block_height), index

    def put_transaction_index_from_block(self, minor_block):
        for i, tx in enumerate(minor_block.txList):
            self.put_transaction_index(tx, minor_block.header.height, i)

        self.put_transaction_history_index_from_block(minor_block)

    def remove_transaction_index_from_block(self, minor_block):
        for i, tx in enumerate(minor_block.txList):
            self.remove_transaction_index(tx, minor_block.header.height, i)

        self.remove_transaction_history_index_from_block(minor_block)

    # -------------------------- Cross-shard tx operations ----------------------------
    def put_minor_block_xshard_tx_list(self, h, tx_list: CrossShardTransactionList):
        # self.xShardSet.add(h)
        self.db.put(b"xShard_" + h, tx_list.serialize())

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

    def __init__(self, env, shard_id, db=None):
        self.env = env
        self.shard_id = shard_id
        self.diff_calc = self.env.config.MINOR_DIFF_CALCULATOR
        self.diff_hash_func = self.env.config.DIFF_HASH_FUNC
        self.reward_calc = ConstMinorBlockRewardCalcultor(env)
        self.raw_db = db if db is not None else env.db
        self.branch = Branch.create(env.config.SHARD_SIZE, shard_id)
        self.db = ShardDbOperator(self.raw_db, self.env, self.branch)
        self.tx_queue = TransactionQueue()  # queue of EvmTransaction
        self.tx_dict = dict()  # hash -> Transaction for explorer
        self.initialized = False

        # assure ShardState is in good shape after constructor returns though we still
        # rely on master calling init_from_root_block to bring the cluster into consistency
        self.__create_genesis_blocks(shard_id)

    def init_from_root_block(self, root_block):
        """ Master will send its root chain tip when it connects to slaves.
        Shards will initialize its state based on the root block.
        """
        def __get_header_tip_from_root_block(branch):
            header_tip = None
            for m_header in root_block.minorBlockHeaderList:
                if m_header.branch == branch:
                    check(header_tip is None or header_tip.height + 1 == m_header.height)
                    header_tip = m_header
            check(header_tip is not None)
            return header_tip

        check(not self.initialized)
        self.initialized = True

        Logger.info("Initializing shard state from root height {} hash {}".format(
            root_block.header.height, root_block.header.get_hash().hex()))

        if root_block.header.height <= 1:
            Logger.info("Created genesis block")
            return

        shard_size = root_block.header.shardInfo.get_shard_size()
        check(self.branch == Branch.create(shard_size, self.shard_id))
        self.root_tip = root_block.header
        self.header_tip = __get_header_tip_from_root_block(self.branch)

        self.db.recover_state(self.root_tip, self.header_tip)
        Logger.info("[{}] done recovery from db. shard tip {} {} root tip {} {}".format(
            self.branch.get_shard_id(), self.header_tip.height, self.header_tip.get_hash().hex(),
            self.root_tip.height, self.root_tip.get_hash().hex()))

        self.meta_tip = self.db.get_minor_block_meta_by_hash(self.header_tip.get_hash())
        self.confirmed_header_tip = self.header_tip
        self.confirmed_meta_tip = self.meta_tip
        self.evm_state = self.__create_evm_state()
        self.evm_state.trie.root_hash = self.meta_tip.hashEvmStateRoot
        check(self.db.get_minor_block_evm_root_hash_by_hash(self.header_tip.get_hash()) == self.meta_tip.hashEvmStateRoot)

        self.__rewrite_block_index_to(self.db.get_minor_block_by_hash(self.header_tip.get_hash()), add_tx_back_to_queue=False)

    def __create_evm_state(self):
        return EvmState(env=self.env.evmEnv, db=self.raw_db)

    def __create_genesis_blocks(self, shardId):
        evm_list = create_genesis_evm_list(env=self.env, dbMap={self.shard_id: self.raw_db})
        genesis_root_block0, genesis_root_block1, g_minor_block_list0, g_minor_block_list1 = create_genesis_blocks(
            env=self.env, evmList=evm_list)

        # Add x-shard list to db
        for m_block1, evm_state in zip(g_minor_block_list1, evm_list):
            if m_block1.header.branch.get_shard_id() == shardId:
                continue
            self.add_cross_shard_tx_list_by_minor_block_hash(
                m_block1.header.get_hash(), CrossShardTransactionList(txList=[]))

        # Local helper variables
        genesis_minor_block0 = g_minor_block_list0[self.shard_id]
        genesis_minor_block1 = g_minor_block_list1[self.shard_id]
        check(genesis_minor_block1.header.branch.get_shard_id() == self.shard_id)

        check(genesis_minor_block0.header.branch == self.branch)
        self.evm_state = evm_list[self.shard_id]
        self.db.put_minor_block(genesis_minor_block0, [])
        self.db.put_minor_block_index(genesis_minor_block0)
        self.db.put_minor_block(genesis_minor_block1, [])
        self.db.put_minor_block_index(genesis_minor_block1)
        self.db.put_root_block(genesis_root_block0, genesis_minor_block0.header)
        self.db.put_root_block(genesis_root_block1, genesis_minor_block1.header)

        self.root_tip = genesis_root_block1.header
        # Tips that are confirmed by root
        self.confirmed_header_tip = genesis_minor_block1.header
        self.confirmed_meta_tip = genesis_minor_block1.header
        # Tips that are unconfirmed by root
        self.header_tip = genesis_minor_block1.header
        self.meta_tip = genesis_minor_block1.meta

    def __validate_tx(self, tx: Transaction, evm_state, from_address=None) -> EvmTransaction:
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

        evm_tx = tx.code.get_evm_transaction()

        if from_address:
            check(evm_tx.fromFullShardId == from_address.fullShardId)
            nonce = evm_state.get_nonce(from_address.recipient)
            # have to create a new evmTx as nonce is immutable
            evm_tx = EvmTransaction(
                nonce, evm_tx.gasprice, evm_tx.startgas, evm_tx.to, evm_tx.value, evm_tx.data,
                fromFullShardId=evm_tx.fromFullShardId, toFullShardId=evm_tx.toFullShardId, networkId=evm_tx.networkId)
            evm_tx.sender = from_address.recipient

        evm_tx.set_shard_size(self.branch.get_shard_size())

        if evm_tx.networkId != self.env.config.NETWORK_ID:
            raise RuntimeError("evm tx network id mismatch. expect {} but got {}".format(
                self.env.config.NETWORK_ID, evm_tx.networkId))

        if evm_tx.from_shard_id() != self.branch.get_shard_id():
            raise RuntimeError("evm tx from_shard_id mismatch. expect {} but got {}".format(
                self.branch.get_shard_id(), evm_tx.from_shard_id(),
            ))

        # This will check signature, nonce, balance, gas limit
        validate_transaction(evm_state, evm_tx)

        # TODO: Neighborhood and xshard gas limit check
        return evm_tx

    def add_tx(self, tx: Transaction):
        if len(self.tx_queue) > self.env.config.TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD:
            # exceeding tx queue size limit
            return False

        if self.db.contain_transaction_hash(tx.get_hash()):
            return False

        tx_hash = tx.get_hash()
        if tx_hash in self.tx_dict:
            return False

        evm_state = self.evm_state.ephemeral_clone()
        evm_state.gas_used = 0
        try:
            evm_tx = self.__validate_tx(tx, evm_state)
            self.tx_queue.add_transaction(evm_tx)
            self.tx_dict[tx_hash] = tx
            return True
        except Exception as e:
            Logger.warningEverySec("Failed to add transaction: {}".format(e), 1)
            return False

    def execute_tx(self, tx: Transaction, fromAddress) -> Optional[bytes]:
        state = self.evm_state.ephemeral_clone()
        state.gas_used = 0
        try:
            evm_tx = self.__validate_tx(tx, state, fromAddress)
            success, output = apply_transaction(state, evm_tx, tx_wrapper_hash=bytes(32))
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

    def __is_same_minor_chain(self, longer_block_header, shorter_block_header):
        if shorter_block_header.height > longer_block_header.height:
            return False

        header = longer_block_header
        for i in range(longer_block_header.height - shorter_block_header.height):
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)
        return header == shorter_block_header

    def __is_same_root_chain(self, longer_block_header, shorter_block_header):
        if shorter_block_header.height > longer_block_header.height:
            return False

        header = longer_block_header
        for i in range(longer_block_header.height - shorter_block_header.height):
            header = self.db.get_root_block_header_by_hash(header.hashPrevBlock)
        return header == shorter_block_header

    def __validate_block(self, block):
        """ Validate a block before running evm transactions
        """
        if block.header.height <= 1:
            raise ValueError("unexpected height")

        if not self.db.contain_minor_block_by_hash(block.header.hashPrevMinorBlock):
            # TODO:  May put the block back to queue
            raise ValueError("[{}] prev block not found, block height {} prev hash {}".format(self.branch.get_shard_id(), block.header.height, block.header.hashPrevMinorBlock.hex()))
        prev_header = self.db.get_minor_block_header_by_hash(block.header.hashPrevMinorBlock)

        if block.header.height != prev_header.height + 1:
            raise ValueError("height mismatch")

        if block.header.branch != self.branch:
            raise ValueError("branch mismatch")

        if block.header.createTime <= prev_header.createTime:
            raise ValueError("incorrect create time tip time {}, new block time {}".format(
                block.header.createTime, self.chain[-1].createTime))

        if block.header.hashMeta != block.meta.get_hash():
            raise ValueError("Hash of meta mismatch")

        if len(block.meta.extraData) > self.env.config.BLOCK_EXTRA_DATA_SIZE_LIMIT:
            raise ValueError("extraData in block is too large")

        # Make sure merkle tree is valid
        merkle_hash = calculate_merkle_root(block.txList)
        if merkle_hash != block.meta.hashMerkleRoot:
            raise ValueError("incorrect merkle root")

        # Check the first transaction of the block
        if not self.branch.is_in_shard(block.meta.coinbaseAddress.fullShardId):
            raise ValueError("coinbase output address must be in the shard")

        # Check difficulty
        if not self.env.config.SKIP_MINOR_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == NetworkId.MAINNET:
                diff = self.diff_calc.calculate_diff_with_parent(prev_header, block.header.createTime)
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
        root_block_header = self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock)
        if root_block_header is None:
            raise ValueError("cannot find root block for the minor block")

        if root_block_header.height < self.db.get_root_block_header_by_hash(prev_header.hashPrevRootBlock).height:
            raise ValueError("prev root block height must be non-decreasing")

        prev_confirmed_minor_block = self.db.get_last_minor_block_in_root_block(block.header.hashPrevRootBlock)
        if not self.__is_same_minor_chain(prev_header, prev_confirmed_minor_block):
            raise ValueError("prev root block's minor block is not in the same chain as the minor block")

        if not self.__is_same_root_chain(self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock),
                                      self.db.get_root_block_header_by_hash(prev_header.hashPrevRootBlock)):
            raise ValueError("prev root blocks are not on the same chain")

    def run_block(self, block, evm_state=None, evm_tx_included=None, x_shard_receive_tx_list=None):
        if evm_tx_included is None:
            evm_tx_included = []
        if x_shard_receive_tx_list is None:
            x_shard_receive_tx_list = []
        if evm_state is None:
            evm_state = self.__get_evm_state_for_new_block(block, ephemeral=False)
        root_block_header = self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock)
        prev_header = self.db.get_minor_block_header_by_hash(block.header.hashPrevMinorBlock)

        x_shard_receive_tx_list.extend(self.__run_cross_shard_tx_list(
            evm_state=evm_state,
            descendant_root_header=root_block_header,
            ancestor_root_header=self.db.get_root_block_header_by_hash(prev_header.hashPrevRootBlock)))

        for idx, tx in enumerate(block.txList):
            try:
                evm_tx = self.__validate_tx(tx, evm_state)
                evm_tx.set_shard_size(self.branch.get_shard_size())
                apply_transaction(evm_state, evm_tx, tx.get_hash())
                evm_tx_included.append(evm_tx)
            except Exception as e:
                Logger.debugException()
                Logger.debug("failed to process Tx {}, idx {}, reason {}".format(
                    tx.get_hash().hex(), idx, e))
                raise e

        # Put only half of block fee to coinbase address
        check(evm_state.get_balance(evm_state.block_coinbase) >= evm_state.block_fee)
        evm_state.delta_balance(evm_state.block_coinbase, -evm_state.block_fee // 2)

        # Update actual root hash
        evm_state.commit()
        return evm_state

    def __is_minor_block_linked_to_root_tip(self, m_block):
        """ Determine whether a minor block is a descendant of a minor block confirmed by root tip
        """
        if m_block.header.height <= self.confirmed_header_tip.height:
            return False

        header = m_block.header
        for i in range(m_block.header.height - self.confirmed_header_tip.height):
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)

        return header == self.confirmed_header_tip

    def __rewrite_block_index_to(self, minor_block, add_tx_back_to_queue=True):
        """ Find the common ancestor in the current chain and rewrite index till minorblock """
        new_chain = []
        old_chain = []

        # minorBlock height could be lower than the current tip
        # we should revert all the blocks above minorBlock height
        height = minor_block.header.height + 1
        while True:
            orig_block = self.db.get_minor_block_by_height(height)
            if not orig_block:
                break
            old_chain.append(orig_block)
            height += 1

        block = minor_block
        # Find common ancestor and record the blocks that needs to be updated
        while block.header.height >= 0:
            orig_block = self.db.get_minor_block_by_height(block.header.height)
            if orig_block and orig_block.header == block.header:
                break
            new_chain.append(block)
            if orig_block:
                old_chain.append(orig_block)
            block = self.db.get_minor_block_by_hash(block.header.hashPrevMinorBlock)

        for block in old_chain:
            self.db.remove_transaction_index_from_block(block)
            self.db.remove_minor_block_index(block)
            if add_tx_back_to_queue:
                self.__add_transactions_from_block(block)
        for block in new_chain:
            self.db.put_transaction_index_from_block(block)
            self.db.put_minor_block_index(block)
            self.__remove_transactions_from_block(block)

    def __add_transactions_from_block(self, block):
        for tx in block.txList:
            self.tx_dict[tx.get_hash()] = tx
            self.tx_queue.add_transaction(tx.code.get_evm_transaction())

    def __remove_transactions_from_block(self, block):
        evm_tx_list = []
        for tx in block.txList:
            self.tx_dict.pop(tx.get_hash(), None)
            evm_tx_list.append(tx.code.get_evm_transaction())
        self.tx_queue = self.tx_queue.diff(evm_tx_list)

    def add_block(self, block):
        """  Add a block to local db.  Perform validate and update tip accordingly
        Returns None if block is already added.
        Returns a list of CrossShardTransactionDeposit from block.
        Raises on any error.
        """
        start_time = time.time()
        if self.header_tip.height - block.header.height > 700:
            Logger.info("[{}] drop old block {} << {}".format(block.header.height, self.header_tip.height))
            return None
        if self.db.contain_minor_block_by_hash(block.header.get_hash()):
            return None

        evm_tx_included = []
        x_shard_receive_tx_list = []
        # Throw exception if fail to run
        self.__validate_block(block)
        evm_state = self.run_block(block, evm_tx_included=evm_tx_included, x_shard_receive_tx_list=x_shard_receive_tx_list)

        # ------------------------ Validate ending result of the block --------------------
        if block.meta.hashEvmStateRoot != evm_state.trie.root_hash:
            raise ValueError("State root mismatch: header %s computed %s" %
                             (block.meta.hashEvmStateRoot.hex(), evm_state.trie.root_hash.hex()))

        receipt_root = mk_receipt_sha(evm_state.receipts, evm_state.db)
        if block.meta.hashEvmReceiptRoot != receipt_root:
            raise ValueError("Receipt root mismatch: header {} computed {}".format(
                block.meta.hashEvmReceiptRoot.hex(), receipt_root.hex()
            ))

        if evm_state.gas_used != block.meta.evmGasUsed:
            raise ValueError("Gas used mismatch: header %d computed %d" %
                             (block.meta.evmGasUsed, evm_state.gas_used))

        if evm_state.xshard_receive_gas_used != block.meta.evmCrossShardReceiveGasUsed:
            raise ValueError("X-shard gas used mismatch: header %d computed %d" %
                             (block.meta.evmCrossShardReceiveGasUsed, evm_state.xshard_receive_gas_used))

        # The rest fee goes to root block
        if evm_state.block_fee // 2 != block.header.coinbaseAmount:
            raise ValueError("Coinbase reward incorrect")
        # TODO: Check evm bloom

        # TODO: Add block reward to coinbase
        # self.rewardCalc.get_block_reward(self):
        self.db.put_minor_block(block, x_shard_receive_tx_list)

        # Update tip if a block is appended or a fork is longer (with the same ancestor confirmed by root block tip)
        # or they are equal length but the root height confirmed by the block is longer
        update_tip = False
        if not self.__is_same_root_chain(self.root_tip, self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock)):
            # Don't update tip if the block depends on a root block that is not rootTip or rootTip's ancestor
            update_tip = False
        elif block.header.hashPrevMinorBlock == self.header_tip.get_hash():
            update_tip = True
        elif self.__is_minor_block_linked_to_root_tip(block):
            if block.header.height > self.header_tip.height:
                update_tip = True
            elif block.header.height == self.header_tip.height:
                update_tip = self.db.get_root_block_header_by_hash(block.header.hashPrevRootBlock).height > \
                            self.db.get_root_block_header_by_hash(self.header_tip.hashPrevRootBlock).height

        if update_tip:
            self.__rewrite_block_index_to(block)
            self.evm_state = evm_state
            self.header_tip = block.header
            self.meta_tip = block.meta

        check(self.__is_same_root_chain(self.root_tip, self.db.get_root_block_header_by_hash(self.header_tip.hashPrevRootBlock)))
        end_time = time.time()
        Logger.debug("Add block took {} seconds for {} tx".format(
            end_time - start_time, len(block.txList)
        ))
        return evm_state.xshard_list

    def get_tip(self):
        return self.db.get_minor_block_by_hash(self.header_tip.get_hash())

    def tip(self):
        """ Called in diff.py """
        return self.header_tip

    def finalize_and_add_block(self, block):
        block.finalize(evmState=self.run_block(block))
        self.add_block(block)

    def get_block_header_by_height(self, height):
        pass

    def get_balance(self, recipient):
        return self.evm_state.get_balance(recipient)

    def get_transaction_count(self, recipient):
        return self.evm_state.get_nonce(recipient)

    def get_code(self, recipient):
        return self.evm_state.get_code(recipient)

    def get_next_block_difficulty(self, createTime=None):
        if not createTime:
            createTime = max(int(time.time()), self.header_tip.createTime + 1)
        return self.diff_calc.calculate_diff_with_parent(self.header_tip, createTime)

    def get_next_block_reward(self):
        return self.reward_calc.get_block_reward(self)

    def get_next_block_coinbase_amount(self):
        # TODO: add block reward
        # TODO: the current calculation is bogus and just serves as a placeholder.
        coinbase = 0
        for txWrapper in self.tx_queue.peek():
            tx = txWrapper.tx
            coinbase += tx.gasprice * tx.startgas

        if self.root_tip.get_hash() != self.header_tip.hashPrevRootBlock:
            txs = self.__get_cross_shard_tx_list_by_root_block_hash(self.root_tip.get_hash())
            for tx in txs:
                coinbase += tx.gasPrice * opcodes.GTXXSHARDCOST

        return coinbase

    def get_unconfirmed_headers_coinbase_amount(self):
        amount = 0
        header = self.header_tip
        for i in range(header.height - self.confirmed_header_tip.height):
            amount += header.coinbaseAmount
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)
        check(header == self.confirmed_header_tip)
        return amount

    def get_unconfirmed_header_list(self):
        """ height in ascending order """
        header_list = []
        header = self.header_tip
        for i in range(header.height - self.confirmed_header_tip.height):
            header_list.append(header)
            header = self.db.get_minor_block_header_by_hash(header.hashPrevMinorBlock)
        check(header == self.confirmed_header_tip)
        header_list.reverse()
        return header_list

    def __add_transactions_to_fund_loadtest_accounts(self, block, evm_state):
        height = block.header.height
        start_index = (height - 2) * 500
        shard_mask = self.branch.get_shard_size() - 1
        for i in range(500):
            index = start_index + i
            if index >= len(self.env.config.LOADTEST_ACCOUNTS):
                return
            address, key = self.env.config.LOADTEST_ACCOUNTS[index]
            from_full_shard_id = self.env.config.GENESIS_ACCOUNT.fullShardId & (~shard_mask) | self.branch.get_shard_id()
            to_full_shard_id = address.fullShardId & (~shard_mask) | self.branch.get_shard_id()
            gas = 21000
            evm_tx = EvmTransaction(
                nonce=evm_state.get_nonce(evm_state.block_coinbase),
                gasprice=3 * (10 ** 9),
                startgas=gas,
                to=address.recipient,
                value=10 * (10 ** 18),
                data=b'',
                fromFullShardId=from_full_shard_id,
                toFullShardId=to_full_shard_id,
                networkId=self.env.config.NETWORK_ID,
            )
            evm_tx.sign(key=self.env.config.GENESIS_KEY)
            evm_tx.set_shard_size(self.branch.get_shard_size())
            try:
                # tx_wrapper_hash is not needed for in-shard tx
                apply_transaction(evm_state, evm_tx, tx_wrapper_hash=bytes(32))
                block.add_tx(Transaction(code=Code.create_evm_code(evm_tx)))
            except Exception as e:
                Logger.errorException()
                return

    def create_block_to_mine(self, create_time=None, address=None, gas_limit=None):
        """ Create a block to append and include TXs to maximize rewards
        """
        start_time = time.time()
        if not create_time:
            create_time = max(int(time.time()), self.header_tip.createTime + 1)
        difficulty = self.get_next_block_difficulty(create_time)
        block = self.get_tip().create_block_to_append(
            createTime=create_time,
            address=address,
            difficulty=difficulty,
        )

        evm_state = self.__get_evm_state_for_new_block(block)

        if gas_limit is not None:
            # Set gasLimit.  Since gas limit is auto adjusted between blocks, this is for test purpose only.
            evm_state.gas_limit = gas_limit

        prev_header = self.header_tip
        ancestor_root_header = self.db.get_root_block_header_by_hash(prev_header.hashPrevRootBlock)
        check(self.__is_same_root_chain(self.root_tip, ancestor_root_header))

        # cross-shard receive must be handled before including tx from txQueue
        block.header.hashPrevRootBlock = self.__include_cross_shard_tx_list(
            evm_state=evm_state,
            descendant_root_header=self.root_tip,
            ancestor_root_header=ancestor_root_header).get_hash()

        # fund load test accounts
        self.__add_transactions_to_fund_loadtest_accounts(block, evm_state)

        poped_txs = []
        while evm_state.gas_used < evm_state.gas_limit:
            evm_tx = self.tx_queue.pop_transaction(
                max_gas=evm_state.gas_limit - evm_state.gas_used,
            )
            if evm_tx is None:
                break
            evm_tx.set_shard_size(self.branch.get_shard_size())
            try:
                tx = Transaction(code=Code.create_evm_code(evm_tx))
                apply_transaction(evm_state, evm_tx, tx.get_hash())
                block.add_tx(tx)
                poped_txs.append(evm_tx)
            except Exception as e:
                Logger.warningEverySec("Failed to include transaction: {}".format(e), 1)
                tx = Transaction(code=Code.create_evm_code(evm_tx))
                self.tx_dict.pop(tx.get_hash(), None)

        # We don't want to drop the transactions if the mined block failed to be appended
        for evm_tx in poped_txs:
            self.tx_queue.add_transaction(evm_tx)

        # Put only half of block fee to coinbase address
        check(evm_state.get_balance(evm_state.block_coinbase) >= evm_state.block_fee)
        evm_state.delta_balance(evm_state.block_coinbase, -evm_state.block_fee // 2)

        # Update actual root hash
        evm_state.commit()

        block.finalize(evmState=evm_state)

        end_time= time.time()
        Logger.debug("Create block to mine took {} seconds for {} tx".format(
            end_time - start_time, len(block.txList)
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

    def add_root_block(self, root_block):
        """ Add a root block.
        Make sure all cross shard tx lists of remote shards confirmed by the root block are in local db.
        """
        if not self.db.contain_root_block_by_hash(root_block.header.hashPrevBlock):
            raise ValueError("cannot find previous root block in pool")

        shard_header = None
        for m_header in root_block.minorBlockHeaderList:
            h = m_header.get_hash()
            if m_header.branch == self.branch:
                if not self.db.contain_minor_block_by_hash(h):
                    raise ValueError("cannot find minor block in local shard")
                if shard_header is None or shard_header.height < m_header.height:
                    shard_header = m_header
                continue

            if not self.__is_neighbor(m_header.branch):
                continue

            if not self.db.contain_remote_minor_block_hash(h):
                raise ValueError("cannot find xShard tx list for {}-{} {}".format(
                    m_header.branch.get_shard_id(), m_header.height, h.hex()))

        # shardHeader cannot be None since PROOF_OF_PROGRESS should be positive
        check(shard_header is not None)

        self.db.put_root_block(root_block, shard_header)
        check(self.__is_same_root_chain(root_block.header,
                                        self.db.get_root_block_header_by_hash(shard_header.hashPrevRootBlock)))

        if root_block.header.height > self.root_tip.height:
            # Switch to the longest root block
            self.root_tip = root_block.header
            self.confirmed_header_tip = shard_header
            self.confirmed_meta_tip = self.db.get_minor_block_meta_by_hash(shard_header.get_hash())

            orig_header_tip = self.header_tip
            orig_block = self.db.get_minor_block_by_height(shard_header.height)
            if not orig_block or orig_block.header != shard_header:
                self.__rewrite_block_index_to(self.db.get_minor_block_by_hash(shard_header.get_hash()))
                # TODO: shardHeader might not be the tip of the longest chain
                # need to switch to the tip of the longest chain
                self.header_tip = shard_header
                self.meta_tip = self.db.get_minor_block_meta_by_hash(self.header_tip.get_hash())
                Logger.info("[{}] (root confirms a fork) shard tip reset from {} to {} by root block {}".format(
                    self.branch.get_shard_id(), orig_header_tip.height, self.header_tip.height, root_block.header.height))
            else:
                # the current headerTip might point to a root block on a fork with rBlock
                # we need to scan back until finding a minor block pointing to the same root chain rBlock is on.
                # the worst case would be that we go all the way back to origBlock (shardHeader)
                while not self.__is_same_root_chain(
                        self.root_tip, self.db.get_root_block_header_by_hash(self.header_tip.hashPrevRootBlock)):
                    self.header_tip = self.db.get_minor_block_header_by_hash(self.header_tip.hashPrevMinorBlock)
                if self.header_tip != orig_header_tip:
                    Logger.info("[{}] shard tip reset from {} to {} by root block {}".format(
                        self.branch.get_shard_id(), orig_header_tip.height, self.header_tip.height, root_block.header.height))
            return True

        check(self.__is_same_root_chain(self.root_tip,
                                        self.db.get_root_block_header_by_hash(self.header_tip.hashPrevRootBlock)))
        return False

    def __is_neighbor(self, remoteBranch):
        # TODO: Apply routing rule to determine neighors that could directly send x-shard tx
        return True

    def __get_cross_shard_tx_list_by_root_block_hash(self, h):
        r_block = self.db.get_root_block_by_hash(h)
        tx_list = []
        for m_header in r_block.minorBlockHeaderList:
            if m_header.branch == self.branch:
                continue

            if not self.__is_neighbor(m_header.branch):
                continue

            h = m_header.get_hash()
            tx_list.extend(self.db.get_minor_block_xshard_tx_list(h).txList)

        # Apply root block coinbase
        if self.branch.is_in_shard(r_block.header.coinbaseAddress.fullShardId):
            tx_list.append(CrossShardTransactionDeposit(
                txHash=bytes(32),
                fromAddress=Address.create_empty_account(0),
                toAddress=r_block.header.coinbaseAddress,
                value=r_block.header.coinbaseAmount,
                gasPrice=0))
        return tx_list

    def __run_one_cross_shard_tx_list_by_root_block_hash(self, r_hash, evm_state):
        tx_list = self.__get_cross_shard_tx_list_by_root_block_hash(r_hash)

        for tx in tx_list:
            evm_state.delta_balance(tx.toAddress.recipient, tx.value)
            evm_state.gas_used = min(
                evm_state.gas_used + (opcodes.GTXXSHARDCOST if tx.gasPrice != 0 else 0),
                evm_state.gas_limit)
            evm_state.block_fee += opcodes.GTXXSHARDCOST * tx.gasPrice
            evm_state.delta_balance(evm_state.block_coinbase, opcodes.GTXXSHARDCOST * tx.gasPrice)
        evm_state.xshard_receive_gas_used = evm_state.gas_used

        return tx_list

    def __include_cross_shard_tx_list(self, evm_state, descendant_root_header, ancestor_root_header):
        """ Include cross-shard transaction as much as possible by confirming root header as much as possible
        """
        if descendant_root_header == ancestor_root_header:
            return ancestor_root_header

        # Find all unconfirmed root headers
        r_header = descendant_root_header
        header_list = []
        while r_header != ancestor_root_header:
            check(r_header.height > ancestor_root_header.height)
            header_list.append(r_header)
            r_header = self.db.get_root_block_header_by_hash(r_header.hashPrevBlock)

        # Add root headers.  Return if we run out of gas.
        for r_header in reversed(header_list):
            self.__run_one_cross_shard_tx_list_by_root_block_hash(r_header.get_hash(), evm_state)
            if evm_state.gas_used == evm_state.gas_limit:
                return r_header

        return descendant_root_header

    def __run_cross_shard_tx_list(self, evm_state, descendant_root_header, ancestor_root_header):
        tx_list = []
        r_header = descendant_root_header
        while r_header != ancestor_root_header:
            if r_header.height == ancestor_root_header.height:
                raise ValueError(
                    "incorrect ancestor root header: expected {}, actual {}",
                    r_header.get_hash().hex(),
                    ancestor_root_header.get_hash().hex())
            if evm_state.gas_used > evm_state.gas_limit:
                raise ValueError("gas consumed by cross-shard tx exceeding limit")

            one_tx_list = self.__run_one_cross_shard_tx_list_by_root_block_hash(r_header.get_hash(), evm_state)
            tx_list.extend(one_tx_list)

            # Move to next root block header
            r_header = self.db.get_root_block_header_by_hash(r_header.hashPrevBlock)

        check(evm_state.gas_used <= evm_state.gas_limit)
        # TODO: Refill local x-shard gas
        return tx_list

    def contain_remote_minor_block_hash(self, h):
        return self.db.contain_remote_minor_block_hash(h)

    def get_transaction_by_hash(self, h):
        """ Returns (block, index) where index is the position of tx in the block """
        block, index = self.db.get_transaction_by_hash(h)
        if block:
            return block, index
        if h in self.tx_dict:
            block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            block.txList.append(self.tx_dict[h])
            return block, 0
        return None, None

    def get_transaction_receipt(self, h) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        block, index = self.db.get_transaction_by_hash(h)
        if not block:
            return None
        receipt = block.get_receipt(self.evm_state.db, index)
        if receipt.contractAddress != Address.create_empty_account(0):
            address = receipt.contractAddress
            check(address.fullShardId == self.evm_state.get_full_shard_id(address.recipient))
        return block, index, receipt

    def get_transaction_list_by_address(self, address, start, limit):
        if not self.env.config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        if start == bytes(1):  # get pending tx
            tx_list = []
            for orderable_tx in self.tx_queue.txs + self.tx_queue.aside:
                tx = orderable_tx.tx
                if Address(tx.sender, tx.fromFullShardId) == address:
                    tx_list.append(TransactionDetail(
                        Transaction(code=Code.create_evm_code(tx)).get_hash(),
                        address,
                        Address(tx.to, tx.toFullShardId) if tx.to else None,
                        tx.value,
                        blockHeight=0,
                        timestamp=0,
                        success=False,
                    ))
            return tx_list, b""

        return self.db.get_transactions_by_address(address, start, limit)

    def get_shard_stats(self) -> ShardStats:
        cutoff = self.header_tip.createTime - 60
        block = self.db.get_minor_block_by_hash(self.header_tip.get_hash())
        tx_count = 0
        block_count = 0
        stale_block_count = 0
        last_block_time = 0
        while block.header.height > 0 and block.header.createTime > cutoff:
            tx_count += len(block.txList)
            block_count += 1
            stale_block_count += max(0, (self.db.get_block_count_by_height(block.header.height) - 1))
            block = self.db.get_minor_block_by_hash(block.header.hashPrevMinorBlock)
            if last_block_time == 0:
                last_block_time = self.header_tip.createTime - block.header.createTime

        check(stale_block_count >= 0)
        return ShardStats(
            branch=self.branch,
            height=self.header_tip.height,
            timestamp=self.header_tip.createTime,
            txCount60s=tx_count,
            pendingTxCount=len(self.tx_queue),
            totalTxCount=self.db.get_total_tx_count(self.header_tip.get_hash()),
            blockCount60s=block_count,
            staleBlockCount60s=stale_block_count,
            lastBlockTime=last_block_time,
        )
