from typing import Tuple, Optional

from quarkchain.cluster.rpc import TransactionDetail
from quarkchain.core import (
    RootBlock,
    MinorBlock,
    MinorBlockHeader,
    CrossShardTransactionList,
    Branch,
    Address,
)
from quarkchain.utils import check, Logger


class TransactionHistoryMixin:
    def __encode_address_transaction_key(self, address, height, index, cross_shard):
        cross_shard_byte = b"\x00" if cross_shard else b"\x01"
        return (
            b"addr_"
            + address.serialize()
            + height.to_bytes(4, "big")
            + cross_shard_byte
            + index.to_bytes(4, "big")
        )

    def put_confirmed_cross_shard_transaction_deposit_list(
        self, minor_block_hash, cross_shard_transaction_deposit_list
    ):
        """Stores a mapping from minor block to the list of CrossShardTransactionDeposit confirmed"""
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return

        l = CrossShardTransactionList(cross_shard_transaction_deposit_list)
        self.db.put(b"xr_" + minor_block_hash, l.serialize())

    def __get_confirmed_cross_shard_transaction_deposit_list(self, minor_block_hash):
        data = self.db.get(b"xr_" + minor_block_hash, None)
        if not data:
            return []
        return CrossShardTransactionList.deserialize(data).tx_list

    def __update_transaction_history_index(self, tx, block_height, index, func):
        evm_tx = tx.code.get_evm_transaction()
        addr = Address(evm_tx.sender, evm_tx.from_full_shard_id)
        key = self.__encode_address_transaction_key(addr, block_height, index, False)
        func(key, b"")
        # "to" can be empty for smart contract deployment
        if evm_tx.to and self.branch.is_in_shard(evm_tx.to_full_shard_id):
            addr = Address(evm_tx.to, evm_tx.to_full_shard_id)
            key = self.__encode_address_transaction_key(
                addr, block_height, index, False
            )
            func(key, b"")

    def put_transaction_history_index(self, tx, block_height, index):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index(
            tx, block_height, index, lambda k, v: self.db.put(k, v)
        )

    def remove_transaction_history_index(self, tx, block_height, index):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index(
            tx, block_height, index, lambda k, v: self.db.remove(k)
        )

    def __update_transaction_history_index_from_block(self, minor_block, func):
        x_shard_receive_tx_list = self.__get_confirmed_cross_shard_transaction_deposit_list(
            minor_block.header.get_hash()
        )
        for i, tx in enumerate(x_shard_receive_tx_list):
            if tx.tx_hash == bytes(32):  # coinbase reward for root block miner
                continue
            key = self.__encode_address_transaction_key(
                tx.to_address, minor_block.header.height, i, True
            )
            func(key, b"")

    def put_transaction_history_index_from_block(self, minor_block):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index_from_block(
            minor_block, lambda k, v: self.db.put(k, v)
        )

    def remove_transaction_history_index_from_block(self, minor_block):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return
        self.__update_transaction_history_index_from_block(
            minor_block, lambda k, v: self.db.remove(k)
        )

    def get_transactions_by_address(self, address, start=b"", limit=10):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        serialized_address = address.serialize()
        end = b"addr_" + serialized_address
        original_start = (int.from_bytes(end, byteorder="big") + 1).to_bytes(
            len(end), byteorder="big"
        )
        next = end
        # reset start to the latest if start is not valid
        if not start or start > original_start:
            start = original_start

        tx_list = []
        for k, v in self.db.reversed_range_iter(start, end):
            limit -= 1
            if limit < 0:
                break
            height = int.from_bytes(k[5 + 24 : 5 + 24 + 4], "big")
            cross_shard = int(k[5 + 24 + 4]) == 0
            index = int.from_bytes(k[5 + 24 + 4 + 1 :], "big")
            if cross_shard:  # cross shard receive
                m_block = self.get_minor_block_by_height(height)
                x_shard_receive_tx_list = self.__get_confirmed_cross_shard_transaction_deposit_list(
                    m_block.header.get_hash()
                )
                tx = x_shard_receive_tx_list[
                    index
                ]  # tx is CrossShardTransactionDeposit
                tx_list.append(
                    TransactionDetail(
                        tx.tx_hash,
                        tx.from_address,
                        tx.to_address,
                        tx.value,
                        height,
                        m_block.header.create_time,
                        True,
                    )
                )
            else:
                m_block = self.get_minor_block_by_height(height)
                receipt = m_block.get_receipt(self.db, index)
                tx = m_block.tx_list[index]  # tx is Transaction
                evm_tx = tx.code.get_evm_transaction()
                tx_list.append(
                    TransactionDetail(
                        tx.get_hash(),
                        Address(evm_tx.sender, evm_tx.from_full_shard_id),
                        Address(evm_tx.to, evm_tx.to_full_shard_id)
                        if evm_tx.to
                        else None,
                        evm_tx.value,
                        height,
                        m_block.header.create_time,
                        receipt.success == b"\x01",
                    )
                )
            next = (int.from_bytes(k, byteorder="big") - 1).to_bytes(
                len(k), byteorder="big"
            )

        return tx_list, next


class ShardDbOperator(TransactionHistoryMixin):
    def __init__(self, db, env, branch: Branch):
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
        self.height_to_minor_block_hashes = dict()

    def __get_last_minor_block_in_root_block(self, root_block):
        # genesis root block contains no minor block header
        if (
            root_block.header.height
            == self.env.quark_chain_config.get_genesis_root_height(
                self.branch.get_shard_id()
            )
        ):
            return None

        l_header = None
        for m_header in root_block.minor_block_header_list:
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
        while (
            len(self.r_header_pool)
            < self.env.quark_chain_config.ROOT.max_root_blocks_in_memory
        ):
            block = RootBlock.deserialize(self.db.get(b"rblock_" + r_hash))
            self.r_minor_header_pool[
                r_hash
            ] = self.__get_last_minor_block_in_root_block(block)
            self.r_header_pool[r_hash] = block.header
            if (
                block.header.height
                <= self.env.quark_chain_config.get_genesis_root_height(
                    self.branch.get_shard_id()
                )
            ):
                break
            r_hash = block.header.hash_prev_block

        m_hash = m_header.get_hash()
        shard_config = self.env.quark_chain_config.SHARD_LIST[
            self.branch.get_shard_id()
        ]
        while len(self.m_header_pool) < shard_config.max_minor_blocks_in_memory:
            block = MinorBlock.deserialize(self.db.get(b"mblock_" + m_hash))
            self.m_header_pool[m_hash] = block.header
            self.m_meta_pool[m_hash] = block.meta
            if block.header.height <= 0:
                break
            m_hash = block.header.hash_prev_minor_block

        Logger.info(
            "[{}] recovered {} minor blocks and {} root blocks".format(
                self.branch.get_shard_id(),
                len(self.m_header_pool),
                len(self.r_header_pool),
            )
        )

    # ------------------------- Root block db operations --------------------------------
    def put_root_block(self, root_block, r_minor_header=None, root_block_hash=None):
        """ r_minor_header: the minor header of the shard in the root block with largest height
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

    # TODO: make sure all the callers check None
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

        self.height_to_minor_block_hashes.setdefault(m_block.header.height, set()).add(
            m_block.header.get_hash()
        )

        self.put_confirmed_cross_shard_transaction_deposit_list(
            m_block_hash, x_shard_receive_tx_list
        )

    def put_total_tx_count(self, m_block):
        prev_count = 0
        if m_block.header.height > 2:
            prev_count = self.get_total_tx_count(m_block.header.hash_prev_minor_block)
        count = prev_count + len(m_block.tx_list)
        self.db.put(b"tx_count_" + m_block.header.get_hash(), count.to_bytes(4, "big"))

    def get_total_tx_count(self, m_block_hash):
        count_bytes = self.db.get(b"tx_count_" + m_block_hash, None)
        if not count_bytes:
            return 0
        return int.from_bytes(count_bytes, "big")

    def get_minor_block_header_by_hash(
        self, h, consistency_check=True
    ) -> Optional[MinorBlockHeader]:
        header = self.m_header_pool.get(h, None)
        if not header and not consistency_check:
            block = self.get_minor_block_by_hash(h, False)
            header = block.header if block else None
        return header

    def get_minor_block_evm_root_hash_by_hash(self, h):
        if h not in self.m_header_pool:
            return None
        check(h in self.m_meta_pool)
        meta = self.m_meta_pool[h]
        return meta.hash_evm_state_root

    def get_minor_block_meta_by_hash(self, h):
        return self.m_meta_pool.get(h, None)

    def get_minor_block_by_hash(
        self, h: bytes, consistency_check=True
    ) -> Optional[MinorBlock]:
        if consistency_check and h not in self.m_header_pool:
            return None
        data = self.db.get(b"mblock_" + h, None)
        return MinorBlock.deserialize(data) if data else None

    def contain_minor_block_by_hash(self, h):
        return h in self.m_header_pool

    def put_minor_block_index(self, block):
        self.db.put(b"mi_%d" % block.header.height, block.header.get_hash())

    def remove_minor_block_index(self, block):
        self.db.remove(b"mi_%d" % block.header.height)

    def get_minor_block_by_height(self, height) -> Optional[MinorBlock]:
        key = b"mi_%d" % height
        if key not in self.db:
            return None
        block_hash = self.db.get(key)
        return self.get_minor_block_by_hash(block_hash, False)

    def get_block_count_by_height(self, height):
        """ Return the total number of blocks with the given height"""
        return len(self.height_to_minor_block_hashes.setdefault(height, set()))

    # ------------------------- Transaction db operations --------------------------------
    def put_transaction_index(self, tx, block_height, index):
        tx_hash = tx.get_hash()
        self.db.put(
            b"txindex_" + tx_hash,
            block_height.to_bytes(4, "big") + index.to_bytes(4, "big"),
        )

        self.put_transaction_history_index(tx, block_height, index)

    def remove_transaction_index(self, tx, block_height, index):
        tx_hash = tx.get_hash()
        self.db.remove(b"txindex_" + tx_hash)

        self.remove_transaction_history_index(tx, block_height, index)

    def contain_transaction_hash(self, tx_hash):
        key = b"txindex_" + tx_hash
        return key in self.db

    def get_transaction_by_hash(
        self, tx_hash
    ) -> Tuple[Optional[MinorBlock], Optional[int]]:
        result = self.db.get(b"txindex_" + tx_hash, None)
        if not result:
            return None, None
        check(len(result) == 8)
        block_height = int.from_bytes(result[:4], "big")
        index = int.from_bytes(result[4:], "big")
        return self.get_minor_block_by_height(block_height), index

    def put_transaction_index_from_block(self, minor_block):
        for i, tx in enumerate(minor_block.tx_list):
            self.put_transaction_index(tx, minor_block.header.height, i)

        self.put_transaction_history_index_from_block(minor_block)

    def remove_transaction_index_from_block(self, minor_block):
        for i, tx in enumerate(minor_block.tx_list):
            self.remove_transaction_index(tx, minor_block.header.height, i)

        self.remove_transaction_history_index_from_block(minor_block)

    # -------------------------- Cross-shard tx operations ----------------------------
    def put_minor_block_xshard_tx_list(self, h, tx_list: CrossShardTransactionList):
        # self.x_shard_set.add(h)
        self.db.put(b"xShard_" + h, tx_list.serialize())

    def get_minor_block_xshard_tx_list(self, h) -> CrossShardTransactionList:
        key = b"xShard_" + h
        if key not in self.db:
            return None
        return CrossShardTransactionList.deserialize(self.db.get(key))

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
