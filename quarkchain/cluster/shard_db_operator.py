from typing import Tuple, Optional, List

from quarkchain.cluster.rpc import TransactionDetail
from quarkchain.core import (
    RootBlock,
    MinorBlock,
    MinorBlockHeader,
    CrossShardTransactionList,
    Branch,
    Address,
    CrossShardTransactionDeposit,
)
from quarkchain.utils import check, Logger
from cachetools import LRUCache


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
        evm_tx = tx.tx.to_evm_tx()
        from_full_shard_id = self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
            evm_tx.from_full_shard_key
        )
        addr = Address(evm_tx.sender, from_full_shard_id)
        key = self.__encode_address_transaction_key(addr, block_height, index, False)
        func(key, b"")
        # "to" can be empty for smart contract deployment
        if evm_tx.to and self.branch.is_in_branch(evm_tx.to_full_shard_key):
            to_full_shard_id = self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                evm_tx.to_full_shard_key
            )
            addr = Address(evm_tx.to, to_full_shard_id)
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
        )  # type: List[CrossShardTransactionDeposit]
        for i, tx in enumerate(x_shard_receive_tx_list):
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

    def get_transactions_by_address(
        self,
        address: Address,
        transfer_token_id: Optional[int] = None,
        start: bytes = b"",
        limit: int = 10,
    ):
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
            if limit <= 0:
                break
            height = int.from_bytes(k[5 + 24 : 5 + 24 + 4], "big")
            cross_shard = int(k[5 + 24 + 4]) == 0
            index = int.from_bytes(k[5 + 24 + 4 + 1 :], "big")
            m_block = self.get_minor_block_by_height(height)
            # skip if token ID specified and not match
            should_skip = (
                lambda tx: transfer_token_id is None
                or tx.transfer_token_id == transfer_token_id
            )
            if cross_shard:  # cross shard receive
                x_shard_receive_tx_list = self.__get_confirmed_cross_shard_transaction_deposit_list(
                    m_block.header.get_hash()
                )
                tx = x_shard_receive_tx_list[
                    index
                ]  # type: CrossShardTransactionDeposit
                if should_skip(tx):
                    limit -= 1
                    tx_list.append(
                        TransactionDetail(
                            tx.tx_hash,
                            tx.from_address,
                            tx.to_address,
                            tx.value,
                            height,
                            m_block.header.create_time,
                            True,
                            tx.gas_token_id,
                            tx.transfer_token_id,
                            is_from_root_chain=tx.is_from_root_chain,
                        )
                    )
            else:
                receipt = m_block.get_receipt(self.db, index)
                tx = m_block.tx_list[index]  # tx is Transaction
                evm_tx = tx.tx.to_evm_tx()
                if should_skip(evm_tx):
                    limit -= 1
                    tx_list.append(
                        TransactionDetail(
                            tx.get_hash(),
                            Address(evm_tx.sender, evm_tx.from_full_shard_key),
                            Address(evm_tx.to, evm_tx.to_full_shard_key)
                            if evm_tx.to
                            else None,
                            evm_tx.value,
                            height,
                            m_block.header.create_time,
                            receipt.success == b"\x01",
                            evm_tx.gas_token_id,
                            evm_tx.transfer_token_id,
                            is_from_root_chain=False,
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
        self.r_header_pool = dict()

        # height -> set(minor block hash) for counting wasted blocks
        self.height_to_minor_block_hashes = dict()
        self.rblock_cache = LRUCache(maxsize=256)
        self.mblock_cache = LRUCache(maxsize=1024)

    # ------------------------- Root block db operations --------------------------------
    def put_root_block(self, root_block, r_minor_header=None):
        """ r_minor_header: the minor header of the shard in the root block with largest height
        """
        root_block_hash = root_block.header.get_hash()

        self.db.put(b"rblock_" + root_block_hash, root_block.serialize())
        r_minor_header_hash = r_minor_header.get_hash() if r_minor_header else b""
        self.db.put(b"r_last_m" + root_block_hash, r_minor_header_hash)

    def get_root_block_by_hash(self, h):
        key = b"rblock_" + h
        if key in self.rblock_cache:
            return self.rblock_cache[key]
        raw_block = self.db.get(key, None)
        block = raw_block and RootBlock.deserialize(raw_block)
        if block is not None:
            self.rblock_cache[key] = block
        return block

    def get_root_block_header_by_hash(self, h):
        block = self.get_root_block_by_hash(h)
        return block and block.header

    def get_root_block_header_by_height(self, h, height):
        r_header = self.get_root_block_header_by_hash(h)
        if height > r_header.height:
            return None
        while height != r_header.height:
            r_header = self.get_root_block_header_by_hash(r_header.hash_prev_block)
        return r_header

    def contain_root_block_by_hash(self, h):
        return (b"rblock_" + h) in self.db

    def get_last_confirmed_minor_block_header_at_root_block(self, root_hash):
        """Return the latest minor block header confirmed by the root chain at the given root hash"""
        r_minor_header_hash = self.db.get(b"r_last_m" + root_hash, None)
        if r_minor_header_hash is None or r_minor_header_hash == b"":
            return None
        return self.get_minor_block_header_by_hash(r_minor_header_hash)

    def put_genesis_block(self, root_block_hash, genesis_block):
        self.db.put(b"genesis_" + root_block_hash, genesis_block.serialize())

    def get_genesis_block(self, root_block_hash):
        data = self.db.get(b"genesis_" + root_block_hash, None)
        if not data:
            return None
        else:
            return MinorBlock.deserialize(data)

    # ------------------------- Minor block db operations --------------------------------
    def put_minor_block(self, m_block, x_shard_receive_tx_list):
        m_block_hash = m_block.header.get_hash()

        self.db.put(b"mblock_" + m_block_hash, m_block.serialize())
        self.put_total_tx_count(m_block)

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

    def get_minor_block_header_by_hash(self, h) -> Optional[MinorBlockHeader]:
        block = self.get_minor_block_by_hash(h)
        return block and block.header

    def get_minor_block_evm_root_hash_by_hash(self, h):
        meta = self.get_minor_block_meta_by_hash(h)
        return meta.hash_evm_state_root if meta else None

    def get_minor_block_meta_by_hash(self, h):
        block = self.get_minor_block_by_hash(h)
        return block and block.meta

    def get_minor_block_by_hash(self, h: bytes) -> Optional[MinorBlock]:
        key = b"mblock_" + h
        if key in self.mblock_cache:
            return self.mblock_cache[key]
        raw_block = self.db.get(key, None)
        block = raw_block and MinorBlock.deserialize(raw_block)
        if block is not None:
            self.mblock_cache[key] = block
        return block

    def contain_minor_block_by_hash(self, h):
        return (b"mblock_" + h) in self.db

    def put_minor_block_index(self, block):
        self.db.put(b"mi_%d" % block.header.height, block.header.get_hash())

    def remove_minor_block_index(self, block):
        self.db.remove(b"mi_%d" % block.header.height)

    def get_minor_block_by_height(self, height) -> Optional[MinorBlock]:
        key = b"mi_%d" % height
        if key not in self.db:
            return None
        block_hash = self.db.get(key)
        return self.get_minor_block_by_hash(block_hash)

    def get_minor_block_header_by_height(self, height) -> Optional[MinorBlock]:
        key = b"mi_%d" % height
        if key not in self.db:
            return None
        block_hash = self.db.get(key)
        return self.get_minor_block_header_by_hash(block_hash)

    def get_block_count_by_height(self, height):
        """ Return the total number of blocks with the given height"""
        return len(self.height_to_minor_block_hashes.setdefault(height, set()))

    def is_minor_block_committed_by_hash(self, h):
        return self.db.get(b"commit_" + h) is not None

    def commit_minor_block_by_hash(self, h):
        self.put(b"commit_" + h, b"")

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
