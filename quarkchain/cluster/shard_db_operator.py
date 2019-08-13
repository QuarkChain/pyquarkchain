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
    TypedTransaction,
    HashList,
)
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import check
from cachetools import LRUCache


class TransactionHistoryMixin:
    @staticmethod
    def __encode_address_transaction_key(recipient, height, index, cross_shard):
        cross_shard_byte = b"\x00" if cross_shard else b"\x01"
        return (
            b"index_addr_"
            + recipient
            + height.to_bytes(4, "big")
            + cross_shard_byte
            + index.to_bytes(4, "big")
        )

    @staticmethod
    def __encode_alltx_key(height, index, cross_shard):
        cross_shard_byte = b"\x00" if cross_shard else b"\x01"
        return (
            b"index_alltx_"
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
        return CrossShardTransactionList.from_data(data).tx_list

    def __update_transaction_history_index(self, tx, block_height, index, func):
        evm_tx = tx.tx.to_evm_tx()
        key = self.__encode_alltx_key(block_height, index, False)
        func(key, b"")
        key = self.__encode_address_transaction_key(
            evm_tx.sender, block_height, index, False
        )
        func(key, b"")
        # "to" can be empty for smart contract deployment
        if evm_tx.to and self.branch.is_in_branch(evm_tx.to_full_shard_key):
            key = self.__encode_address_transaction_key(
                evm_tx.to, block_height, index, False
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
            # ignore dummy coinbase reward deposits
            if tx.is_from_root_chain and tx.value == 0:
                continue
            key = self.__encode_address_transaction_key(
                tx.to_address.recipient, minor_block.header.height, i, True
            )
            func(key, b"")
            # skip ALL coinbase reward deposits for all tx index
            if not tx.is_from_root_chain:
                key = self.__encode_alltx_key(minor_block.header.height, i, True)
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

    def get_all_transactions(
        self, start: bytes = b"", limit: int = 10
    ) -> (List[TransactionDetail], bytes):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        end = b"index_alltx_"
        original_start = (int.from_bytes(end, byteorder="big") + 1).to_bytes(
            len(end), byteorder="big"
        )
        if not start or start > original_start:
            start = original_start
        # decoding starts at byte 12 == len("index_alltx_")
        return self.__get_transaction_details(
            start, end, limit, decoding_byte_offset=12, skip_coinbase_rewards=True
        )

    def get_transactions_by_address(
        self,
        address: Address,
        transfer_token_id: Optional[int] = None,
        start: bytes = b"",
        limit: int = 10,
    ) -> (List[TransactionDetail], bytes):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        # only recipient addr needed to match
        # full_shard_key can be ignored since no TX on other shards is stored here
        end = b"index_addr_" + address.recipient
        original_start = (int.from_bytes(end, byteorder="big") + 1).to_bytes(
            len(end), byteorder="big"
        )
        # reset start to the latest if start is not valid
        if not start or start > original_start:
            start = original_start

        # decoding starts at byte 11 + 20 == len("index_addr_") + len(Address.recipient)
        return self.__get_transaction_details(
            start,
            end,
            limit,
            decoding_byte_offset=11 + 20,
            transfer_token_id=transfer_token_id,
        )

    def __get_transaction_details(
        self,
        start_key: bytes,
        end_key: bytes,
        limit: int,
        decoding_byte_offset: int,
        skip_coinbase_rewards: bool = False,
        transfer_token_id: Optional[int] = None,
    ) -> (List[TransactionDetail], bytes):
        next_key, tx_list = end_key, []
        tx_hashes = set()

        def skip_xshard(xshard_tx: CrossShardTransactionDeposit):
            if xshard_tx.is_from_root_chain:
                return (
                    skip_coinbase_rewards or xshard_tx.value == 0
                )  # value 0 if dummy deposit
            return (
                transfer_token_id is not None
                and xshard_tx.transfer_token_id != transfer_token_id
            )

        def skip_tx(normal_tx: EvmTransaction):
            return (
                transfer_token_id is not None
                and normal_tx.transfer_token_id != transfer_token_id
            )

        for k, v in self.db.reversed_range_iter(start_key, end_key):
            if limit <= 0:
                break
            height = int.from_bytes(
                k[decoding_byte_offset : decoding_byte_offset + 4], "big"
            )
            cross_shard = int(k[decoding_byte_offset + 4]) == 0
            index = int.from_bytes(k[decoding_byte_offset + 4 + 1 :], "big")
            m_block = self.get_minor_block_by_height(height)
            if cross_shard:  # cross shard receive
                x_shard_receive_tx_list = self.__get_confirmed_cross_shard_transaction_deposit_list(
                    m_block.header.get_hash()
                )
                tx = x_shard_receive_tx_list[
                    index
                ]  # type: CrossShardTransactionDeposit
                if tx.tx_hash not in tx_hashes and not skip_xshard(tx):
                    limit -= 1
                    tx_hashes.add(tx.tx_hash)
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
                # no need to provide cross shard deposit list for in-shard tx receipt
                receipt = m_block.get_receipt(
                    self.db, index, x_shard_receive_tx_list=None
                )
                tx = m_block.tx_list[index]  # type: TypedTransaction
                evm_tx = tx.tx.to_evm_tx()
                tx_hash = tx.get_hash()
                if tx_hash not in tx_hashes and not skip_tx(evm_tx):
                    limit -= 1
                    tx_hashes.add(tx_hash)
                    tx_list.append(
                        TransactionDetail(
                            tx_hash,
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
            next_key = (int.from_bytes(k, byteorder="big") - 1).to_bytes(
                len(k), byteorder="big"
            )

        return tx_list, next_key


class ShardDbOperator(TransactionHistoryMixin):
    def __init__(self, db, env, branch: Branch):
        self.env = env
        self.db = db
        self.branch = branch

        # height -> set(minor block hash) for counting wasted blocks
        self.height_to_minor_block_hashes = dict()
        self.rblock_cache = LRUCache(maxsize=256)
        self.mblock_cache = LRUCache(maxsize=4096)
        self.mblock_header_cache = LRUCache(maxsize=10240)

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

        self.__put_xshard_deposit_hash_list(
            m_block_hash, HashList([d.tx_hash for d in x_shard_receive_tx_list])
        )

    def put_total_tx_count(self, m_block):
        prev_count = 0
        if m_block.header.height > 0:
            prev_count = self.get_total_tx_count(m_block.header.hash_prev_minor_block)
        count = prev_count + len(m_block.tx_list)
        self.db.put(b"tx_count_" + m_block.header.get_hash(), count.to_bytes(4, "big"))

    def get_total_tx_count(self, m_block_hash):
        count_bytes = self.db.get(b"tx_count_" + m_block_hash, None)
        if not count_bytes:
            return 0
        return int.from_bytes(count_bytes, "big")

    def get_minor_block_header_by_hash(self, h) -> Optional[MinorBlockHeader]:
        if h in self.mblock_header_cache:
            return self.mblock_header_cache[h]
        block = self.get_minor_block_by_hash(h)
        if block is not None:
            self.mblock_header_cache[h] = block.header
            return block.header
        return None

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
    def __put_txindex(self, tx_hash, block_height, index):
        self.db.put(
            b"txindex_" + tx_hash,
            block_height.to_bytes(4, "big") + index.to_bytes(4, "big"),
        )

    def put_transaction_index(self, tx, block_height, index):
        self.__put_txindex(tx.get_hash(), block_height, index)
        self.put_transaction_history_index(tx, block_height, index)

    def __remove_txindex(self, tx_hash, block_height, index):
        self.db.remove(b"txindex_" + tx_hash)

    def remove_transaction_index(self, tx, block_height, index):
        self.__remove_txindex(tx.get_hash(), block_height, index)
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

        deposit_hlist = self.get_xshard_deposit_hash_list(minor_block.header.get_hash())
        # Old version of db may not have the hash list
        if deposit_hlist is not None:
            for i, h in enumerate(deposit_hlist.hlist):
                self.__put_txindex(
                    h, minor_block.header.height, i + len(minor_block.tx_list)
                )

        self.put_transaction_history_index_from_block(minor_block)

    def remove_transaction_index_from_block(self, minor_block):
        for i, tx in enumerate(minor_block.tx_list):
            self.remove_transaction_index(tx, minor_block.header.height, i)

        deposit_hlist = self.get_xshard_deposit_hash_list(minor_block.header.get_hash())
        # Old version of db may not have the hash list
        if deposit_hlist is not None:
            for i, h in enumerate(deposit_hlist.hlist):
                self.__remove_txindex(
                    h, minor_block.header.height, i + len(minor_block.tx_list)
                )

        self.remove_transaction_history_index_from_block(minor_block)

    # -------------------------- Cross-shard tx operations ----------------------------
    def put_minor_block_xshard_tx_list(self, h, tx_list: CrossShardTransactionList):
        self.db.put(b"xShard_" + h, tx_list.serialize())

    def get_minor_block_xshard_tx_list(self, h) -> Optional[CrossShardTransactionList]:
        key = b"xShard_" + h
        if key not in self.db:
            return None
        return CrossShardTransactionList.from_data(self.db.get(key))

    def contain_remote_minor_block_hash(self, h):
        key = b"xShard_" + h
        return key in self.db

    def __put_xshard_deposit_hash_list(self, h, hlist: HashList):
        self.db.put(b"xd_" + h, hlist.serialize())

    def get_xshard_deposit_hash_list(self, h) -> Optional[HashList]:
        data = self.db.get(b"xd_" + h)
        if data is None:
            return None
        return HashList.deserialize(data)

    # ------------------------- Common operations -----------------------------------------
    def put(self, key, value):
        self.db.put(key, value)

    def get(self, key, default=None):
        return self.db.get(key, default)

    def __getitem__(self, key):
        return self[key]
