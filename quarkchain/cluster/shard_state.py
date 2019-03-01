import asyncio
import functools
import json
import time
from collections import Counter, defaultdict, deque
from fractions import Fraction
from typing import Dict, List, Optional, Tuple, Union

from quarkchain.cluster.filter import Filter
from quarkchain.cluster.miner import validate_seal
from quarkchain.cluster.neighbor import is_neighbor
from quarkchain.cluster.rpc import ShardStats, TransactionDetail
from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.core import (
    Address,
    Branch,
    CrossShardTransactionDeposit,
    CrossShardTransactionList,
    Log,
    MinorBlock,
    MinorBlockHeader,
    MinorBlockMeta,
    RootBlock,
    SerializedEvmTransaction,
    TokenBalanceMap,
    TransactionReceipt,
    TypedTransaction,
    XshardTxCursorInfo,
    calculate_merkle_root,
    mk_receipt_sha,
)
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.evm import opcodes
from quarkchain.evm.messages import apply_transaction, validate_transaction
from quarkchain.evm.state import State as EvmState
from quarkchain.evm.transaction_queue import TransactionQueue
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.evm.utils import add_dict
from quarkchain.genesis import GenesisManager
from quarkchain.reward import ConstMinorBlockRewardCalcultor
from quarkchain.utils import Logger, check, time_ms


class GasPriceSuggestionOracle:
    def __init__(
        self, last_price: int, last_head: bytes, check_blocks: int, percentile: int
    ):
        self.last_price = last_price
        self.last_head = last_head
        self.check_blocks = check_blocks
        self.percentile = percentile


class XshardTxCursor:

    # Cursor definitions (root_block_height, mblock_index, deposit_index)
    # (x, 0, 0): EOF
    # (x, 0, z), z > 0: Root-block coinbase tx (always exist)
    # (x, y, z), y > 0: Minor-block x-shard tx (may not exist if not neighbor or no xshard)
    #
    # Note that: the cursor must be
    # - EOF
    # - A valid x-shard transaction deposit

    def __init__(self, shard_state, mblock_header, cursor_info):
        self.shard_state = shard_state
        self.db = shard_state.db

        # Recover cursor
        self.max_rblock_header = self.db.get_root_block_header_by_hash(
            mblock_header.hash_prev_root_block
        )
        rblock_header = self.db.get_root_block_header_by_height(
            mblock_header.hash_prev_root_block, cursor_info.root_block_height
        )
        self.mblock_index = cursor_info.minor_block_index
        self.xshard_deposit_index = cursor_info.xshard_deposit_index

        # Recover rblock and xtx_list if it is processing tx from peer-shard
        self.xtx_list = None
        if rblock_header is not None:
            self.rblock = self.db.get_root_block_by_hash(rblock_header.get_hash())
            if self.mblock_index != 0:
                self.xtx_list = self.db.get_minor_block_xshard_tx_list(
                    self.rblock.minor_block_header_list[
                        self.mblock_index - 1
                    ].get_hash()
                ).tx_list
        else:
            # EOF
            self.rblock = None

    def __get_current_tx(self):
        if self.mblock_index == 0:
            # 0 is reserved for EOF
            check(self.xshard_deposit_index == 1 or self.xshard_deposit_index == 2)
            # TODO: For single native token only
            if self.xshard_deposit_index == 1:
                coinbase_amount = 0
                if self.shard_state.branch.is_in_branch(
                    self.rblock.header.coinbase_address.full_shard_key
                ):
                    coinbase_amount = self.rblock.header.coinbase_amount_map.balance_map.get(
                        self.shard_state.genesis_token_id, 0
                    )

                # Perform x-shard from root chain coinbase
                return CrossShardTransactionDeposit(
                    tx_hash=self.rblock.header.get_hash(),
                    from_address=self.rblock.header.coinbase_address,
                    to_address=self.rblock.header.coinbase_address,
                    value=coinbase_amount,
                    gas_price=0,
                    gas_token_id=self.shard_state.genesis_token_id,
                    transfer_token_id=self.shard_state.genesis_token_id,
                )

            return None
        elif self.xshard_deposit_index < len(self.xtx_list):
            return self.xtx_list[self.xshard_deposit_index]
        else:
            return None

    def get_next_tx(self):
        """ Return XshardDeposit if succeed else return None
        """
        # Check if reach EOF
        if self.rblock is None:
            return None

        self.xshard_deposit_index += 1
        tx = self.__get_current_tx()
        # Reach the EOF of the mblock or rblock x-shard txs
        if tx is not None:
            return tx

        self.mblock_index += 1
        self.xshard_deposit_index = 0

        # Iterate minor blocks' cross-shard transactions
        while self.mblock_index <= len(self.rblock.minor_block_header_list):
            # If it is not neighbor, move to next minor block
            mblock_header = self.rblock.minor_block_header_list[self.mblock_index - 1]
            if (
                not self.shard_state._is_neighbor(
                    mblock_header.branch, self.rblock.header.height
                )
                or mblock_header.branch == self.shard_state.branch
            ):
                check(self.xshard_deposit_index == 0)
                self.mblock_index += 1
                continue

            # Check if the neighbor has the permission to send tx to local shard
            prev_root_header = self.db.get_root_block_header_by_hash(
                mblock_header.hash_prev_root_block
            )
            if (
                prev_root_header.height
                <= self.shard_state.env.quark_chain_config.get_genesis_root_height(
                    self.shard_state.full_shard_id
                )
            ):
                check(self.xshard_deposit_index == 0)
                check(
                    self.db.get_minor_block_xshard_tx_list(mblock_header.get_hash())
                    is None
                )
                self.mblock_index += 1
                continue

            self.xtx_list = self.db.get_minor_block_xshard_tx_list(
                mblock_header.get_hash()
            ).tx_list

            tx = self.__get_current_tx()
            if tx is not None:
                return tx

            # Move to next minor block
            check(self.xshard_deposit_index == 0)
            self.mblock_index += 1

        # Move to next root block
        rblock_header = self.db.get_root_block_header_by_height(
            self.max_rblock_header.get_hash(), self.rblock.header.height + 1
        )
        if rblock_header is None:
            # EOF
            self.rblock = None
            self.mblock_index = 0
            self.xshard_deposit_index = 0
            return None
        else:
            # Root-block coinbase (always exist)
            self.rblock = self.db.get_root_block_by_hash(rblock_header.get_hash())
            self.mblock_index = 0
            self.xshard_deposit_index = 1
            return self.__get_current_tx()

    def get_cursor_info(self):
        root_block_height = (
            self.rblock.header.height
            if self.rblock is not None
            else self.max_rblock_header.height + 1
        )
        return XshardTxCursorInfo(
            root_block_height=root_block_height,
            minor_block_index=self.mblock_index,
            xshard_deposit_index=self.xshard_deposit_index,
        )


class ShardState:
    """  State of a shard, which includes
    - evm state
    - minor blockchain
    - root blockchain and cross-shard transactions
    TODO: Support
    - reshard by split
    """

    def __init__(self, env, full_shard_id: int, db=None, diff_calc=None):
        self.env = env
        self.shard_config = env.quark_chain_config.shards[full_shard_id]
        self.full_shard_id = full_shard_id
        if not diff_calc:
            cutoff = self.shard_config.DIFFICULTY_ADJUSTMENT_CUTOFF_TIME
            diff_factor = self.shard_config.DIFFICULTY_ADJUSTMENT_FACTOR
            min_diff = self.shard_config.GENESIS.DIFFICULTY
            check(cutoff > 0 and diff_factor > 0 and min_diff > 0)
            diff_calc = EthDifficultyCalculator(
                cutoff=cutoff, diff_factor=diff_factor, minimum_diff=min_diff
            )
        self.diff_calc = diff_calc
        self.reward_calc = ConstMinorBlockRewardCalcultor(env)
        self.raw_db = db if db is not None else env.db
        self.branch = Branch(full_shard_id)
        self.db = ShardDbOperator(self.raw_db, self.env, self.branch)
        self.tx_queue = TransactionQueue()  # queue of EvmTransaction
        self.tx_dict = dict()  # hash -> Transaction for explorer
        self.initialized = False
        self.header_tip = None  # MinorBlockHeader
        # TODO: make the oracle configurable
        self.gas_price_suggestion_oracle = GasPriceSuggestionOracle(
            last_price=0, last_head=b"", check_blocks=5, percentile=50
        )

        # new blocks that passed POW validation and should be made available to whole network
        self.new_block_pool = dict()
        # header hash -> (height, [coinbase address]) during previous blocks (ascending)
        self.coinbase_addr_cache = dict()  # type: Dict[bytes, Tuple[int, Deque[bytes]]]
        self.genesis_token_id = self.env.quark_chain_config.genesis_token
        self.local_fee_rate = (
            1 - self.env.quark_chain_config.reward_tax_rate
        )  # type: Fraction

    def init_from_root_block(self, root_block):
        """ Master will send its root chain tip when it connects to slaves.
        Shards will initialize its state based on the root block.
        """
        check(
            root_block.header.height
            > self.env.quark_chain_config.get_genesis_root_height(self.full_shard_id)
        )
        check(not self.initialized)
        self.initialized = True

        Logger.info(
            "[{}] Initializing shard state from root height {} hash {}".format(
                self.branch.to_str(),
                root_block.header.height,
                root_block.header.get_hash().hex(),
            )
        )

        confirmed_header_tip = self.db.get_last_confirmed_minor_block_header_at_root_block(
            root_block.header.get_hash()
        )
        header_tip = confirmed_header_tip
        if not header_tip:
            # root chain has not confirmed any block on this shard
            # get the genesis block from db
            header_tip = self.db.get_minor_block_by_height(0).header

        self.header_tip = header_tip
        self.root_tip = root_block.header
        header_tip_hash = header_tip.get_hash()

        self.db.recover_state(self.root_tip, self.header_tip)
        Logger.info(
            "[{}] Done recovery from db. shard tip {} {}, root tip {} {}".format(
                self.branch.to_str(),
                self.header_tip.height,
                header_tip_hash.hex(),
                self.root_tip.height,
                self.root_tip.get_hash().hex(),
            )
        )

        self.meta_tip = self.db.get_minor_block_meta_by_hash(header_tip_hash)
        self.confirmed_header_tip = confirmed_header_tip
        self.evm_state = self.__create_evm_state(
            self.meta_tip.hash_evm_state_root, header_hash=header_tip_hash
        )
        check(
            self.db.get_minor_block_evm_root_hash_by_hash(header_tip_hash)
            == self.meta_tip.hash_evm_state_root
        )

        self.__rewrite_block_index_to(
            self.db.get_minor_block_by_hash(header_tip_hash), add_tx_back_to_queue=False
        )

    def __create_evm_state(
        self, trie_root_hash: Optional[bytes], header_hash: Optional[bytes]
    ):
        """EVM state with given root hash and block hash AFTER which being evaluated."""
        state = EvmState(
            env=self.env.evm_env, db=self.raw_db, qkc_config=self.env.quark_chain_config
        )
        state.shard_config = self.shard_config
        if trie_root_hash:
            state.trie.root_hash = trie_root_hash

        if self.shard_config.POSW_CONFIG.ENABLED and header_hash is not None:
            state.sender_disallow_list = self._get_posw_coinbase_blockcnt(
                header_hash
            ).keys()
        return state

    def init_genesis_state(self, root_block):
        """ root_block should have the same height as configured in shard GENESIS.
        If a genesis block has already been created (probably from another root block
        with the same height), create and store the new genesis block from root_block
        without modifying the in-memory state of this ShardState object.
        Additionally returns the coinbase_amount_map from the genesis block
        """
        height = self.env.quark_chain_config.get_genesis_root_height(self.full_shard_id)
        check(root_block.header.height == height)

        genesis_manager = GenesisManager(self.env.quark_chain_config)
        genesis_block, coinbase_amount_map = genesis_manager.create_minor_block(
            root_block,
            self.full_shard_id,
            self.__create_evm_state(trie_root_hash=None, header_hash=None),
        )

        self.db.put_minor_block(genesis_block, [])
        self.db.put_root_block(root_block)
        self.db.put_genesis_block(root_block.header.get_hash(), genesis_block)

        if self.initialized:
            # already initialized. just return the block without resetting the state.
            return genesis_block, coinbase_amount_map

        # block index should not be overwritten if there is already a genesis block
        # this must happen after the above initialization check
        self.db.put_minor_block_index(genesis_block)

        self.root_tip = root_block.header
        # Tips that are confirmed by root
        self.confirmed_header_tip = None
        # Tips that are unconfirmed by root
        self.header_tip = genesis_block.header
        self.meta_tip = genesis_block.meta
        self.evm_state = self.__create_evm_state(
            genesis_block.meta.hash_evm_state_root,
            header_hash=genesis_block.header.get_hash(),
        )

        Logger.info(
            "[{}] Initialized genensis state at root block {} {}, genesis block hash {}".format(
                self.branch.to_str(),
                self.root_tip.height,
                self.root_tip.get_hash().hex(),
                self.header_tip.get_hash().hex(),
            )
        )
        self.initialized = True
        return genesis_block, coinbase_amount_map

    def __validate_tx(
        self,
        tx: TypedTransaction,
        evm_state,
        from_address=None,
        gas=None,
        xshard_gas_limit=None,
    ) -> EvmTransaction:
        """from_address will be set for execute_tx"""
        evm_tx = tx.tx.to_evm_tx()

        if from_address:
            check(evm_tx.from_full_shard_key == from_address.full_shard_key)
            nonce = evm_state.get_nonce(from_address.recipient)
            # have to create a new evm_tx as nonce is immutable
            evm_tx = EvmTransaction(
                nonce,
                evm_tx.gasprice,
                gas if gas else evm_tx.startgas,  # override gas if specified
                evm_tx.to,
                evm_tx.value,
                evm_tx.data,
                from_full_shard_key=evm_tx.from_full_shard_key,
                to_full_shard_key=evm_tx.to_full_shard_key,
                network_id=evm_tx.network_id,
                gas_token_id=evm_tx.gas_token_id,
                transfer_token_id=evm_tx.transfer_token_id,
            )
            evm_tx.sender = from_address.recipient

        evm_tx.set_quark_chain_config(self.env.quark_chain_config)

        if evm_tx.network_id != self.env.quark_chain_config.NETWORK_ID:
            raise RuntimeError(
                "evm tx network id mismatch. expect {} but got {}".format(
                    self.env.quark_chain_config.NETWORK_ID, evm_tx.network_id
                )
            )

        if not self.branch.is_in_branch(evm_tx.from_full_shard_key):
            raise RuntimeError(
                "evm tx from_full_shard_key ({}) not in this branch ({}).".format(
                    hex(evm_tx.from_full_shard_key),
                    hex(self.branch.get_full_shard_id()),
                )
            )

        to_branch = Branch(evm_tx.to_full_shard_id)

        initialized_full_shard_ids = self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
            self.root_tip.height
        )
        if (
            evm_tx.is_cross_shard
            and to_branch.get_full_shard_id() not in initialized_full_shard_ids
        ):
            raise RuntimeError(
                "evm tx to_full_shard_id {} is not initialized yet. current root height {}".format(
                    evm_tx.to_full_shard_id, self.root_tip.height
                )
            )
        if evm_tx.is_cross_shard and not self._is_neighbor(to_branch):
            raise RuntimeError(
                "evm tx to_full_shard_id {} is not a neighbor of from_full_shard_id {}".format(
                    evm_tx.to_full_shard_id, evm_tx.from_full_shard_id
                )
            )
        xshard_gas_limit = self.get_xshard_gas_limit(xshard_gas_limit=xshard_gas_limit)
        if evm_tx.is_cross_shard and evm_tx.startgas > xshard_gas_limit:
            raise RuntimeError("xshard evm tx exceeds xshard gas limit")

        # This will check signature, nonce, balance, gas limit
        validate_transaction(evm_state, evm_tx)

        return evm_tx

    def get_gas_limit(self, gas_limit=None):
        if gas_limit is None:
            gas_limit = self.env.quark_chain_config.gas_limit
        return gas_limit

    def get_xshard_gas_limit(self, gas_limit=None, xshard_gas_limit=None):
        if xshard_gas_limit is not None:
            return xshard_gas_limit
        return self.get_gas_limit(gas_limit=gas_limit) // 2

    def get_gas_limit_all(self, gas_limit=None, xshard_gas_limit=None):
        return (
            self.get_gas_limit(gas_limit),
            self.get_xshard_gas_limit(gas_limit, xshard_gas_limit),
        )

    def add_tx(self, tx: TypedTransaction, xshard_gas_limit=None):
        """ Add a tx to the tx queue
        xshard_gas_limit is used for testing, which discards the tx if
        - tx is x-shard; and
        - tx's startgas exceeds xshard_gas_limit
        """
        if (
            len(self.tx_queue)
            > self.env.quark_chain_config.TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD
        ):
            # exceeding tx queue size limit
            return False

        tx_hash = tx.get_hash()

        if self.db.contain_transaction_hash(tx_hash):
            return False

        if tx_hash in self.tx_dict:
            return False

        evm_state = self.evm_state.ephemeral_clone()
        evm_state.gas_used = 0
        try:
            evm_tx = self.__validate_tx(
                tx, evm_state, xshard_gas_limit=xshard_gas_limit
            )
            self.tx_queue.add_transaction(evm_tx)
            self.tx_dict[tx_hash] = tx
            return True
        except Exception as e:
            Logger.warning_every_sec("Failed to add transaction: {}".format(e), 1)
            return False

    def _get_evm_state_for_new_block(self, block, ephemeral=True):
        root_hash = self.db.get_minor_block_evm_root_hash_by_hash(
            block.header.hash_prev_minor_block
        )
        state = self.__create_evm_state(
            root_hash, header_hash=block.header.hash_prev_minor_block
        )
        if ephemeral:
            state = state.ephemeral_clone()
        state.timestamp = block.header.create_time
        state.gas_limit = block.header.evm_gas_limit
        state.block_number = block.header.height
        state.recent_uncles[
            state.block_number
        ] = []  # TODO [x.hash for x in block.uncles]
        # TODO: Create a account with shard info if the account is not created
        # Right now the full_shard_key for coinbase actually comes from the first tx that got applied
        state.block_coinbase = block.header.coinbase_address.recipient
        state.block_difficulty = block.header.difficulty
        state.block_reward = 0
        state.prev_headers = []  # TODO: state.add_block_header(block.header)
        return state

    def __is_same_minor_chain(self, longer_block_header, shorter_block_header):
        if shorter_block_header.height > longer_block_header.height:
            return False

        header = longer_block_header
        for i in range(longer_block_header.height - shorter_block_header.height):
            header = self.db.get_minor_block_header_by_hash(
                header.hash_prev_minor_block
            )
        return header == shorter_block_header

    def __is_same_root_chain(self, longer_block_header, shorter_block_header):
        if shorter_block_header.height > longer_block_header.height:
            return False

        header = longer_block_header
        for i in range(longer_block_header.height - shorter_block_header.height):
            header = self.db.get_root_block_header_by_hash(header.hash_prev_block)
        return header == shorter_block_header

    def __validate_block(
        self, block: MinorBlock, gas_limit=None, xshard_gas_limit=None
    ):
        """ Validate a block before running evm transactions
        """
        height = block.header.height
        if height < 1:
            raise ValueError("unexpected height")

        if not self.db.contain_minor_block_by_hash(block.header.hash_prev_minor_block):
            # TODO:  May put the block back to queue
            raise ValueError(
                "[{}] prev block not found, block height {} prev hash {}".format(
                    self.branch.to_str(),
                    height,
                    block.header.hash_prev_minor_block.hex(),
                )
            )
        prev_header = self.db.get_minor_block_header_by_hash(
            block.header.hash_prev_minor_block
        )

        if height != prev_header.height + 1:
            raise ValueError("height mismatch")

        if block.header.branch != self.branch:
            raise ValueError("branch mismatch")

        if block.header.create_time <= prev_header.create_time:
            raise ValueError(
                "incorrect create time tip time {}, new block time {}".format(
                    block.header.create_time, self.chain[-1].create_time
                )
            )

        if block.header.hash_meta != block.meta.get_hash():
            raise ValueError("hash of meta mismatch")

        if (
            len(block.header.extra_data)
            > self.env.quark_chain_config.BLOCK_EXTRA_DATA_SIZE_LIMIT
        ):
            raise ValueError("extra_data in block is too large")

        if (
            len(block.tracking_data)
            > self.env.quark_chain_config.BLOCK_EXTRA_DATA_SIZE_LIMIT
        ):
            raise ValueError("tracking_data in block is too large")

        # Gas limit check
        gas_limit, xshard_gas_limit = self.get_gas_limit_all(
            gas_limit=gas_limit, xshard_gas_limit=xshard_gas_limit
        )
        if block.header.evm_gas_limit != gas_limit:
            raise ValueError(
                "incorrect gas limit, expected %d, actual %d"
                % (gas_limit, block.header.evm_gas_limit)
            )
        if block.meta.evm_xshard_gas_limit >= block.header.evm_gas_limit:
            raise ValueError(
                "xshard_gas_limit %d should not exceed total gas_limit %d"
                % (block.meta.evm_xshard_gas_limit, block.header.evm_gas_limit)
            )
        if block.meta.evm_xshard_gas_limit != xshard_gas_limit:
            raise ValueError(
                "incorrect xshard gas limit, expected %d, actual %d"
                % (xshard_gas_limit, block.meta.evm_xshard_gas_limit)
            )

        # Make sure merkle tree is valid
        merkle_hash = calculate_merkle_root(block.tx_list)
        if merkle_hash != block.meta.hash_merkle_root:
            raise ValueError("incorrect merkle root")

        # Check the first transaction of the block
        if not self.branch.is_in_branch(block.header.coinbase_address.full_shard_key):
            raise ValueError("coinbase output address must be in the shard")

        # Check difficulty
        if not self.env.quark_chain_config.SKIP_MINOR_DIFFICULTY_CHECK:
            diff = self.diff_calc.calculate_diff_with_parent(
                prev_header, block.header.create_time
            )
            if diff != block.header.difficulty:
                raise ValueError("incorrect difficulty")

        if not self.branch.is_in_branch(block.header.coinbase_address.full_shard_key):
            raise ValueError("coinbase output must be in local shard")

        # Check whether the root header is in the root chain
        root_block_header = self.db.get_root_block_header_by_hash(
            block.header.hash_prev_root_block
        )
        if root_block_header is None:
            raise ValueError("cannot find root block for the minor block")

        if (
            root_block_header.height
            < self.db.get_root_block_header_by_hash(
                prev_header.hash_prev_root_block
            ).height
        ):
            raise ValueError("prev root block height must be non-decreasing")

        prev_confirmed_minor_block = self.db.get_last_confirmed_minor_block_header_at_root_block(
            block.header.hash_prev_root_block
        )
        if prev_confirmed_minor_block and not self.__is_same_minor_chain(
            prev_header, prev_confirmed_minor_block
        ):
            raise ValueError(
                "prev root block's minor block is not in the same chain as the minor block"
            )

        if not self.__is_same_root_chain(
            self.db.get_root_block_header_by_hash(block.header.hash_prev_root_block),
            self.db.get_root_block_header_by_hash(prev_header.hash_prev_root_block),
        ):
            raise ValueError("prev root blocks are not on the same chain")

        # Check PoW / PoSW
        self.validate_minor_block_seal(block)

    def run_block(
        self, block, evm_state=None, evm_tx_included=None, x_shard_receive_tx_list=None
    ):
        if evm_tx_included is None:
            evm_tx_included = []
        if x_shard_receive_tx_list is None:
            x_shard_receive_tx_list = []
        if evm_state is None:
            evm_state = self._get_evm_state_for_new_block(block, ephemeral=False)
        root_block_header = self.db.get_root_block_header_by_hash(
            block.header.hash_prev_root_block
        )
        prev_header = self.db.get_minor_block_header_by_hash(
            block.header.hash_prev_minor_block
        )

        xtx_list, evm_state.xshard_tx_cursor_info = self.__run_cross_shard_tx_with_cursor(
            evm_state=evm_state, mblock=block
        )
        x_shard_receive_tx_list.extend(xtx_list)

        # Adjust inshard gas limit if xshard gas limit is not exhausted
        if evm_state.gas_used < block.meta.evm_xshard_gas_limit:
            evm_state.gas_limit -= block.meta.evm_xshard_gas_limit - evm_state.gas_used

        for idx, tx in enumerate(block.tx_list):
            try:
                evm_tx = self.__validate_tx(
                    tx, evm_state, xshard_gas_limit=block.meta.evm_xshard_gas_limit
                )
                evm_tx.set_quark_chain_config(self.env.quark_chain_config)
                apply_transaction(evm_state, evm_tx, tx.get_hash())
                evm_tx_included.append(evm_tx)
            except Exception as e:
                Logger.debug_exception()
                Logger.debug(
                    "Failed to process Tx {}, idx {}, reason {}".format(
                        tx.get_hash().hex(), idx, e
                    )
                )
                raise e

        # Pay miner
        pure_coinbase_amount = self.get_coinbase_amount_map(block.header.height)
        for k, v in pure_coinbase_amount.balance_map.items():
            evm_state.delta_token_balance(evm_state.block_coinbase, k, v)

        # Update actual root hash
        evm_state.commit()
        return evm_state

    def __is_minor_block_linked_to_root_tip(self, m_block):
        """ Determine whether a minor block is a descendant of a minor block confirmed by root tip
        """
        if not self.confirmed_header_tip:
            # genesis
            return True

        if m_block.header.height <= self.confirmed_header_tip.height:
            return False

        header = m_block.header
        for i in range(m_block.header.height - self.confirmed_header_tip.height):
            header = self.db.get_minor_block_header_by_hash(
                header.hash_prev_minor_block
            )

        return header == self.confirmed_header_tip

    def __rewrite_block_index_to(self, minor_block, add_tx_back_to_queue=True):
        """ Find the common ancestor in the current chain and rewrite index till minor_block """
        new_chain = []
        old_chain = []

        # minor_block height could be lower than the current tip
        # we should revert all the blocks above minor_block height
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
            if block.header.height <= 0:
                break
            block = self.db.get_minor_block_by_hash(block.header.hash_prev_minor_block)

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
        for tx in block.tx_list:
            self.tx_dict[tx.get_hash()] = tx
            self.tx_queue.add_transaction(tx.tx.to_evm_tx())

    def __remove_transactions_from_block(self, block):
        evm_tx_list = []
        for tx in block.tx_list:
            self.tx_dict.pop(tx.get_hash(), None)
            evm_tx_list.append(tx.tx.to_evm_tx())
        self.tx_queue = self.tx_queue.diff(evm_tx_list)

    def add_block(
        self, block, skip_if_too_old=True, gas_limit=None, xshard_gas_limit=None
    ):
        """  Add a block to local db.  Perform validate and update tip accordingly
        gas_limit and xshard_gas_limit are used for testing only.
        Returns None if block is already added.
        Returns a list of CrossShardTransactionDeposit from block.
        Additionally, returns a map of reward token balances for this block
        Raises on any error.
        """
        start_time = time.time()
        start_ms = time_ms()

        if skip_if_too_old:
            if (
                self.header_tip.height - block.header.height
                > self.shard_config.max_stale_minor_block_height_diff
            ):
                Logger.info(
                    "[{}] drop old block {} << {}".format(
                        self.branch.to_str(),
                        block.header.height,
                        self.header_tip.height,
                    )
                )
                raise ValueError(
                    "block is too old {} << {}".format(
                        block.header.height, self.header_tip.height
                    )
                )

        block_hash = block.header.get_hash()
        if self.db.contain_minor_block_by_hash(block_hash):
            return None, None

        evm_tx_included = []
        x_shard_receive_tx_list = []
        # Throw exception if fail to run
        self.__validate_block(
            block, gas_limit=gas_limit, xshard_gas_limit=xshard_gas_limit
        )
        evm_state = self.run_block(
            block,
            evm_tx_included=evm_tx_included,
            x_shard_receive_tx_list=x_shard_receive_tx_list,
        )

        # ------------------------ Validate ending result of the block --------------------
        if evm_state.xshard_tx_cursor_info != block.meta.xshard_tx_cursor_info:
            raise ValueError("Cross-shard transaction cursor info mismatches!")

        if block.meta.hash_evm_state_root != evm_state.trie.root_hash:
            raise ValueError(
                "state root mismatch: header %s computed %s"
                % (block.meta.hash_evm_state_root.hex(), evm_state.trie.root_hash.hex())
            )

        receipt_root = mk_receipt_sha(evm_state.receipts, evm_state.db)
        if block.meta.hash_evm_receipt_root != receipt_root:
            raise ValueError(
                "receipt root mismatch: header {} computed {}".format(
                    block.meta.hash_evm_receipt_root.hex(), receipt_root.hex()
                )
            )

        if evm_state.gas_used != block.meta.evm_gas_used:
            raise ValueError(
                "gas used mismatch: header %d computed %d"
                % (block.meta.evm_gas_used, evm_state.gas_used)
            )

        if (
            evm_state.xshard_receive_gas_used
            != block.meta.evm_cross_shard_receive_gas_used
        ):
            raise ValueError(
                "x-shard gas used mismatch: header %d computed %d"
                % (
                    block.meta.evm_cross_shard_receive_gas_used,
                    evm_state.xshard_receive_gas_used,
                )
            )
        coinbase_amount_map = self.get_coinbase_amount_map(block.header.height)
        # add block reward
        coinbase_amount_map.add(evm_state.block_fee_tokens)

        if (
            coinbase_amount_map.balance_map
            != block.header.coinbase_amount_map.balance_map
        ):
            raise ValueError("coinbase reward incorrect")

        if evm_state.bloom != block.header.bloom:
            raise ValueError("bloom mismatch")

        self.db.put_minor_block(block, x_shard_receive_tx_list)

        # Update tip if a block is appended or a fork is longer (with the same ancestor confirmed by root block tip)
        # or they are equal length but the root height confirmed by the block is longer
        update_tip = False
        if not self.__is_same_root_chain(
            self.root_tip,
            self.db.get_root_block_header_by_hash(block.header.hash_prev_root_block),
        ):
            # Don't update tip if the block depends on a root block that is not root_tip or root_tip's ancestor
            update_tip = False
        elif block.header.hash_prev_minor_block == self.header_tip.get_hash():
            update_tip = True
        elif self.__is_minor_block_linked_to_root_tip(block):
            if block.header.height > self.header_tip.height:
                update_tip = True
            elif block.header.height == self.header_tip.height:
                update_tip = (
                    self.db.get_root_block_header_by_hash(
                        block.header.hash_prev_root_block
                    ).height
                    > self.db.get_root_block_header_by_hash(
                        self.header_tip.hash_prev_root_block
                    ).height
                )

        if update_tip:
            self.__rewrite_block_index_to(block)
            self.evm_state = evm_state
            # Safe to update PoSW blacklist here
            if self.shard_config.POSW_CONFIG.ENABLED:
                disallow_list = self._get_posw_coinbase_blockcnt(block_hash).keys()
                self.evm_state.sender_disallow_list = disallow_list
            self.header_tip = block.header
            self.meta_tip = block.meta

        check(
            self.__is_same_root_chain(
                self.root_tip,
                self.db.get_root_block_header_by_hash(
                    self.header_tip.hash_prev_root_block
                ),
            )
        )
        Logger.debug(
            "Add block took {} seconds for {} tx".format(
                time.time() - start_time, len(block.tx_list)
            )
        )
        tracking_data_str = block.tracking_data.decode("utf-8")
        if tracking_data_str != "":
            tracking_data = json.loads(tracking_data_str)
            sample = {
                "time": time_ms() // 1000,
                "shard": str(block.header.branch.get_full_shard_id()),
                "network": self.env.cluster_config.MONITORING.NETWORK_NAME,
                "cluster": self.env.cluster_config.MONITORING.CLUSTER_ID,
                "hash": block_hash.hex(),
                "height": block.header.height,
                "original_cluster": tracking_data["cluster"],
                "inception": tracking_data["inception"],
                "creation_latency_ms": tracking_data["creation_ms"],
                "add_block_latency_ms": time_ms() - start_ms,
                "mined": tracking_data.get("mined", 0),
                "propagation_latency_ms": start_ms - tracking_data.get("mined", 0),
                "num_tx": len(block.tx_list),
            }
            asyncio.ensure_future(
                self.env.cluster_config.kafka_logger.log_kafka_sample_async(
                    self.env.cluster_config.MONITORING.PROPAGATION_TOPIC, sample
                )
            )
        return evm_state.xshard_list, coinbase_amount_map

    def get_coinbase_amount_map(self, height) -> TokenBalanceMap:
        epoch = (
            height
            // self.env.quark_chain_config.shards[self.full_shard_id].EPOCH_INTERVAL
        )
        decay_numerator = (
            self.env.quark_chain_config.block_reward_decay_factor.numerator ** epoch
        )
        decay_denominator = (
            self.env.quark_chain_config.block_reward_decay_factor.denominator ** epoch
        )
        coinbase_amount = (
            self.env.quark_chain_config.shards[self.full_shard_id].COINBASE_AMOUNT
            * self.local_fee_rate.numerator
            * decay_numerator
            // self.local_fee_rate.denominator
            // decay_denominator
        )
        # shard coinbase only in genesis_token
        return TokenBalanceMap(
            {self.env.quark_chain_config.genesis_token: coinbase_amount}
        )

    def get_tip(self) -> MinorBlock:
        return self.db.get_minor_block_by_hash(self.header_tip.get_hash())

    def finalize_and_add_block(self, block, gas_limit=None, xshard_gas_limit=None):
        """ Finalize the block by filling post-tx data including tx fee collected
        gas_limit and xshard_gas_limit is used to verify customized gas limits and they are for test purpose only
        """
        evm_state = self.run_block(block)
        coinbase_amount_map = self.get_coinbase_amount_map(block.header.height)
        coinbase_amount_map.add(evm_state.block_fee_tokens)
        block.finalize(evm_state=evm_state, coinbase_amount_map=coinbase_amount_map)
        self.add_block(block, gas_limit=gas_limit, xshard_gas_limit=xshard_gas_limit)

    def get_token_balance(
        self, recipient: bytes, token_id: int, height: Optional[int] = None
    ) -> int:
        evm_state = self._get_evm_state_from_height(height)
        if not evm_state:
            return 0
        return evm_state.get_balance(recipient, token_id=token_id)

    def get_balances(self, recipient: bytes, height: Optional[int] = None) -> dict:
        evm_state = self._get_evm_state_from_height(height)
        if not evm_state:
            return {}
        return evm_state.get_balances(recipient)

    def get_transaction_count(
        self, recipient: bytes, height: Optional[int] = None
    ) -> int:
        evm_state = self._get_evm_state_from_height(height)
        if not evm_state:
            return 0
        return evm_state.get_nonce(recipient)

    def get_code(self, recipient: bytes, height: Optional[int] = None) -> bytes:
        evm_state = self._get_evm_state_from_height(height)
        if not evm_state:
            return b""
        return evm_state.get_code(recipient)

    def get_storage_at(
        self, recipient: bytes, key: int, height: Optional[int] = None
    ) -> bytes:
        evm_state = self._get_evm_state_from_height(height)
        if not evm_state:
            return b""
        int_result = evm_state.get_storage_data(recipient, key)  # type: int
        return int_result.to_bytes(32, byteorder="big")

    def execute_tx(
        self, tx: TypedTransaction, from_address, height: Optional[int] = None
    ) -> Optional[bytes]:
        """Execute the tx using a copy of state
        """
        evm_state = self._get_evm_state_from_height(height)
        if not evm_state:
            return None

        state = evm_state.ephemeral_clone()
        state.gas_used = 0

        # Use the maximum gas allowed if gas is 0
        evm_tx = tx.tx.to_evm_tx()
        gas = evm_tx.startgas if evm_tx.startgas else state.gas_limit

        try:
            evm_tx = self.__validate_tx(tx, state, from_address, gas)
            success, output = apply_transaction(
                state, evm_tx, tx_wrapper_hash=bytes(32)
            )
            return output if success else None
        except Exception as e:
            Logger.warning_every_sec("Failed to apply transaction: {}".format(e), 1)
            return None

    def get_next_block_difficulty(self, create_time=None):
        if not create_time:
            create_time = max(int(time.time()), self.header_tip.create_time + 1)
        return self.diff_calc.calculate_diff_with_parent(self.header_tip, create_time)

    def get_next_block_reward(self):
        return self.reward_calc.get_block_reward(self)

    def get_next_block_coinbase_amount(self):
        # TODO: add block reward
        # TODO: the current calculation is bogus and just serves as a placeholder.
        coinbase = 0
        for tx_wrapper in self.tx_queue.peek():
            tx = tx_wrapper.tx
            coinbase += tx.gasprice * tx.startgas

        # TODO: add x-shard tx
        return coinbase

    def __get_all_unconfirmed_header_list(self) -> List[MinorBlockHeader]:
        """ height in ascending order """
        header_list = []
        header = self.header_tip
        start_height = (
            self.confirmed_header_tip.height if self.confirmed_header_tip else -1
        )
        for i in range(header.height - start_height):
            header_list.append(header)
            header = self.db.get_minor_block_header_by_hash(
                header.hash_prev_minor_block
            )
        check(header == self.confirmed_header_tip)
        header_list.reverse()
        return header_list

    def get_unconfirmed_header_list(self) -> List[MinorBlockHeader]:
        headers = self.__get_all_unconfirmed_header_list()
        max_blocks = self.__get_max_blocks_in_one_root_block()
        return headers[0:max_blocks]

    def get_unconfirmed_headers_coinbase_amount(self) -> int:
        """ only returns genesis token coinbase amount
        TODO remove coinbase_amount_map from minor header, this is the ONLY place that requires it
        """
        amount = 0
        headers = self.get_unconfirmed_header_list()
        for header in headers:
            amount += header.coinbase_amount_map.balance_map.get(
                self.env.quark_chain_config.genesis_token, 0
            )
        return amount

    def __get_max_blocks_in_one_root_block(self) -> int:
        return self.shard_config.max_blocks_per_shard_in_one_root_block

    def __add_transactions_to_block(self, block: MinorBlock, evm_state: EvmState):
        """ Fill up the block tx list with tx from the tx queue"""
        poped_txs = []

        while evm_state.gas_used < evm_state.gas_limit:
            evm_tx = self.tx_queue.pop_transaction(
                max_gas=evm_state.gas_limit - evm_state.gas_used
            )
            if evm_tx is None:  # tx_queue is exhausted
                break

            evm_tx.set_quark_chain_config(self.env.quark_chain_config)
            to_branch = Branch(evm_tx.to_full_shard_id)

            tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
            try:
                apply_transaction(evm_state, evm_tx, tx.get_hash())
                block.add_tx(tx)
                poped_txs.append(evm_tx)
            except Exception as e:
                Logger.warning_every_sec(
                    "Failed to include transaction: {}".format(e), 1
                )
                self.tx_dict.pop(tx.get_hash(), None)

        # We don't want to drop the transactions if the mined block failed to be appended
        for evm_tx in poped_txs:
            self.tx_queue.add_transaction(evm_tx)

    def create_block_to_mine(
        self,
        create_time=None,
        address=None,
        gas_limit=None,
        xshard_gas_limit=None,
        include_tx=True,
    ):
        """ Create a block to append and include TXs to maximize rewards
        """
        start_time = time.time()
        tracking_data = {
            "inception": time_ms(),
            "cluster": self.env.cluster_config.MONITORING.CLUSTER_ID,
        }
        if not create_time:
            create_time = max(int(time.time()), self.header_tip.create_time + 1)
        difficulty = self.get_next_block_difficulty(create_time)
        prev_block = self.get_tip()
        block = prev_block.create_block_to_append(
            create_time=create_time, address=address, difficulty=difficulty
        )

        # Add corrected gas limit
        # Set gas_limit.  Since gas limit is fixed between blocks, this is for test purpose only.
        gas_limit, xshard_gas_limit = self.get_gas_limit_all(
            gas_limit, xshard_gas_limit
        )
        block.header.evm_gas_limit = gas_limit
        block.meta.evm_xshard_gas_limit = xshard_gas_limit
        evm_state = self._get_evm_state_for_new_block(block)

        # Cross-shard receive must be handled before including tx from tx_queue
        # This is part of consensus.
        block.header.hash_prev_root_block = self.root_tip.get_hash()
        xtx_list, evm_state.xshard_tx_cursor_info = self.__run_cross_shard_tx_with_cursor(
            evm_state=evm_state, mblock=block
        )

        # Adjust inshard tx limit if xshard gas limit is not exhausted
        if evm_state.gas_used < xshard_gas_limit:
            evm_state.gas_limit -= xshard_gas_limit - evm_state.gas_used

        if include_tx:
            self.__add_transactions_to_block(block, evm_state)

        # Pay miner
        pure_coinbase_amount = self.get_coinbase_amount_map(block.header.height)
        for k, v in pure_coinbase_amount.balance_map.items():
            evm_state.delta_token_balance(evm_state.block_coinbase, k, v)

        # Update actual root hash
        evm_state.commit()

        pure_coinbase_amount.add(evm_state.block_fee_tokens)
        block.finalize(evm_state=evm_state, coinbase_amount_map=pure_coinbase_amount)

        tracking_data["creation_ms"] = time_ms() - tracking_data["inception"]
        block.tracking_data = json.dumps(tracking_data).encode("utf-8")
        end_time = time.time()
        Logger.debug(
            "Create block to mine took {} seconds for {} tx".format(
                end_time - start_time, len(block.tx_list)
            )
        )
        return block

    def get_block_by_hash(self, h):
        """ Return an validated block.  Return None if no such block exists in db
        """
        return self.db.get_minor_block_by_hash(h)

    def contain_block_by_hash(self, h):
        return self.db.contain_minor_block_by_hash(h)

    def get_pending_tx_size(self):
        return self.transaction_pool.size()

    #
    # ============================ Cross-shard transaction handling =============================
    #
    def add_cross_shard_tx_list_by_minor_block_hash(
        self, h, tx_list: CrossShardTransactionList
    ):
        """ Add a cross shard tx list from remote shard
        The list should be validated by remote shard, however,
        it is better to diagnose some bugs in peer shard if we could check
        - x-shard gas limit exceeded
        - it is a neighor of current shard following our routing rule
        """
        self.db.put_minor_block_xshard_tx_list(h, tx_list)

    def add_root_block(self, root_block: RootBlock):
        """ Add a root block.
        Make sure all cross shard tx lists of remote shards confirmed by the root block are in local db.
        Return True if the new block become head else False.
        Raise ValueError on any failure.
        """
        check(
            root_block.header.height
            > self.env.quark_chain_config.get_genesis_root_height(self.full_shard_id)
        )
        if not self.db.contain_root_block_by_hash(root_block.header.hash_prev_block):
            raise ValueError("cannot find previous root block in pool")

        shard_headers = []
        for m_header in root_block.minor_block_header_list:
            h = m_header.get_hash()
            if m_header.branch == self.branch:
                if not self.db.contain_minor_block_by_hash(h):
                    raise ValueError("cannot find minor block in local shard")
                shard_headers.append(m_header)
                continue

            prev_root_header = self.db.get_root_block_header_by_hash(
                m_header.hash_prev_root_block
            )
            # prev_root_header can be None when the shard is not created at root height 0
            if (
                not prev_root_header
                or prev_root_header.height
                == self.env.quark_chain_config.get_genesis_root_height(
                    self.full_shard_id
                )
                or not self._is_neighbor(m_header.branch, prev_root_header.height)
            ):
                check(
                    not self.db.contain_remote_minor_block_hash(h),
                    "minor block {} {} from shard {} shouldn't have been broadcasted to shard {}".format(
                        m_header.height,
                        m_header.get_hash().hex(),
                        m_header.branch.get_full_shard_id(),
                        self.branch.get_full_shard_id(),
                    ),
                )
                continue

            check(
                self.db.contain_remote_minor_block_hash(h),
                "cannot find x_shard tx list for {}-{} {}".format(
                    m_header.branch.get_full_shard_id(), m_header.height, h.hex()
                ),
            )

        if len(shard_headers) > self.__get_max_blocks_in_one_root_block():
            raise ValueError(
                "too many minor blocks in the root block for shard {}".format(
                    self.branch.get_full_shard_id()
                )
            )

        last_minor_header_in_prev_root_block = self.db.get_last_confirmed_minor_block_header_at_root_block(
            root_block.header.hash_prev_block
        )
        if shard_headers:
            # Master should assure this check will not fail
            check(
                shard_headers[0].height == 0
                or shard_headers[0].hash_prev_minor_block
                == last_minor_header_in_prev_root_block.get_hash()
            )
            shard_header = shard_headers[-1]
        else:
            shard_header = last_minor_header_in_prev_root_block

        # shard_header can be None meaning the genesis shard block has not been confirmed by any root block

        self.db.put_root_block(root_block, shard_header)
        if shard_header:
            check(
                self.__is_same_root_chain(
                    root_block.header,
                    self.db.get_root_block_header_by_hash(
                        shard_header.hash_prev_root_block
                    ),
                )
            )

        # No change to root tip
        if root_block.header.height <= self.root_tip.height:
            check(
                self.__is_same_root_chain(
                    self.root_tip,
                    self.db.get_root_block_header_by_hash(
                        self.header_tip.hash_prev_root_block
                    ),
                )
            )
            return False

        # Switch to the longest root block
        self.root_tip = root_block.header
        self.confirmed_header_tip = shard_header

        orig_header_tip = self.header_tip
        if shard_header:
            orig_block = self.db.get_minor_block_by_height(shard_header.height)
            # get_minor_block_by_height only returns block on the best chain
            # so orig_block could be on a fork and thus will not be found by
            # get_minor_block_by_height
            if not orig_block or orig_block.header != shard_header:
                # TODO: shard_header might not be the tip of the longest chain
                # need to switch to the tip of the longest chain
                self.header_tip = shard_header

        # the current header_tip might point to a root block on a fork with r_block
        # we need to scan back until finding a minor block pointing to the same root chain r_block is on.
        # the worst case would be that we go all the way back to orig_block (shard_header)
        while not self.__is_same_root_chain(
            self.root_tip,
            self.db.get_root_block_header_by_hash(self.header_tip.hash_prev_root_block),
        ):
            if self.header_tip.height == 0:
                # we are at genesis block now but the root block it points to is still on a fork from root_tip.
                # we have to reset the genesis block based on the root chain identified by root_tip
                genesis_root_header = self.root_tip
                genesis_height = self.env.quark_chain_config.get_genesis_root_height(
                    self.full_shard_id
                )
                check(genesis_root_header.height >= genesis_height)
                # first find the root block at genesis root height
                while genesis_root_header.height != genesis_height:
                    genesis_root_header = self.db.get_root_block_header_by_hash(
                        genesis_root_header.hash_prev_block
                    )
                    check(genesis_root_header is not None)
                # recover the genesis block
                self.header_tip = self.db.get_genesis_block(
                    genesis_root_header.get_hash()
                ).header
                check(self.header_tip is not None)
                break

            self.header_tip = self.db.get_minor_block_header_by_hash(
                self.header_tip.hash_prev_minor_block
            )

        if self.header_tip != orig_header_tip:
            header_tip_hash = self.header_tip.get_hash()
            self.meta_tip = self.db.get_minor_block_meta_by_hash(header_tip_hash)
            self.__rewrite_block_index_to(
                self.db.get_minor_block_by_hash(header_tip_hash)
            )
            Logger.info(
                "[{}] shard tip reset from {} to {} by root block {}".format(
                    self.branch.to_str(),
                    orig_header_tip.height,
                    self.header_tip.height,
                    root_block.header.height,
                )
            )

        return True

    def _is_neighbor(self, remote_branch: Branch, root_height=None):
        root_height = self.root_tip.height if root_height is None else root_height
        shard_size = len(
            self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
                root_height
            )
        )
        return is_neighbor(self.branch, remote_branch, shard_size)

    def __run_one_xshard_tx(self, evm_state, xshard_deposit_tx):
        tx = xshard_deposit_tx
        # TODO: Check if target address is a smart contract address or user address
        evm_state.delta_token_balance(
            tx.to_address.recipient, tx.transfer_token_id, tx.value
        )
        evm_state.gas_used = evm_state.gas_used + (
            opcodes.GTXXSHARDCOST if tx.gas_price != 0 else 0
        )
        check(evm_state.gas_used <= evm_state.gas_limit)

        xshard_fee = (
            opcodes.GTXXSHARDCOST
            * tx.gas_price
            * self.local_fee_rate.numerator
            // self.local_fee_rate.denominator
        )
        add_dict(evm_state.block_fee_tokens, {tx.gas_token_id: xshard_fee})
        evm_state.delta_token_balance(
            evm_state.block_coinbase, tx.gas_token_id, xshard_fee
        )

    def __run_cross_shard_tx_with_cursor(self, evm_state, mblock):
        cursor_info = self.db.get_minor_block_meta_by_hash(
            mblock.header.hash_prev_minor_block
        ).xshard_tx_cursor_info
        cursor = XshardTxCursor(self, mblock.header, cursor_info)
        tx_list = []

        while True:
            xshard_deposit_tx = cursor.get_next_tx()
            if xshard_deposit_tx is None:
                # EOF
                break

            tx_list.append(xshard_deposit_tx)

            self.__run_one_xshard_tx(evm_state, xshard_deposit_tx)

            # Impose soft-limit of xshard gas limit
            if evm_state.gas_used >= mblock.meta.evm_xshard_gas_limit:
                break

        evm_state.xshard_receive_gas_used = evm_state.gas_used
        return tx_list, cursor.get_cursor_info()

    def contain_remote_minor_block_hash(self, h):
        return self.db.contain_remote_minor_block_hash(h)

    def get_transaction_by_hash(self, h):
        """ Returns (block, index) where index is the position of tx in the block """
        block, index = self.db.get_transaction_by_hash(h)
        if block:
            return block, index
        if h in self.tx_dict:
            block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            block.tx_list.append(self.tx_dict[h])
            return block, 0
        return None, None

    def get_transaction_receipt(
        self, h
    ) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        block, index = self.db.get_transaction_by_hash(h)
        if not block:
            return None
        receipt = block.get_receipt(self.evm_state.db, index)
        if receipt.contract_address != Address.create_empty_account(0):
            address = receipt.contract_address
            check(
                address.full_shard_key
                == self.evm_state.get_full_shard_key(address.recipient)
            )
        return block, index, receipt

    def get_transaction_list_by_address(self, address, start, limit):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        if start == bytes(1):  # get pending tx
            tx_list = []
            for orderable_tx in self.tx_queue.txs + self.tx_queue.aside:
                tx = orderable_tx.tx
                if Address(tx.sender, tx.from_full_shard_key) == address:
                    tx_list.append(
                        TransactionDetail(
                            TypedTransaction(
                                SerializedEvmTransaction.from_evm_tx(tx)
                            ).get_hash(),
                            address,
                            Address(tx.to, tx.to_full_shard_key) if tx.to else None,
                            tx.value,
                            block_height=0,
                            timestamp=0,
                            success=False,
                            gas_token_id=tx.gas_token_id,
                            transfer_token_id=tx.transfer_token_id,
                        )
                    )
            return tx_list, b""

        return self.db.get_transactions_by_address(address, start, limit)

    def get_shard_stats(self) -> ShardStats:
        cutoff = self.header_tip.create_time - 60
        block = self.db.get_minor_block_by_hash(self.header_tip.get_hash())
        tx_count = 0
        block_count = 0
        stale_block_count = 0
        last_block_time = 0
        while block.header.height > 0 and block.header.create_time > cutoff:
            tx_count += len(block.tx_list)
            block_count += 1
            stale_block_count += max(
                0, (self.db.get_block_count_by_height(block.header.height) - 1)
            )
            block = self.db.get_minor_block_by_hash(block.header.hash_prev_minor_block)
            if last_block_time == 0:
                last_block_time = self.header_tip.create_time - block.header.create_time

        check(stale_block_count >= 0)
        return ShardStats(
            branch=self.branch,
            height=self.header_tip.height,
            difficulty=self.header_tip.difficulty,
            coinbase_address=self.header_tip.coinbase_address,
            timestamp=self.header_tip.create_time,
            tx_count60s=tx_count,
            pending_tx_count=len(self.tx_queue),
            total_tx_count=self.db.get_total_tx_count(self.header_tip.get_hash()),
            block_count60s=block_count,
            stale_block_count60s=stale_block_count,
            last_block_time=last_block_time,
        )

    def get_logs(
        self,
        addresses: List[Address],
        topics: List[Optional[Union[str, List[str]]]],
        start_block: int,
        end_block: int,
    ) -> Optional[List[Log]]:
        if addresses and (
            len(set(addr.full_shard_key for addr in addresses)) != 1
            or self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                addresses[0].full_shard_key
            )
            != self.full_shard_id
        ):
            # should have the same full_shard_id for the given addresses
            return None

        log_filter = Filter(self.db, addresses, topics, start_block, end_block)

        try:
            logs = log_filter.run()
            return logs
        except Exception as e:
            Logger.error_exception()
            return None

    def estimate_gas(self, tx: TypedTransaction, from_address) -> Optional[int]:
        """Estimate a tx's gas usage by binary searching."""
        evm_tx_start_gas = tx.tx.to_evm_tx().startgas
        # binary search. similar as in go-ethereum
        lo = 21000 - 1
        hi = evm_tx_start_gas if evm_tx_start_gas > 21000 else self.evm_state.gas_limit
        cap = hi

        def run_tx(gas):
            try:
                evm_state = self.evm_state.ephemeral_clone()  # type: EvmState
                evm_state.gas_used = 0
                evm_tx = self.__validate_tx(tx, evm_state, from_address, gas=gas)
                success, _ = apply_transaction(
                    evm_state, evm_tx, tx_wrapper_hash=bytes(32)
                )
                return success
            except Exception:
                return False

        while lo + 1 < hi:
            mid = (lo + hi) // 2
            if run_tx(mid):
                hi = mid
            else:
                lo = mid

        if hi == cap and not run_tx(hi):
            return None
        return hi

    def gas_price(self) -> Optional[int]:
        curr_head = self.header_tip.get_hash()
        if curr_head == self.gas_price_suggestion_oracle.last_head:
            return self.gas_price_suggestion_oracle.last_price
        curr_height = self.header_tip.height
        start_height = curr_height - self.gas_price_suggestion_oracle.check_blocks + 1
        if start_height < 3:
            start_height = 3
        prices = []
        for i in range(start_height, curr_height + 1):
            block = self.db.get_minor_block_by_height(i)
            if not block:
                Logger.error("Failed to get block {} to retrieve gas price".format(i))
                continue
            prices.extend(block.get_block_prices())
        if not prices:
            return None
        prices.sort()
        price = prices[
            (len(prices) - 1) * self.gas_price_suggestion_oracle.percentile // 100
        ]
        self.gas_price_suggestion_oracle.last_price = price
        self.gas_price_suggestion_oracle.last_head = curr_head
        return price

    def validate_minor_block_seal(self, block: MinorBlock):
        consensus_type = self.env.quark_chain_config.shards[
            block.header.branch.get_full_shard_id()
        ].CONSENSUS_TYPE
        if not self.shard_config.POSW_CONFIG.ENABLED:
            validate_seal(block.header, consensus_type)
        else:
            diff = self.posw_diff_adjust(block)
            validate_seal(block.header, consensus_type, adjusted_diff=diff)

    def posw_diff_adjust(self, block: MinorBlock) -> int:
        start_time = time.time()
        header = block.header
        diff = header.difficulty
        coinbase_address = header.coinbase_address.recipient
        # Evaluate stakes before the to-be-added block
        evm_state = self._get_evm_state_for_new_block(block, ephemeral=True)
        config = self.shard_config.POSW_CONFIG
        stakes = evm_state.get_balance(
            coinbase_address, self.env.quark_chain_config.genesis_token
        )
        block_threshold = stakes // config.TOTAL_STAKE_PER_BLOCK
        block_threshold = min(config.WINDOW_SIZE, block_threshold)
        # The func is inclusive, so need to fetch block counts until prev block
        # Also only fetch prev window_size - 1 block counts because the
        # new window should count the current block
        block_cnt = self._get_posw_coinbase_blockcnt(
            header.hash_prev_minor_block, length=config.WINDOW_SIZE - 1
        )
        cnt = block_cnt.get(coinbase_address, 0)
        if cnt < block_threshold:
            diff //= config.DIFF_DIVIDER
        # TODO: remove it if verified not time consuming
        passed_ms = (time.time() - start_time) * 1000
        Logger.debug("Adjust PoSW diff took %s milliseconds" % passed_ms)
        return diff

    def _get_evm_state_from_height(self, height: Optional[int]) -> Optional[EvmState]:
        if height is None or height == self.header_tip.height:
            return self.evm_state

        # note `_get_evm_state_for_new_block` actually fetches the state in the previous block
        # so adding 1 is needed here to get the next block
        block = self.db.get_minor_block_by_height(height + 1)
        if not block:
            Logger.error("Failed to get block at height {}".format(height))
            return None
        return self._get_evm_state_for_new_block(block)

    def __get_coinbase_addresses_until_block(
        self, header_hash: bytes, length: int
    ) -> List[bytes]:
        """Get coinbase addresses up until block of given hash within the window."""
        curr_block = self.db.get_minor_block_by_hash(header_hash)
        if not curr_block:
            raise ValueError("curr block not found: hash {}".format(header_hash.hex()))
        header = curr_block.header
        height = header.height
        prev_hash = header.hash_prev_minor_block
        if prev_hash in self.coinbase_addr_cache:  # mem cache hit
            _, addrs = self.coinbase_addr_cache[prev_hash]
            addrs = addrs.copy()
            if len(addrs) == length:
                addrs.popleft()
            addrs.append(header.coinbase_address.recipient)
        else:  # miss, iterating DB
            addrs = deque()
            for _ in range(length):
                addrs.appendleft(header.coinbase_address.recipient)
                if header.height == 0:
                    break
                header = self.db.get_minor_block_header_by_hash(
                    header.hash_prev_minor_block
                )
                check(header is not None, "mysteriously missing block")
        self.coinbase_addr_cache[header_hash] = (height, addrs)
        # in case cached too much, clean up
        if len(self.coinbase_addr_cache) > 128:  # size around 640KB if window size 256
            self.coinbase_addr_cache = {
                k: (h, addrs)
                for k, (h, addrs) in self.coinbase_addr_cache.items()
                if h > height - 16  # keep most recent ones
            }
        return list(addrs)

    @functools.lru_cache(maxsize=16)
    def _get_posw_coinbase_blockcnt(
        self, header_hash: bytes, length: int = None
    ) -> Dict[bytes, int]:
        """ PoSW needed function: get coinbase addresses up until the given block
        hash (inclusive) along with block counts within the PoSW window.

        Raise ValueError if anything goes wrong.
        """
        if length is None:
            length = self.shard_config.POSW_CONFIG.WINDOW_SIZE
        coinbase_addrs = self.__get_coinbase_addresses_until_block(header_hash, length)
        return Counter(coinbase_addrs)
