import asyncio
import json
import time
from fractions import Fraction
from typing import Dict, List, Optional, Tuple, Union

from rlp import DecodingError

from quarkchain import utils
from quarkchain.cluster.log_filter import LogFilter
from quarkchain.cluster.miner import validate_seal
from quarkchain.cluster.neighbor import is_neighbor
from quarkchain.cluster.rpc import ShardStats, TransactionDetail
from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.cluster.subscription import SubscriptionManager
from quarkchain.core import (
    Address,
    Branch,
    CrossShardTransactionDeposit,
    CrossShardTransactionList,
    Log,
    MinorBlock,
    MinorBlockHeader,
    MinorBlockMeta,
    PoSWInfo,
    RootBlock,
    TokenBalanceMap,
    TransactionReceipt,
    TypedTransaction,
    XshardTxCursorInfo,
    calculate_merkle_root,
    mk_receipt_sha,
)
from quarkchain.constants import ALLOWED_FUTURE_BLOCKS_TIME_VALIDATION
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.evm import opcodes
from quarkchain.evm.messages import (
    apply_transaction,
    validate_transaction,
    apply_xshard_deposit,
    convert_to_default_chain_token_gasprice,
)
from quarkchain.evm.specials import SystemContract
from quarkchain.evm.state import State as EvmState
from quarkchain.evm.transaction_queue import TransactionQueue
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.evm.utils import add_dict
from quarkchain.genesis import GenesisManager
from quarkchain.cluster.posw import get_posw_coinbase_blockcnt, get_posw_info
from quarkchain.reward import ConstMinorBlockRewardCalcultor
from quarkchain.utils import Logger, check, time_ms
from cachetools import LRUCache
from quarkchain.config import ConsensusType

MAX_FUTURE_TX_NONCE = 64


class GasPriceSuggestionOracle:
    def __init__(self, check_blocks: int, percentile: int):
        self.cache = LRUCache(maxsize=128)
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
            # TODO: for single native token only
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
                    # Use root block hash as identifier.
                    tx_hash=self.rblock.header.get_hash(),
                    from_address=self.rblock.header.coinbase_address,
                    to_address=self.rblock.header.coinbase_address,
                    value=coinbase_amount,
                    gas_price=0,
                    gas_token_id=self.shard_state.genesis_token_id,
                    transfer_token_id=self.shard_state.genesis_token_id,
                    is_from_root_chain=True,
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
        self.tx_queue = TransactionQueue(
            env.quark_chain_config.TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD
        )
        self.initialized = False
        self.header_tip = None  # type: Optional[MinorBlockHeader]
        # TODO: make the oracle configurable
        self.gas_price_suggestion_oracle = GasPriceSuggestionOracle(
            check_blocks=5, percentile=50
        )

        # new blocks that passed POW validation and should be made available to whole network
        self.new_block_header_pool = dict()
        # header hash -> [coinbase address] during previous blocks (ascending)
        self.coinbase_addr_cache = LRUCache(maxsize=128)
        self.genesis_token_id = self.env.quark_chain_config.genesis_token
        self.local_fee_rate = (
            1 - self.env.quark_chain_config.reward_tax_rate
        )  # type: Fraction
        self.subscription_manager = SubscriptionManager()

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
        sender_disallow_map = self._get_sender_disallow_map(header_tip)
        self.evm_state = self.__create_evm_state(
            self.meta_tip.hash_evm_state_root, sender_disallow_map, int(time.time())
        )
        check(
            self.db.get_minor_block_evm_root_hash_by_hash(header_tip_hash)
            == self.meta_tip.hash_evm_state_root
        )

        self.__rewrite_block_index_to(
            self.db.get_minor_block_by_hash(header_tip_hash), add_tx_back_to_queue=False
        )

    def __create_evm_state(
        self,
        trie_root_hash: Optional[bytes],
        sender_disallow_map: Dict[bytes, int],
        timestamp: Optional[int] = None,
    ):
        """EVM state with given root hash and block hash AFTER which being evaluated."""
        state = EvmState(
            env=self.env.evm_env, db=self.raw_db, qkc_config=self.env.quark_chain_config
        )
        state.shard_config = self.shard_config
        if trie_root_hash:
            state.trie.root_hash = trie_root_hash
        state.sender_disallow_map = sender_disallow_map
        if timestamp:
            state.timestamp = timestamp
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
            self.__create_evm_state(trie_root_hash=None, sender_disallow_map={}),
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
            genesis_block.meta.hash_evm_state_root, sender_disallow_map={}
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

        # usually from call / estimateGas
        if from_address:
            nonce = evm_state.get_nonce(from_address.recipient)
            # have to create a new evm_tx as nonce is immutable
            evm_tx = EvmTransaction(
                nonce,
                evm_tx.gasprice,
                gas if gas else evm_tx.startgas,  # override gas if specified
                evm_tx.to,
                evm_tx.value,
                evm_tx.data,
                from_full_shard_key=from_address.full_shard_key,
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

        # check if TX is disabled
        if (
            self.env.quark_chain_config.ENABLE_TX_TIMESTAMP is not None
            and evm_state.timestamp < self.env.quark_chain_config.ENABLE_TX_TIMESTAMP
        ):
            if evm_tx.sender not in self.env.quark_chain_config.tx_whitelist_senders:
                raise RuntimeError(
                    "unwhitelisted senders not allowed before tx is enabled"
                )

        # Check if EVM is disabled
        if (
            self.env.quark_chain_config.ENABLE_EVM_TIMESTAMP is not None
            and evm_state.timestamp < self.env.quark_chain_config.ENABLE_EVM_TIMESTAMP
        ):
            if evm_tx.to == b"" or evm_tx.data != b"":
                raise RuntimeError(
                    "smart contract tx is not allowed before evm is enabled"
                )

        req_nonce = evm_state.get_nonce(evm_tx.sender)
        if req_nonce < evm_tx.nonce <= req_nonce + MAX_FUTURE_TX_NONCE:
            return evm_tx

        # This will check signature, nonce, balance, gas limit. Skip if nonce not strictly incremental
        validate_transaction(evm_state, evm_tx)
        return evm_tx

    def get_gas_limit(self, gas_limit=None):
        if gas_limit is None:
            gas_limit = self.env.quark_chain_config.gas_limit(self.full_shard_id)
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

        if tx_hash in self.tx_queue:
            return False

        evm_state = self.evm_state.ephemeral_clone()
        evm_state.gas_used = 0
        try:
            evm_tx = self.__validate_tx(
                tx, evm_state, xshard_gas_limit=xshard_gas_limit
            )

            # Don't add the tx if the gasprice in QKC is too low.
            # Note that this is not enforced by consensus,
            # but miners will likely discard the tx if the gasprice is too low.
            default_gasprice = convert_to_default_chain_token_gasprice(
                evm_state, evm_tx.gas_token_id, evm_tx.gasprice
            )
            if default_gasprice < self.env.quark_chain_config.MIN_TX_POOL_GAS_PRICE:
                return False

            self.tx_queue.add_transaction(tx)
            asyncio.ensure_future(
                self.subscription_manager.notify_new_pending_tx(
                    [tx_hash + evm_tx.from_full_shard_key.to_bytes(4, byteorder="big")]
                )
            )
            return True
        except Exception as e:
            Logger.warning_every_sec("Failed to add transaction: {}".format(e), 1)
            return False

    def _get_evm_state_for_new_block(self, block, ephemeral=True):
        prev_minor_hash = block.header.hash_prev_minor_block
        prev_minor_header = self.db.get_minor_block_header_by_hash(prev_minor_hash)
        root_hash = self.db.get_minor_block_evm_root_hash_by_hash(prev_minor_hash)
        coinbase_recipient = block.header.coinbase_address.recipient
        sender_disallow_map = self._get_sender_disallow_map(
            prev_minor_header, recipient=coinbase_recipient
        )

        state = self.__create_evm_state(
            root_hash, sender_disallow_map, block.header.create_time
        )
        if ephemeral:
            state = state.ephemeral_clone()
        state.gas_limit = block.header.evm_gas_limit
        state.block_number = block.header.height
        state.recent_uncles[
            state.block_number
        ] = []  # TODO [x.hash for x in block.uncles]
        # TODO: create a account with shard info if the account is not created
        # Right now the full_shard_key for coinbase actually comes from the first tx that got applied
        state.block_coinbase = coinbase_recipient
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

    def validate_block(self, block: MinorBlock, gas_limit=None, xshard_gas_limit=None):
        """ Validate a block before running evm transactions
        """
        if block.header.version != 0:
            raise ValueError("incorrect minor block version")

        height = block.header.height
        if height < 1:
            raise ValueError("unexpected height")

        if not self.db.contain_minor_block_by_hash(block.header.hash_prev_minor_block):
            # TODO:  may put the block back to queue
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

        if (
            block.header.create_time
            > time_ms() // 1000 + ALLOWED_FUTURE_BLOCKS_TIME_VALIDATION
        ):
            raise ValueError("block too far into future")

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
        self.validate_diff_match_prev(block.header, prev_header)

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
        if not self.env.quark_chain_config.DISABLE_POW_CHECK:
            self.validate_minor_block_seal(block)

    def validate_diff_match_prev(self, curr_header, prev_header):
        if not self.env.quark_chain_config.SKIP_MINOR_DIFFICULTY_CHECK:
            diff = self.diff_calc.calculate_diff_with_parent(
                prev_header, curr_header.create_time
            )
            if diff != curr_header.difficulty:
                raise ValueError("incorrect difficulty")

    def run_block(self, block, evm_state=None, x_shard_receive_tx_list=None):
        if x_shard_receive_tx_list is None:
            x_shard_receive_tx_list = []
        if evm_state is None:
            evm_state = self._get_evm_state_for_new_block(block, ephemeral=False)
        (
            xtx_list,
            evm_state.xshard_tx_cursor_info,
        ) = self.__run_cross_shard_tx_with_cursor(evm_state=evm_state, mblock=block)
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
        if len(old_chain) > 0:
            asyncio.ensure_future(self.subscription_manager.notify_log(old_chain, True))
        for block in new_chain:
            self.db.put_transaction_index_from_block(block)
            self.db.put_minor_block_index(block)
            self.__remove_transactions_from_block(block)
        # new_chain has at least one block, starting from minor_block with block height descending
        asyncio.ensure_future(
            self.subscription_manager.notify_new_heads(
                sorted(new_chain, key=lambda x: x.header.height)
            )
        )
        asyncio.ensure_future(self.subscription_manager.notify_log(new_chain))

    # will be called for chain reorganization
    def __add_transactions_from_block(self, block):
        tx_hashes = []
        for tx in block.tx_list:
            evm_tx = tx.tx.to_evm_tx()
            tx_hash = tx.get_hash()
            self.tx_queue.add_transaction(tx)
            tx_hashes.append(
                tx_hash + evm_tx.from_full_shard_key.to_bytes(4, byteorder="big")
            )
        asyncio.ensure_future(
            self.subscription_manager.notify_new_pending_tx(tx_hashes)
        )

    def __remove_transactions_from_block(self, block):
        self.tx_queue = self.tx_queue.diff(block.tx_list)

    def add_block(
        self,
        block,
        skip_if_too_old=True,
        gas_limit=None,
        xshard_gas_limit=None,
        force=False,
        write_db=True,
    ):
        """  Add a block to local db.  Perform validate and update tip accordingly
        gas_limit and xshard_gas_limit are used for testing only.
        Returns None if block is already added (if force is False).
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
        if not force and self.db.contain_minor_block_by_hash(block_hash):
            return None, None

        x_shard_receive_tx_list = []
        # Throw exception if fail to run
        self.validate_block(
            block, gas_limit=gas_limit, xshard_gas_limit=xshard_gas_limit
        )
        evm_state = self.run_block(
            block, x_shard_receive_tx_list=x_shard_receive_tx_list
        )

        # ------------------------ Validate ending result of the block --------------------
        if evm_state.xshard_tx_cursor_info != block.meta.xshard_tx_cursor_info:
            raise ValueError("Cross-shard transaction cursor info mismatches!")

        if block.meta.hash_evm_state_root != evm_state.trie.root_hash:
            raise ValueError(
                "state root mismatch: header %s computed %s"
                % (block.meta.hash_evm_state_root.hex(), evm_state.trie.root_hash.hex())
            )

        receipts = evm_state.receipts[:] + evm_state.xshard_deposit_receipts[:]
        receipt_root = mk_receipt_sha(receipts, evm_state.db)
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

        if evm_state.get_bloom() != block.header.bloom:
            raise ValueError("bloom mismatch")

        if write_db:
            self.db.put_minor_block(block, x_shard_receive_tx_list)
        else:
            # Return immediately if it is not put into db
            return evm_state.xshard_list, coinbase_amount_map

        # Update tip if a block is appended or a fork is longer (with the same ancestor confirmed by root block tip)
        # or they are equal length but the root height confirmed by the block is longer
        update_tip = False
        prev_root_header = self.db.get_root_block_header_by_hash(
            block.header.hash_prev_root_block
        )
        if not prev_root_header:
            raise ValueError("missing prev root block")
        tip_prev_root_header = self.db.get_root_block_header_by_hash(
            self.header_tip.hash_prev_root_block
        )
        if not self.__is_same_root_chain(self.root_tip, prev_root_header):
            # Don't update tip if the block depends on a root block that is not root_tip or root_tip's ancestor
            update_tip = False
        elif block.header.hash_prev_minor_block == self.header_tip.get_hash():
            update_tip = True
        elif self.__is_minor_block_linked_to_root_tip(block):
            if block.header.height > self.header_tip.height:
                update_tip = True
            elif block.header.height == self.header_tip.height:
                update_tip = prev_root_header.height > tip_prev_root_header.height

        if update_tip:
            tip_prev_root_header = prev_root_header
            evm_state.sender_disallow_map = self._get_sender_disallow_map(block.header)
            self.__update_tip(block, evm_state)

        check(self.__is_same_root_chain(self.root_tip, tip_prev_root_header))
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
        coinbase_amount = (
            self.__decay_by_epoch(
                self.env.quark_chain_config.shards[self.full_shard_id].COINBASE_AMOUNT,
                height,
            )
            * self.local_fee_rate.numerator
            // self.local_fee_rate.denominator
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
        self, tx: TypedTransaction, from_address=None, height: Optional[int] = None
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

    def get_mining_info(self, recipient: bytes, token_balance: Dict[bytes, int]):
        if self._posw_enabled(self.header_tip):
            coinbase_address = Address(recipient, self.full_shard_id)
            next_block = MinorBlock(
                self.header_tip, MinorBlockMeta(), [], b""
            ).create_block_to_append(address=coinbase_address)
            stakes = token_balance.get(self.env.quark_chain_config.genesis_token, 0)
            posw_info = self._posw_info(next_block, stakes)
            if posw_info:
                return posw_info.posw_mined_blocks - 1, posw_info.posw_mineable_blocks

        block_cnt = self._get_posw_coinbase_blockcnt(self.header_tip.get_hash())
        return block_cnt.get(recipient, 0), 0

    def get_next_block_difficulty(self, create_time=None):
        if not create_time:
            create_time = max(int(time.time()), self.header_tip.create_time + 1)
        return self.diff_calc.calculate_diff_with_parent(self.header_tip, create_time)

    def get_next_block_coinbase_amount(self):
        # TODO: add block reward
        # TODO: the current calculation is bogus and just serves as a placeholder.
        coinbase = 0
        for tx_wrapper in self.tx_queue.peek():
            tx = tx_wrapper.tx.tx.to_evm_tx()
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
            tx = self.tx_queue.pop_transaction(
                req_nonce_getter=evm_state.get_nonce,
                max_gas=evm_state.gas_limit - evm_state.gas_used,
            )
            if tx is None:  # tx_queue is exhausted
                break

            evm_tx = tx.tx.to_evm_tx()
            evm_tx.set_quark_chain_config(self.env.quark_chain_config)

            default_gasprice = convert_to_default_chain_token_gasprice(
                evm_state, evm_tx.gas_token_id, evm_tx.gasprice
            )
            # simply ignore tx with lower gas price than specified
            if default_gasprice < self.env.quark_chain_config.MIN_MINING_GAS_PRICE:
                continue

            # check if TX is disabled
            if (
                self.env.quark_chain_config.ENABLE_TX_TIMESTAMP is not None
                and block.header.create_time
                < self.env.quark_chain_config.ENABLE_TX_TIMESTAMP
            ):
                if (
                    evm_tx.sender
                    not in self.env.quark_chain_config.tx_whitelist_senders
                ):
                    continue

            # Check if EVM is disabled
            if (
                self.env.quark_chain_config.ENABLE_EVM_TIMESTAMP is not None
                and block.header.create_time
                < self.env.quark_chain_config.ENABLE_EVM_TIMESTAMP
            ):
                if evm_tx.to == b"" or evm_tx.data != b"":
                    # Drop the smart contract creation tx from tx_queue
                    continue

            try:
                apply_transaction(evm_state, evm_tx, tx.get_hash())
                block.add_tx(tx)
                poped_txs.append(tx)
            except Exception as e:
                Logger.warning_every_sec(
                    "Failed to include transaction: {}".format(e), 1
                )

        # We don't want to drop the transactions if the mined block failed to be appended
        for tx in poped_txs:
            self.tx_queue.add_transaction(tx)

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
        (
            xtx_list,
            evm_state.xshard_tx_cursor_info,
        ) = self.__run_cross_shard_tx_with_cursor(evm_state=evm_state, mblock=block)

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
        tx_hashes = [
            tx.tx_hash + tx.from_address.full_shard_key.to_bytes(4, byteorder="big")
            for tx in tx_list.tx_list
        ]
        asyncio.ensure_future(
            self.subscription_manager.notify_new_pending_tx(tx_hashes)
        )

    def __update_tip(self, block, evm_state):
        self.__rewrite_block_index_to(block)
        self.evm_state = evm_state
        self.header_tip = block.header
        self.meta_tip = block.meta

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
        if root_block.header.version != 0:
            raise ValueError("incorrect root block version")

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
        if len(shard_headers) != 0:
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
        if shard_header is not None:
            prev_root_header = self.db.get_root_block_header_by_hash(
                shard_header.hash_prev_root_block
            )
            check(self.__is_same_root_chain(root_block.header, prev_root_header))

        # No change to root tip
        tip_prev_root_header = self.db.get_root_block_header_by_hash(
            self.header_tip.hash_prev_root_block
        )
        if root_block.header.total_difficulty <= self.root_tip.total_difficulty:
            check(self.__is_same_root_chain(self.root_tip, tip_prev_root_header))
            return False

        # Switch to the root block with higher total diff
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
            h = self.header_tip.get_hash()
            b = self.db.get_minor_block_by_hash(h)
            sender_disallow_map = self._get_sender_disallow_map(b.header)
            evm_state = self.__create_evm_state(
                b.meta.hash_evm_state_root, sender_disallow_map, int(time.time())
            )
            self.__update_tip(b, evm_state)
            Logger.info(
                "[{}] shard tip reset from {} to {} by root block {}".format(
                    self.branch.to_str(),
                    orig_header_tip.height,
                    self.header_tip.height,
                    root_block.header.height,
                )
            )

        return True

    def get_root_block_header_by_hash(self, h):
        return self.db.get_root_block_header_by_hash(h)

    def _is_neighbor(self, remote_branch: Branch, root_height=None):
        root_height = self.root_tip.height if root_height is None else root_height
        shard_size = len(
            self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
                root_height
            )
        )
        return is_neighbor(self.branch, remote_branch, shard_size)

    def __run_one_xshard_tx(self, evm_state, deposit, check_is_from_root_chain):
        gas_used_start = 0
        if check_is_from_root_chain:
            gas_used_start = (
                opcodes.GTXXSHARDCOST if not deposit.is_from_root_chain else 0
            )
        else:
            gas_used_start = opcodes.GTXXSHARDCOST if deposit.gas_price != 0 else 0

        if (
            self.env.quark_chain_config.ENABLE_EVM_TIMESTAMP is not None
            and evm_state.timestamp < self.env.quark_chain_config.ENABLE_EVM_TIMESTAMP
        ):
            tx = deposit
            # FIXME: full_shard_key is not set
            evm_state.delta_token_balance(
                tx.to_address.recipient, tx.transfer_token_id, tx.value
            )
            evm_state.gas_used = evm_state.gas_used + gas_used_start

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
        else:
            apply_xshard_deposit(evm_state, deposit, gas_used_start)
        check(evm_state.gas_used <= evm_state.gas_limit)

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

            self.__run_one_xshard_tx(
                evm_state,
                xshard_deposit_tx,
                cursor.rblock.header.height
                >= self.env.quark_chain_config.XSHARD_GAS_DDOS_FIX_ROOT_HEIGHT,
            )

            # impose soft-limit of xshard gas limit
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
        if h in self.tx_queue:
            block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            block.tx_list.append(self.tx_queue[h])
            return block, 0
        return None, None

    def get_transaction_receipt(
        self, h
    ) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        block, index = self.db.get_transaction_by_hash(h)
        if not block:
            return None
        try:
            deposit_hlist = None
            if index >= len(block.tx_list):
                deposit_hlist = self.db.get_xshard_deposit_hash_list(
                    block.header.get_hash()
                )
            # only provide deposit list when needed
            receipt = block.get_receipt(self.evm_state.db, index, deposit_hlist)
        except DecodingError:
            # must be a cross-shard tx at target while EVM is not enabled yet
            check(index >= len(block.tx_list))
            Logger.debug(
                "[{}] Querying xshard receipt before enabled with tx hash {}".format(
                    self.branch.to_str(), h
                )
            )
            # make a fake receipt
            receipt = TransactionReceipt.create_empty_receipt()
            receipt.success = b"\x01"
            return block, index, receipt

        if receipt.contract_address != Address.create_empty_account(0):
            address = receipt.contract_address
            if self.evm_state.account_exists(address.recipient):
                check(
                    address.full_shard_key
                    == self.evm_state.get_full_shard_key(address.recipient)
                )
        return block, index, receipt

    def get_all_transactions(self, start: bytes, limit: int):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        return self.db.get_all_transactions(start, limit)

    def get_transaction_list_by_address(
        self,
        address: Address,
        transfer_token_id: Optional[int],
        start: bytes,
        limit: int,
    ):
        if not self.env.cluster_config.ENABLE_TRANSACTION_HISTORY:
            return [], b""

        if start == bytes(1):  # get pending tx
            tx_list = []
            for orderable_tx in self.tx_queue.txs:
                tx = orderable_tx.tx  # type: TypedTransaction
                evm_tx = tx.tx.to_evm_tx()
                # TODO: could also show incoming pending tx
                if (
                    evm_tx.sender == address.recipient or evm_tx.to == address.recipient
                ) and (
                    transfer_token_id is None
                    or evm_tx.transfer_token_id == transfer_token_id
                ):
                    tx_list.append(
                        TransactionDetail(
                            tx.get_hash(),
                            Address(evm_tx.sender, evm_tx.from_full_shard_key),
                            Address(evm_tx.to, evm_tx.to_full_shard_key)
                            if evm_tx.to
                            else None,
                            evm_tx.value,
                            block_height=0,
                            timestamp=0,
                            success=False,
                            gas_token_id=evm_tx.gas_token_id,
                            transfer_token_id=evm_tx.transfer_token_id,
                            is_from_root_chain=False,
                        )
                    )
            return tx_list, b""

        return self.db.get_transactions_by_address(
            address, transfer_token_id, start, limit
        )

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

        size = end_block - start_block + 1
        end_block_header = self.db.get_minor_block_header_by_height(end_block)
        log_filter = LogFilter.create_from_end_block_header(
            self.db, addresses, topics, end_block_header, size
        )

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
                evm_tx = tx.tx.to_evm_tx()
                evm_tx.set_quark_chain_config(self.env.quark_chain_config)
                evm_state = self.evm_state.ephemeral_clone()  # type: EvmState
                # simulate applying xshard deposit at target shard for both transfer value and gas fee
                if (
                    evm_tx.is_cross_shard
                    and evm_tx.to_full_shard_id == self.full_shard_id
                ):
                    evm_state.delta_token_balance(
                        from_address.recipient, evm_tx.transfer_token_id, evm_tx.value
                    )
                    evm_state.delta_token_balance(
                        from_address.recipient,
                        evm_tx.gas_token_id,
                        evm_tx.gasprice * gas,
                    )
                evm_state.gas_used = 0
                evm_tx = self.__validate_tx(tx, evm_state, from_address, gas=gas)
                success, _ = apply_transaction(
                    evm_state, evm_tx, tx_wrapper_hash=bytes(32)
                )
                return success
            except Exception:
                return None

        while lo + 1 < hi:
            mid = (lo + hi) // 2
            if run_tx(mid):
                hi = mid
            else:
                lo = mid

        if hi == cap and not run_tx(hi):
            return None
        return hi

    def gas_price(self, token_id: int) -> Optional[int]:
        curr_head = self.header_tip.get_hash()
        if (curr_head, token_id) in self.gas_price_suggestion_oracle.cache:
            return self.gas_price_suggestion_oracle.cache[(curr_head, token_id)]
        curr_height = self.header_tip.height
        start_height = curr_height - self.gas_price_suggestion_oracle.check_blocks + 1
        if start_height < 3:
            start_height = 3
        prices = []
        for i in range(start_height, curr_height + 1):
            block = self.db.get_minor_block_by_height(i)
            if not block:
                Logger.error("Failed to get block {} to retrieve gas price".format(i))
                return
            block_prices = block.get_block_prices()
            if token_id in block_prices:
                prices.extend(block_prices[token_id])

        if not prices:
            return self.env.quark_chain_config.MIN_TX_POOL_GAS_PRICE

        prices.sort()
        price = prices[
            (len(prices) - 1) * self.gas_price_suggestion_oracle.percentile // 100
        ]
        self.gas_price_suggestion_oracle.cache[(curr_head, token_id)] = price
        return price

    def validate_minor_block_seal(self, block: MinorBlock):
        """A more complete validation on PoSW."""
        consensus_type = self.env.quark_chain_config.shards[
            block.header.branch.get_full_shard_id()
        ].CONSENSUS_TYPE
        posw_diff = self.posw_diff_adjust(block)  # could be None
        validate_seal(
            block.header,
            consensus_type,
            adjusted_diff=posw_diff,
            qkchash_with_rotation_stats=consensus_type == ConsensusType.POW_QKCHASH
            and self._qkchashx_enabled(block.header),
        )

    def posw_diff_adjust(self, block: MinorBlock) -> Optional[int]:
        posw_info = self._posw_info(block)
        return posw_info and posw_info.effective_difficulty

    def _posw_info(
        self, block: MinorBlock, stakes: Optional[int] = None
    ) -> Optional[PoSWInfo]:
        header = block.header
        if header.height == 0:  # genesis
            return None

        if stakes is None:
            stakes = self._get_evm_state_for_new_block(
                block, ephemeral=True
            ).get_balance(
                header.coinbase_address.recipient,
                self.env.quark_chain_config.genesis_token,
            )
        block_cnt = self._get_posw_coinbase_blockcnt(header.hash_prev_minor_block)
        stake_per_block = self.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK
        enable_decay_ts = (
            self.env.quark_chain_config.ENABLE_POSW_STAKING_DECAY_TIMESTAMP
        )
        if enable_decay_ts is not None and block.header.create_time > enable_decay_ts:
            stake_per_block = self.__decay_by_epoch(stake_per_block, header.height)
        return get_posw_info(
            self.shard_config.POSW_CONFIG,
            header,
            stakes,
            block_cnt,
            stake_per_block=stake_per_block,
        )

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

    def _get_evm_state_from_hash(self, block_hash: bytes) -> Optional[EvmState]:
        if block_hash == self.header_tip.get_hash():
            return self.evm_state

        # note `_get_evm_state_for_new_block` actually fetches the state in the previous block
        # first get the current block then get next block through height
        block = self.db.get_minor_block_by_hash(block_hash)
        if not block:
            Logger.error("Failed to get block with hash {}".format(block_hash.hex()))
            return None
        next_block = self.db.get_minor_block_by_height(block.header.height + 1)
        if next_block.header.hash_prev_minor_block != block_hash:
            Logger.error(
                "Blocks not correctly linked at height {}".format(block.header.height)
            )
            return None
        return self._get_evm_state_for_new_block(next_block)

    def _get_posw_coinbase_blockcnt(self, header_hash: bytes) -> Dict[bytes, int]:
        """ PoSW needed function: get coinbase addresses up until the given block
        hash (inclusive) along with block counts within the PoSW window.

        Raise ValueError if anything goes wrong.
        """
        return get_posw_coinbase_blockcnt(
            self.shard_config.POSW_CONFIG.WINDOW_SIZE,
            self.coinbase_addr_cache,
            header_hash,
            self.db.get_minor_block_header_by_hash,
        )

    def _get_sender_disallow_map(self, header, recipient=None) -> Dict[bytes, int]:
        """Take an additional recipient parameter and add its block count."""
        if not self._posw_enabled(header):
            return {}
        total_stakes = self.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK
        blockcnt = self._get_posw_coinbase_blockcnt(header.get_hash())
        if recipient:
            blockcnt[recipient] += 1
        return {k: v * total_stakes for k, v in blockcnt.items()}

    def is_committed_by_hash(self, h):
        return self.db.is_minor_block_committed_by_hash(h)

    def commit_by_hash(self, h):
        self.db.commit_minor_block_by_hash(h)

    def get_minor_block_by_hash(
        self, block_hash: bytes, need_extra_info: bool
    ) -> Tuple[Optional[MinorBlock], Optional[Dict]]:
        block = self.db.get_minor_block_by_hash(block_hash)
        if not block:
            return None, None
        if not need_extra_info:
            return block, None
        posw_info = self._posw_info(block)
        return block, posw_info and posw_info._asdict()

    def get_minor_block_by_height(
        self, height: int, need_extra_info: bool
    ) -> Tuple[Optional[MinorBlock], Optional[Dict]]:
        block = self.db.get_minor_block_by_height(height)
        if not block:
            return None, None
        if not need_extra_info:
            return block, None
        posw_info = self._posw_info(block)
        return block, posw_info and posw_info._asdict()

    def get_root_chain_stakes(
        self,
        recipient: bytes,
        block_hash: bytes,
        mock_evm_state: Optional[EvmState] = None,  # open for testing
    ) -> (int, bytes):
        meta = self.db.get_minor_block_meta_by_hash(block_hash)
        check(meta is not None)
        evm_state = mock_evm_state or self.__create_evm_state(
            meta.hash_evm_state_root, {}
        )
        check(evm_state is not None)
        contract_addr = SystemContract.ROOT_CHAIN_POSW.addr()
        code = evm_state.get_code(contract_addr)
        if not code:
            return 0, bytes(20)
        # have to make sure the code is expected
        if (
            utils.sha3_256(code)
            != self.env.quark_chain_config.root_chain_posw_contract_bytecode_hash
        ):
            return 0, bytes(20)

        # call the contract's 'getLockedStakes' function
        mock_sender = bytes(20)  # empty address
        data = bytes.fromhex("fd8c4646000000000000000000000000") + recipient
        evm_tx = EvmTransaction(
            evm_state.get_nonce(mock_sender),
            0,  # gas price
            1000000,  # startgas
            contract_addr,
            0,  # value
            data,
            gas_token_id=self.genesis_token_id,
            transfer_token_id=self.genesis_token_id,
        )
        evm_tx.set_quark_chain_config(self.env.quark_chain_config)
        evm_tx.sender = mock_sender
        success, output = apply_transaction(
            evm_state, evm_tx, tx_wrapper_hash=bytes(32)
        )
        if not success or not output:
            return 0, bytes(20)
        stakes = int.from_bytes(output[:32], byteorder="big")
        signer = output[32 + 12 :]
        return stakes, signer

    def _posw_enabled(self, header):
        config = self.shard_config.POSW_CONFIG
        return config.ENABLED and header.create_time >= config.ENABLE_TIMESTAMP

    def _qkchashx_enabled(self, header):
        config = self.env.quark_chain_config
        return (
            config.ENABLE_QKCHASHX_HEIGHT is not None
            and header.height >= config.ENABLE_QKCHASHX_HEIGHT
        )

    def __decay_by_epoch(self, value: int, block_height: int):
        epoch = (
            block_height
            // self.env.quark_chain_config.shards[self.full_shard_id].EPOCH_INTERVAL
        )
        decay_numerator = (
            self.env.quark_chain_config.block_reward_decay_factor.numerator ** epoch
        )
        decay_denominator = (
            self.env.quark_chain_config.block_reward_decay_factor.denominator ** epoch
        )
        return value * decay_numerator // decay_denominator

    def get_total_balance(
        self,
        token_id: int,
        block_hash: bytes,
        limit: int,
        start: Optional[bytes] = None,
    ) -> Tuple[int, bytes]:
        """
        Start should be exclusive during the iteration.
        """
        evm_state = self._get_evm_state_from_hash(block_hash)
        if not evm_state:
            raise Exception("block hash not found")
        trie = evm_state.trie.trie
        total = 0
        key = start or bytes(32)  # convert None to empty bytes
        while limit > 0:
            key = trie.next(key)
            if not key:
                break
            addr = evm_state.db.get(key)
            total += evm_state.get_balance(addr, token_id, should_cache=False)
            limit -= 1
        return total, key or bytes(32)
