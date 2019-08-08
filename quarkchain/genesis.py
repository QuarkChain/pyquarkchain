from copy import copy
from fractions import Fraction
from typing import Tuple

from quarkchain.config import QuarkChainConfig
from quarkchain.core import (
    Address,
    MinorBlockMeta,
    MinorBlockHeader,
    MinorBlock,
    Branch,
    RootBlockHeader,
    RootBlock,
    TokenBalanceMap,
    XshardTxCursorInfo,
)
from quarkchain.evm.state import State as EvmState
from quarkchain.evm.utils import decode_hex, remove_0x_head, big_endian_to_int
from quarkchain.utils import sha3_256, check, token_id_encode


class GenesisManager:
    """ Manage the creation of genesis blocks based on the genesis configs from env"""

    def __init__(self, qkc_config: QuarkChainConfig):
        self._qkc_config = qkc_config

    def create_root_block(self) -> RootBlock:
        """ Create the genesis root block """
        genesis = self._qkc_config.ROOT.GENESIS
        header = RootBlockHeader(
            version=genesis.VERSION,
            height=genesis.HEIGHT,
            hash_prev_block=bytes.fromhex(genesis.HASH_PREV_BLOCK),
            hash_merkle_root=bytes.fromhex(genesis.HASH_MERKLE_ROOT),
            create_time=genesis.TIMESTAMP,
            difficulty=genesis.DIFFICULTY,
            total_difficulty=genesis.DIFFICULTY,
            nonce=genesis.NONCE,
        )
        return RootBlock(header=header, minor_block_header_list=[])

    def create_minor_block(
        self, root_block: RootBlock, full_shard_id: int, evm_state: EvmState
    ) -> Tuple[MinorBlock, TokenBalanceMap]:
        """ Create genesis block for shard.
        Genesis block's hash_prev_root_block is set to the genesis root block.
        Genesis state will be committed to the given evm_state.
        Based on ALLOC, genesis_token will be added to initial accounts.
        """
        branch = Branch(full_shard_id)
        shard_config = self._qkc_config.shards[full_shard_id]
        genesis = shard_config.GENESIS

        for address_hex, alloc_data in genesis.ALLOC.items():
            address = Address.create_from(bytes.fromhex(address_hex))
            check(
                self._qkc_config.get_full_shard_id_by_full_shard_key(
                    address.full_shard_key
                )
                == full_shard_id
            )
            evm_state.full_shard_key = address.full_shard_key
            recipient = address.recipient
            if "code" in alloc_data:
                code = decode_hex(remove_0x_head(alloc_data["code"]))
                evm_state.set_code(recipient, code)
                evm_state.set_nonce(recipient, 1)
            if "storage" in alloc_data:
                for k, v in alloc_data["storage"].items():
                    evm_state.set_storage_data(
                        recipient,
                        big_endian_to_int(decode_hex(k[2:])),
                        big_endian_to_int(decode_hex(v[2:])),
                    )
            # backward compatible:
            # v1: {addr: {QKC: 1234}}
            # v2: {addr: {balances: {QKC: 1234}, code: 0x, storage: {0x12: 0x34}}}
            balances = alloc_data
            if "balances" in alloc_data:
                balances = alloc_data["balances"]
            for k, v in balances.items():
                if k in ("code", "storage"):
                    continue
                evm_state.delta_token_balance(recipient, token_id_encode(k), v)

        evm_state.commit()

        meta = MinorBlockMeta(
            hash_merkle_root=bytes.fromhex(genesis.HASH_MERKLE_ROOT),
            hash_evm_state_root=evm_state.trie.root_hash,
            xshard_tx_cursor_info=XshardTxCursorInfo(root_block.header.height, 0, 0),
        )

        local_fee_rate = 1 - self._qkc_config.reward_tax_rate  # type: Fraction
        coinbase_tokens = {
            self._qkc_config.genesis_token: shard_config.COINBASE_AMOUNT
            * local_fee_rate.numerator
            // local_fee_rate.denominator
        }

        coinbase_address = Address.create_empty_account(full_shard_id)

        header = MinorBlockHeader(
            version=genesis.VERSION,
            height=genesis.HEIGHT,
            branch=branch,
            hash_prev_minor_block=bytes.fromhex(genesis.HASH_PREV_MINOR_BLOCK),
            hash_prev_root_block=root_block.header.get_hash(),
            evm_gas_limit=genesis.GAS_LIMIT,
            hash_meta=sha3_256(meta.serialize()),
            coinbase_amount_map=TokenBalanceMap(coinbase_tokens),
            coinbase_address=coinbase_address,
            create_time=genesis.TIMESTAMP,
            difficulty=genesis.DIFFICULTY,
            extra_data=bytes.fromhex(genesis.EXTRA_DATA),
        )
        return (
            MinorBlock(header=header, meta=meta, tx_list=[]),
            TokenBalanceMap(coinbase_tokens),
        )
