from fractions import Fraction

from quarkchain.config import QuarkChainConfig
from quarkchain.core import (
    Address,
    MinorBlockMeta,
    MinorBlockHeader,
    MinorBlock,
    Branch,
    RootBlockHeader,
    RootBlock,
)
from quarkchain.evm.state import State as EvmState
from quarkchain.utils import sha3_256, check


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
        )
        return RootBlock(header=header, minor_block_header_list=[])

    def create_minor_block(
        self, root_block: RootBlock, full_shard_id: int, evm_state: EvmState
    ) -> MinorBlock:
        """ Create genesis block for shard.
        Genesis block's hash_prev_root_block is set to the genesis root block.
        Genesis state will be committed to the given evm_state.
        Based on ALLOC, genesis_token will be added to initial accounts.
        """
        branch = Branch(full_shard_id)
        shard_config = self._qkc_config.shards[full_shard_id]
        genesis = shard_config.GENESIS

        for address_hex, alloc_amount in genesis.ALLOC.items():
            address = Address.create_from(bytes.fromhex(address_hex))
            check(
                self._qkc_config.get_full_shard_id_by_full_shard_key(
                    address.full_shard_key
                )
                == full_shard_id
            )
            evm_state.full_shard_key = address.full_shard_key
            if isinstance(alloc_amount, dict):
                for k, v in alloc_amount.items():
                    evm_state.delta_token_balance(address.recipient, k, v)
            else:
                evm_state.delta_token_balance(
                    address.recipient, self._qkc_config.genesis_token, alloc_amount
                )

        evm_state.commit()

        meta = MinorBlockMeta(
            hash_merkle_root=bytes.fromhex(genesis.HASH_MERKLE_ROOT),
            hash_evm_state_root=evm_state.trie.root_hash,
        )

        local_fee_rate = 1 - self._qkc_config.reward_tax_rate  # type: Fraction
        coinbase_amount = (
            shard_config.COINBASE_AMOUNT
            * local_fee_rate.numerator
            // local_fee_rate.denominator
        )
        coinbase_address = Address.create_empty_account(full_shard_id)

        header = MinorBlockHeader(
            version=genesis.VERSION,
            height=genesis.HEIGHT,
            branch=branch,
            hash_prev_minor_block=bytes.fromhex(genesis.HASH_PREV_MINOR_BLOCK),
            hash_prev_root_block=root_block.header.get_hash(),
            evm_gas_limit=genesis.GAS_LIMIT,
            hash_meta=sha3_256(meta.serialize()),
            coinbase_amount=coinbase_amount,
            coinbase_address=coinbase_address,
            create_time=genesis.TIMESTAMP,
            difficulty=genesis.DIFFICULTY,
            extra_data=bytes.fromhex(genesis.EXTRA_DATA),
        )
        return MinorBlock(header=header, meta=meta, tx_list=[])
