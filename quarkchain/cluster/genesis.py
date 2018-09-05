from quarkchain.evm.config import Env as EvmEnv
from quarkchain.config import QuarkChainConfig
from quarkchain.core import (
    MinorBlockMeta,
    MinorBlockHeader,
    MinorBlock,
    Branch,
    ShardInfo,
)
from quarkchain.core import RootBlockHeader, RootBlock
from quarkchain.core import calculate_merkle_root, Address
from quarkchain.db import InMemoryDb
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
            shard_info=ShardInfo.create(self._qkc_config.SHARD_SIZE),
            hash_prev_block=bytes.fromhex(genesis.HASH_PREV_BLOCK),
            hash_merkle_root=bytes.fromhex(genesis.HASH_MERKLE_ROOT),
            create_time=genesis.TIMESTAMP,
            difficulty=genesis.DIFFICULTY,
        )
        return RootBlock(header=header, minor_block_header_list=[])

    def create_minor_block(self, shard_id: int, evm_state: EvmState) -> MinorBlock:
        """ Create a genesis minor block.
        Changes will be committed to evm_state.
        """
        branch = Branch.create(self._qkc_config.SHARD_SIZE, shard_id)
        genesis = self._qkc_config.SHARD_LIST[shard_id].GENESIS
        coinbase_address = Address.create_from(bytes.fromhex(genesis.COINBASE_ADDRESS))
        check(coinbase_address.get_shard_id(self._qkc_config.SHARD_SIZE) == shard_id)

        for address_hex, amount_in_wei in genesis.ALLOC.items():
            address = Address.create_from(bytes.fromhex(address_hex))
            check(address.get_shard_id(self._qkc_config.SHARD_SIZE) == shard_id)
            evm_state.full_shard_id = address.full_shard_id
            evm_state.delta_balance(address.recipient, amount_in_wei)

        evm_state.commit()

        meta = MinorBlockMeta(
            hash_merkle_root=bytes.fromhex(genesis.HASH_MERKLE_ROOT),
            hash_evm_state_root=evm_state.trie.root_hash,
            coinbase_address=coinbase_address,
            extra_data=bytes.fromhex(genesis.EXTRA_DATA)
        )
        header = MinorBlockHeader(
            version=genesis.VERSION,
            height=genesis.HEIGHT,
            branch=branch,
            hash_prev_minor_block=bytes.fromhex(genesis.HASH_PREV_MINOR_BLOCK),
            hash_prev_root_block=self.create_root_block().header.get_hash(),
            hash_meta=sha3_256(meta.serialize()),
            coinbase_amount=genesis.COINBASE_AMOUNT,
            create_time=genesis.TIMESTAMP,
            difficulty=genesis.DIFFICULTY,
        )
        return MinorBlock(header=header, meta=meta, tx_list=[])

    def get_minor_block_hash(self, shard_id: int) -> bytes:
        if self._qkc_config.SHARD_LIST[shard_id].GENESIS.HASH:
            return bytes.fromhex(self._qkc_config.SHARD_LIST[shard_id].GENESIS.HASH)


# Structure of genesis blocks
#
#  +--+     +--+
#  |m0|<----|m1|
#  +--+  +--+--+
#    ^   |    ^
#    |   |    |
#  +--+<-+  +--+
#  |r0|<----|r1|
#  +--+     +--+
#
# where m0 is the first block in each shard, m1 is the second block in each shard, and
#       r0 is the first root block, which confirms m0, r1 is the second root block, which confirms m1
# This make sures that all hash pointers of m1 and r1 points to previous valid blocks.


def create_genesis_minor_block(env, shard_id, evm_state):
    """ Create a genesis minor block that doesn't depend on any root block
    """
    branch = Branch.create(env.quark_chain_config.SHARD_SIZE, shard_id)

    meta = MinorBlockMeta(
        hash_merkle_root=bytes(32),
        hash_evm_state_root=evm_state.trie.root_hash,
        coinbase_address=env.config.GENESIS_ACCOUNT.address_in_branch(branch),
        extra_data=b"It was the best of times, it was the worst of times, ... - Charles Dickens",
    )
    header = MinorBlockHeader(
        version=0,
        height=0,
        branch=branch,
        hash_prev_minor_block=bytes(32),
        hash_meta=sha3_256(meta.serialize()),
        coinbase_amount=env.config.GENESIS_MINOR_COIN,
        create_time=env.config.GENESIS_CREATE_TIME,
        difficulty=env.config.GENESIS_MINOR_DIFFICULTY,
    )

    return MinorBlock(header=header, meta=meta, tx_list=[])


def create_genesis_root_block(env, minor_block_header_list=[]):
    header = RootBlockHeader(
        version=0,
        height=0,
        shard_info=ShardInfo.create(env.quark_chain_config.SHARD_SIZE),
        hash_prev_block=bytes(32),
        hash_merkle_root=calculate_merkle_root(minor_block_header_list),
        create_time=env.config.GENESIS_CREATE_TIME,
        difficulty=env.config.GENESIS_DIFFICULTY,
    )
    block = RootBlock(header=header, minor_block_header_list=minor_block_header_list)
    return block


def create_genesis_evm_list(env, db_map=dict()):
    """ Create genesis evm state map.  Basically, it will load account balance from json file to pre-mine.
    """
    evm_list = []
    for shard_id in range(env.quark_chain_config.SHARD_SIZE):
        evm_state = EvmState(
            env=env.evm_env, db=db_map[shard_id] if shard_id in db_map else InMemoryDb()
        )
        evm_state.full_shard_id = (
            env.config.GENESIS_ACCOUNT.full_shard_id & (~(env.quark_chain_config.SHARD_SIZE - 1))
            | shard_id
        )
        evm_state.delta_balance(
            env.config.GENESIS_ACCOUNT.recipient, env.config.GENESIS_MINOR_COIN
        )

        if env.config.ACCOUNTS_TO_FUND:
            for address in env.config.ACCOUNTS_TO_FUND:
                if address.get_shard_id(env.quark_chain_config.SHARD_SIZE) == shard_id:
                    evm_state.full_shard_id = address.full_shard_id
                    evm_state.delta_balance(
                        address.recipient, env.config.ACCOUNTS_TO_FUND_COIN
                    )

        evm_state.commit()
        evm_list.append(evm_state)
    return evm_list


def create_genesis_blocks(env, evm_list):
    """ Create genesis block with pre-defined evm states.
    """
    # List of minor blocks with height 0
    genesis_minor_block_list0 = []
    for shard_id in range(env.quark_chain_config.SHARD_SIZE):
        genesis_minor_block_list0.append(
            create_genesis_minor_block(
                env=env, shard_id=shard_id, evm_state=evm_list[shard_id]
            )
        )
    genesis_root_block0 = create_genesis_root_block(
        env, [b.header for b in genesis_minor_block_list0]
    )

    # List of minor blocks with height 1
    genesis_minor_block_list1 = []
    for shard_id, block in enumerate(genesis_minor_block_list0):
        genesis_minor_block_list1.append(
            block.create_block_to_append().finalize(
                evm_state=evm_list[shard_id],
                hash_prev_root_block=genesis_root_block0.header.get_hash(),
            )
        )

    genesis_root_block1 = (
        genesis_root_block0.create_block_to_append()
        .extend_minor_block_header_list([b.header for b in genesis_minor_block_list1])
        .finalize()
    )

    return (
        genesis_root_block0,
        genesis_root_block1,
        genesis_minor_block_list0,
        genesis_minor_block_list1,
    )
