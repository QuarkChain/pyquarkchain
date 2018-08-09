from quarkchain.config import DEFAULT_ENV
from quarkchain.core import MinorBlockMeta, MinorBlockHeader, MinorBlock, Branch, ShardInfo
from quarkchain.core import RootBlockHeader, RootBlock
from quarkchain.core import calculate_merkle_root
from quarkchain.db import InMemoryDb
from quarkchain.evm.state import State as EvmState
from quarkchain.utils import sha3_256


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
    branch = Branch.create(env.config.SHARD_SIZE, shard_id)

    meta = MinorBlockMeta(hash_merkle_root=bytes(32),
                          hash_evm_state_root=evm_state.trie.root_hash,
                          coinbase_address=env.config.GENESIS_ACCOUNT.address_in_branch(branch),
                          extra_data=b'It was the best of times, it was the worst of times, ... - Charles Dickens')
    header = MinorBlockHeader(version=0,
                              height=0,
                              branch=branch,
                              hash_prev_minor_block=bytes(32),
                              hash_meta=sha3_256(meta.serialize()),
                              coinbase_amount=env.config.GENESIS_MINOR_COIN,
                              create_time=env.config.GENESIS_CREATE_TIME,
                              difficulty=env.config.GENESIS_MINOR_DIFFICULTY)

    return MinorBlock(
        header=header,
        meta=meta,
        tx_list=[])


def create_genesis_root_block(env, minor_block_header_list=[]):
    header = RootBlockHeader(version=0,
                             height=0,
                             shard_info=ShardInfo.create(env.config.SHARD_SIZE),
                             hash_prev_block=bytes(32),
                             hash_merkle_root=calculate_merkle_root(minor_block_header_list),
                             create_time=env.config.GENESIS_CREATE_TIME,
                             difficulty=env.config.GENESIS_DIFFICULTY)
    block = RootBlock(
        header=header,
        minor_block_header_list=minor_block_header_list)
    return block


def create_genesis_evm_list(env, db_map=dict()):
    """ Create genesis evm state map.  Basically, it will load account balance from json file to pre-mine.
    """
    evm_list = []
    for shard_id in range(env.config.SHARD_SIZE):
        evm_state = EvmState(
            env=env.evm_env,
            db=db_map[shard_id] if shard_id in db_map else InMemoryDb())
        evm_state.full_shard_id = env.config.GENESIS_ACCOUNT.full_shard_id & (~(env.config.SHARD_SIZE - 1)) | shard_id
        evm_state.delta_balance(env.config.GENESIS_ACCOUNT.recipient, env.config.GENESIS_MINOR_COIN)

        if env.config.ACCOUNTS_TO_FUND:
            for address in env.config.ACCOUNTS_TO_FUND:
                if address.get_shard_id(env.config.SHARD_SIZE) == shard_id:
                    evm_state.full_shard_id = address.full_shard_id
                    evm_state.delta_balance(address.recipient, env.config.ACCOUNTS_TO_FUND_COIN)

        evm_state.commit()
        evm_list.append(evm_state)
    return evm_list


def create_genesis_blocks(env, evm_list):
    """ Create genesis block with pre-defined evm states.
    """
    # List of minor blocks with height 0
    genesis_minor_block_list0 = []
    for shard_id in range(env.config.SHARD_SIZE):
        genesis_minor_block_list0.append(
            create_genesis_minor_block(env=env, shard_id=shard_id, evm_state=evm_list[shard_id]))
    genesis_root_block0 = create_genesis_root_block(env, [b.header for b in genesis_minor_block_list0])

    # List of minor blocks with height 1
    genesis_minor_block_list1 = []
    for shard_id, block in enumerate(genesis_minor_block_list0):
        genesis_minor_block_list1.append(
            block.create_block_to_append().finalize(
                evm_state=evm_list[shard_id],
                hash_prev_root_block=genesis_root_block0.header.get_hash()))

    genesis_root_block1 = genesis_root_block0   \
        .create_block_to_append()              \
        .extend_minor_block_header_list([b.header for b in genesis_minor_block_list1]) \
        .finalize()

    return genesis_root_block0, genesis_root_block1, genesis_minor_block_list0, genesis_minor_block_list1


def main():
    block = create_genesis_root_block(DEFAULT_ENV)
    s = block.serialize()
    print(s.hex())
    print(len(s))


if __name__ == '__main__':
    main()
