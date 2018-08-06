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


def create_genesis_minor_block(env, shardId, evmState):
    ''' Create a genesis minor block that doesn't depend on any root block
    '''
    branch = Branch.create(env.config.SHARD_SIZE, shardId)

    meta = MinorBlockMeta(hashMerkleRoot=bytes(32),
                          hashEvmStateRoot=evmState.trie.root_hash,
                          coinbaseAddress=env.config.GENESIS_ACCOUNT.addressInBranch(branch),
                          extraData=b'It was the best of times, it was the worst of times, ... - Charles Dickens')
    header = MinorBlockHeader(version=0,
                              height=0,
                              branch=branch,
                              hashPrevMinorBlock=bytes(32),
                              hashMeta=sha3_256(meta.serialize()),
                              coinbaseAmount=env.config.GENESIS_MINOR_COIN,
                              createTime=env.config.GENESIS_CREATE_TIME,
                              difficulty=env.config.GENESIS_MINOR_DIFFICULTY)

    return MinorBlock(
        header=header,
        meta=meta,
        txList=[])


def create_genesis_root_block(env, minorBlockHeaderList=[]):
    header = RootBlockHeader(version=0,
                             height=0,
                             shardInfo=ShardInfo.create(env.config.SHARD_SIZE),
                             hashPrevBlock=bytes(32),
                             hashMerkleRoot=calculate_merkle_root(minorBlockHeaderList),
                             createTime=env.config.GENESIS_CREATE_TIME,
                             difficulty=env.config.GENESIS_DIFFICULTY)
    block = RootBlock(
        header=header,
        minorBlockHeaderList=minorBlockHeaderList)
    return block


def create_genesis_evm_list(env, dbMap=dict()):
    ''' Create genesis evm state map.  Basically, it will load account balance from json file to pre-mine.
    '''
    evmList = []
    for shardId in range(env.config.SHARD_SIZE):
        evmState = EvmState(
            env=env.evmEnv,
            db=dbMap[shardId] if shardId in dbMap else InMemoryDb())
        evmState.full_shard_id = env.config.GENESIS_ACCOUNT.fullShardId & (~(env.config.SHARD_SIZE - 1)) | shardId
        evmState.delta_balance(env.config.GENESIS_ACCOUNT.recipient, env.config.GENESIS_MINOR_COIN)

        if env.config.ACCOUNTS_TO_FUND:
            for address in env.config.ACCOUNTS_TO_FUND:
                if address.getShardId(env.config.SHARD_SIZE) == shardId:
                    evmState.full_shard_id = address.fullShardId
                    evmState.delta_balance(address.recipient, env.config.ACCOUNTS_TO_FUND_COIN)

        evmState.commit()
        evmList.append(evmState)
    return evmList


def create_genesis_blocks(env, evmList):
    ''' Create genesis block with pre-defined evm states.
    '''
    # List of minor blocks with height 0
    genesisMinorBlockList0 = []
    for shardId in range(env.config.SHARD_SIZE):
        genesisMinorBlockList0.append(
            create_genesis_minor_block(
                env=env,
                shardId=shardId,
                evmState=evmList[shardId]))
    genesisRootBlock0 = create_genesis_root_block(env, [b.header for b in genesisMinorBlockList0])

    # List of minor blocks with height 1
    genesisMinorBlockList1 = []
    for shardId, block in enumerate(genesisMinorBlockList0):
        genesisMinorBlockList1.append(
            block.createBlockToAppend().finalize(
                evmState=evmList[shardId],
                hashPrevRootBlock=genesisRootBlock0.header.getHash()))

    genesisRootBlock1 = genesisRootBlock0   \
        .createBlockToAppend()              \
        .extendMinorBlockHeaderList([b.header for b in genesisMinorBlockList1]) \
        .finalize()

    return genesisRootBlock0, genesisRootBlock1, genesisMinorBlockList0, genesisMinorBlockList1


def main():
    block = create_genesis_root_block(DEFAULT_ENV)
    s = block.serialize()
    print(s.hex())
    print(len(s))


if __name__ == '__main__':
    main()
