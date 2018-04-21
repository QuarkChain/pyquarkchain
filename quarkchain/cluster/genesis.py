from quarkchain.cluster.core import RootBlockHeader, RootBlock
from quarkchain.cluster.core import MinorBlockMeta, MinorBlockHeader, MinorBlock, Branch, ShardInfo
from quarkchain.config import DEFAULT_ENV
from quarkchain.utils import sha3_256


def create_genesis_minor_block(env, shardId, hashRootBlock=bytes(32)):
    branch = Branch.create(env.config.SHARD_SIZE, shardId)
    meta = MinorBlockMeta(hashPrevRootBlock=hashRootBlock,
                          hashMerkleRoot=bytes(32),
                          hashEvmStateRoot=bytes(32),
                          coinbaseAddress=env.config.GENESIS_ACCOUNT.addressInBranch(branch),
                          extraData=b'It was the best of times, it was the worst of times, ... - Charles Dickens')
    header = MinorBlockHeader(version=0,
                              height=0,
                              branch=branch,
                              hashPrevMinorBlock=bytes(32),
                              hashMeta=sha3_256(meta.serialize()),
                              createTime=env.config.GENESIS_CREATE_TIME,
                              difficulty=env.config.GENESIS_MINOR_DIFFICULTY)
    return MinorBlock(
        header=header,
        meta=meta,
        txList=[])


def create_genesis_root_block(env):
    header = RootBlockHeader(version=0,
                             height=0,
                             shardInfo=ShardInfo.create(env.config.SHARD_SIZE),
                             hashPrevBlock=bytes(32),
                             hashMerkleRoot=bytes(32),
                             createTime=env.config.GENESIS_CREATE_TIME,
                             difficulty=env.config.GENESIS_DIFFICULTY)
    block = RootBlock(
        header=header,
        minorBlockHeaderList=[])
    return block


def create_genesis_blocks(env):
    rootBlock = create_genesis_root_block(env)
    minorBlockList = [create_genesis_minor_block(
        env, i, rootBlock.header.getHash()) for i in range(env.config.SHARD_SIZE)]
    rootBlock.minorBlockHeaderList = [b.header for b in minorBlockList]
    return (rootBlock, minorBlockList)


def main():
    block = create_genesis_root_block(DEFAULT_ENV)
    s = block.serialize()
    print(s.hex())
    print(len(s))


if __name__ == '__main__':
    main()
