from quarkchain.core import RootBlockHeader, RootBlock, Transaction, TransactionOutput, Code
from quarkchain.core import MinorBlockHeader, MinorBlock, Branch, ShardInfo
from quarkchain.config import DEFAULT_ENV


def create_genesis_minor_block(env, shardId, hashRootBlock=bytes(32)):
    header = MinorBlockHeader(version=0,
                              height=0,
                              branch=Branch.create(
                                  env.config.SHARD_SIZE, shardId),
                              hashPrevRootBlock=hashRootBlock,
                              hashPrevMinorBlock=bytes(32),
                              hashMerkleRoot=bytes(32),
                              createTime=env.config.GENESIS_CREATE_TIME,
                              difficulty=env.config.GENESIS_MINOR_DIFFICULTY)
    return MinorBlock(header, [Transaction(
        inList=[],
        code=Code.createMinorBlockCoinbaseCode(header.height),
        outList=[TransactionOutput(env.config.GENESIS_ACCOUNT, env.config.GENESIS_MINOR_COIN)])])


def create_genesis_root_block(env):
    header = RootBlockHeader(version=0,
                             height=0,
                             shardInfo=ShardInfo.create(env.config.SHARD_SIZE),
                             hashPrevBlock=bytes(32),
                             hashMerkleRoot=bytes(32),
                             createTime=env.config.GENESIS_CREATE_TIME,
                             difficulty=env.config.GENESIS_DIFFICULTY)
    block = RootBlock(header, Transaction(
        inList=[],
        code=Code.createRootBlockCoinbaseCode(header.height),
        outList=[TransactionOutput(env.config.GENESIS_ACCOUNT, env.config.GENESIS_COIN)]),
        [])
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
