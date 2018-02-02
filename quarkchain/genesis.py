from quarkchain.core import RootBlockHeader, RootBlock
from quarkchain.core import MinorBlockHeader, MinorBlock
from quarkchain.config import DEFAULT_ENV
import time


def create_genesis_minor_block(env, shardId):
    header = MinorBlockHeader(version=0,
                              height=0,
                              branch=(1 << env.config.SHARD_SIZE_BITS) + shardId,
                              hashPrevRootBlock=bytes(32),
                              hashPrevMinorBlock=bytes(32),
                              hashMerkleRoot=bytes(32),
                              createTime=env.config.GENESIS_CREATE_TIME,
                              difficulty=env.config.GENESIS_MINOR_DIFFICULTY)
    return MinorBlock(header, [])


def create_genesis_root_block(env):
    header = RootBlockHeader(version=0, height=0, shardInfo=env.config.SHARD_SIZE,
                             hashPrevBlock=bytes(32),
                             hashMerkleRoot=bytes(32),
                             coinbaseAddress=env.config.GENESIS_ACCOUNT,
                             coinbaseValue=env.config.GENESIS_COIN,
                             createTime=env.config.GENESIS_CREATE_TIME,
                             difficulty=env.config.GENESIS_DIFFICULTY)
    block = RootBlock(header, [])
    return block


def main():
    block = create_genesis_root_block(DEFAULT_ENV)
    s = block.serialize()
    print(s.hex())
    print(len(s))


if __name__ == '__main__':
    main()
