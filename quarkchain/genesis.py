from quarkchain.core import RootBlockHeader, RootBlock
from quarkchain.config import DEFAULT_ENV
import time


def create_genesis_root_block(env):
    header = RootBlockHeader(version=0, height=0, shardInfo=env.config.SHARD_SIZE,
                             hashPrevBlock=bytes(32),
                             hashMerkleRoot=bytes(32),
                             coinbaseAddress=env.config.GENESIS_ACCOUNT,
                             coinbaseValue=env.config.GENESIS_COIN,
                             createTime=int(time.time()),
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
