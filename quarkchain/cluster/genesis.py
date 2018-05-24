from quarkchain.cluster.core import RootBlockHeader, RootBlock
from quarkchain.cluster.core import MinorBlockMeta, MinorBlockHeader, MinorBlock, Branch, ShardInfo
from quarkchain.config import DEFAULT_ENV
from quarkchain.utils import sha3_256


def create_genesis_minor_block(env, shardId, evmState, hashRootBlock=bytes(32)):
    branch = Branch.create(env.config.SHARD_SIZE, shardId)
    evmState.delta_balance(env.config.GENESIS_ACCOUNT.recipient, env.config.GENESIS_MINOR_COIN)

    if env.config.ACCOUNTS_TO_FUND:
        for address in env.config.ACCOUNTS_TO_FUND:
            if address.getShardId(env.config.SHARD_SIZE) == shardId:
                evmState.delta_balance(address.recipient, env.config.ACCOUNTS_TO_FUND_COIN)

    if env.config.LOADTEST_ACCOUNTS:
        for address in env.config.LOADTEST_ACCOUNTS:
            evmState.delta_balance(address.recipient, env.config.LOADTEST_ACCOUNTS_COIN)

    evmState.commit()

    meta = MinorBlockMeta(hashMerkleRoot=bytes(32),
                          hashEvmStateRoot=evmState.trie.root_hash,
                          coinbaseAddress=env.config.GENESIS_ACCOUNT.addressInBranch(branch),
                          extraData=b'It was the best of times, it was the worst of times, ... - Charles Dickens')
    header = MinorBlockHeader(version=0,
                              height=0,
                              branch=branch,
                              hashPrevMinorBlock=bytes(32),
                              hashPrevRootBlock=hashRootBlock,
                              hashMeta=sha3_256(meta.serialize()),
                              coinbaseAmount=env.config.GENESIS_MINOR_COIN,
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


def main():
    block = create_genesis_root_block(DEFAULT_ENV)
    s = block.serialize()
    print(s.hex())
    print(len(s))


if __name__ == '__main__':
    main()
