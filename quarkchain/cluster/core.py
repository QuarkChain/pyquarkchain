#!/usr/bin/python3

# Core data structures of cluster

import copy


from quarkchain.utils import sha3_256
from quarkchain.core import uint256, hash256, uint32, uint64, calculate_merkle_root
from quarkchain.core import Address, Branch, Constant, Transaction
from quarkchain.core import Serializable, ShardInfo
from quarkchain.core import PreprendedSizeBytesSerializer, PreprendedSizeListSerializer


class MinorBlockMeta(Serializable):
    """ Meta data that are not included in root block
    """
    FIELDS = [
        ("hashPrevRootBlock", hash256),
        ("hashMerkleRoot", hash256),
        ("hashEvmStateRoot", hash256),
        ("coinbaseAddress", Address),
        ("coinbaseAmount", uint256),
        ("evmGasLimit", uint256),
        ("evmGasUsed", uint256),
        ("extraData", PreprendedSizeBytesSerializer(2)),
    ]

    def __init__(self,
                 hashPrevRootBlock=bytes(Constant.HASH_LENGTH),
                 hashMerkleRoot=bytes(Constant.HASH_LENGTH),
                 hashEvmStateRoot=bytes(Constant.HASH_LENGTH),
                 coinbaseAddress=bytes(Constant.ADDRESS_LENGTH),
                 coinbaseAmount=0,
                 evmGasLimit=100000000000,
                 evmGasUsed=0,
                 extraData=b''):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def getHash(self):
        return sha3_256(self.serialize())


class MinorBlockHeader(Serializable):
    """ Header fields that are included in root block so that the root chain could quickly verify
    - Verify minor block headers included are valid appends on existing shards
    - Verify minor block headers reach sufficient difficulty
    - Verify minor block headers have correct timestamp
    Once the root block contains these correct information, then it is highly likely that the root block is valid.
    """
    FIELDS = [
        ("version", uint32),
        ("branch", Branch),
        ("height", uint64),
        ("hashPrevMinorBlock", hash256),
        ("hashMeta", hash256),
        ("createTime", uint64),
        ("difficulty", uint64),
        ("nonce", uint64),
    ]

    def __init__(self,
                 version=0,
                 height=0,
                 branch=Branch.create(1, 0),
                 hashPrevMinorBlock=bytes(Constant.HASH_LENGTH),
                 hashMeta=bytes(Constant.HASH_LENGTH),
                 createTime=0,
                 difficulty=0,
                 nonce=0):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def getHash(self):
        return sha3_256(self.serialize())


class MinorBlock(Serializable):
    FIELDS = [
        ("header", MinorBlockHeader),
        ("meta", MinorBlockMeta),
        ("txList", PreprendedSizeListSerializer(4, Transaction))
    ]

    def __init__(self, header, meta, txList=None):
        self.header = header
        self.meta = meta
        self.txList = [] if txList is None else txList

    def calculateMerkleRoot(self):
        return calculate_merkle_root(self.txList)

    def finalizeMerkleRoot(self):
        """ Compute merkle root hash and put it in the field
        """
        self.meta.hashMerkleRoot = self.calculateMerkleRoot()
        return self

    def finalize(self, evmState, hashPrevRootBlock=None):
        if hashPrevRootBlock is not None:
            self.meta.hashPrevRootBlock = hashPrevRootBlock
        self.meta.hashEvmStateRoot = evmState.trie.root_hash
        self.meta.evmGasUsed = evmState.gas_used
        self.meta.coinbaseAmount = evmState.block_fee // 2
        self.finalizeMerkleRoot()
        self.header.hashMeta = self.meta.getHash()
        return self

    def addTx(self, tx):
        self.txList.append(tx)
        return self

    def createBlockToAppend(self,
                            address=None,
                            createTime=None,
                            difficulty=None,
                            extraData=b'',
                            nonce=0):
        if address is None:
            address = Address.createEmptyAccount(fullShardId=self.meta.coinbaseAddress.fullShardId)
        meta = MinorBlockMeta(hashPrevRootBlock=self.meta.hashPrevRootBlock,
                              coinbaseAddress=address,
                              evmGasLimit=self.meta.evmGasLimit,
                              extraData=extraData)

        createTime = self.header.createTime + 1 if createTime is None else createTime
        difficulty = self.header.difficulty if difficulty is None else difficulty
        header = MinorBlockHeader(version=self.header.version,
                                  height=self.header.height + 1,
                                  branch=self.header.branch,
                                  hashPrevMinorBlock=self.header.getHash(),
                                  createTime=createTime,
                                  difficulty=difficulty,
                                  nonce=nonce)

        return MinorBlock(header, meta, [])


class RootBlockHeader(Serializable):
    FIELDS = [
        ("version", uint32),
        ("height", uint32),
        ("shardInfo", ShardInfo),
        ("hashPrevBlock", hash256),
        ("hashMerkleRoot", hash256),
        ("coinbaseAddress", Address),
        ("coinbaseAmount", uint256),
        ("createTime", uint32),
        ("difficulty", uint32),
        ("nonce", uint32)]

    def __init__(self,
                 version=0,
                 height=0,
                 shardInfo=ShardInfo.create(1, False),
                 hashPrevBlock=bytes(32),
                 hashMerkleRoot=bytes(32),
                 coinbaseAddress=Address.createEmptyAccount(),
                 coinbaseAmount=0,
                 createTime=0,
                 difficulty=0,
                 nonce=0):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def getHash(self):
        return sha3_256(self.serialize())

    def createBlockToAppend(self, createTime=None, difficulty=None, address=None):
        createTime = self.createTime + 1 if createTime is None else createTime
        address = Address.createEmptyAccount() if address is None else address
        difficulty = difficulty if difficulty is not None else self.difficulty
        header = RootBlockHeader(version=self.version,
                                 height=self.height + 1,
                                 shardInfo=copy.copy(self.shardInfo),
                                 hashPrevBlock=self.getHash(),
                                 hashMerkleRoot=bytes(32),
                                 createTime=createTime,
                                 difficulty=difficulty,
                                 nonce=0)
        return RootBlock(header)


class RootBlock(Serializable):
    FIELDS = [
        ("header", RootBlockHeader),
        ("minorBlockHeaderList", PreprendedSizeListSerializer(4, MinorBlockHeader))
    ]

    def __init__(self, header, minorBlockHeaderList=None):
        self.header = header
        self.minorBlockHeaderList = [] if minorBlockHeaderList is None else minorBlockHeaderList

    def finalize(self, quarkash=0, coinbaseAddress=Address.createEmptyAccount()):
        self.header.hashMerkleRoot = calculate_merkle_root(
            self.minorBlockHeaderList)

        self.header.coinbaseAmount = quarkash
        self.header.coinbaseAddress = coinbaseAddress
        return self

    def addMinorBlockHeader(self, header):
        self.minorBlockHeaderList.append(header)
        return self

    def extendMinorBlockHeaderList(self, headerList):
        self.minorBlockHeaderList.extend(headerList)
        return self

    def createBlockToAppend(self, createTime=None, difficulty=None, address=None):
        return self.header.createBlockToAppend(createTime=createTime, difficulty=difficulty, address=address)


class CrossShardTransactionDeposit(Serializable):
    """ Destination of x-shard tx
    """
    FIELDS = [
        ("address", Address),
        ("amount", uint256),
        ("gasPrice", uint256),
    ]

    def __init__(self, address, amount, gasPrice):
        self.address = address
        self.amount = amount
        self.gasPrice = gasPrice


class CrossShardTransactionList(Serializable):
    FIELDS = [
        ("txList", PreprendedSizeListSerializer(4, CrossShardTransactionDeposit))
    ]

    def __init__(self, txList):
        self.txList = txList
