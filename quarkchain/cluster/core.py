#!/usr/bin/python3

# Core data structures of cluster

import copy

from typing import Optional

# to bypass circular imports
import quarkchain.evm.messages

from quarkchain.utils import sha3_256
from quarkchain.core import uint2048, uint256, hash256, uint32, uint64, calculate_merkle_root
from quarkchain.core import Address, Branch, Constant, Transaction, FixedSizeBytesSerializer
from quarkchain.core import Serializable, ShardInfo
from quarkchain.core import PrependedSizeBytesSerializer, PrependedSizeListSerializer
from quarkchain.evm import trie
from quarkchain.evm.messages import Log as EthLog
import rlp


def mk_receipt_sha(receipts, db):
    t = trie.Trie(db)
    for i, receipt in enumerate(receipts):
        t.update(rlp.encode(i), rlp.encode(receipt))
    return t.root_hash


class Log(Serializable):

    FIELDS = [
        ("recipient", FixedSizeBytesSerializer(20)),
        ("topics", PrependedSizeListSerializer(1, FixedSizeBytesSerializer(32))),
        ("data", PrependedSizeBytesSerializer(4)),
    ]

    def __init__(self, recipient, topics, data):
        self.recipient = recipient
        self.topics = topics
        self.data = data

    @classmethod
    def createFromEthLog(cls, ethLog: EthLog):
        recipient = ethLog.address
        data = ethLog.data

        topics = []
        for topic in ethLog.topics:
            topics.append(topic.to_bytes(32, "big"))
        return Log(recipient, topics, data)

    def to_dict(self):
        return {
            "recipient": self.recipient.hex(),
            "topics": [t.hex() for t in self.topics],
            "data": self.data.hex(),
        }


class TransactionReceipt(Serializable):
    """ Wrapper over tx receipts from EVM """
    FIELDS = [
        ('success', PrependedSizeBytesSerializer(1)),
        ('gasUsed', uint64),
        ('prevGasUsed', uint64),
        ('bloom', uint2048),
        ('contractAddress', Address),
        ("logs", PrependedSizeListSerializer(4, Log)),
    ]

    def __init__(self, success, gasUsed, prevGasUsed, contractAddress, bloom, logs):
        self.success = success
        self.gasUsed = gasUsed
        self.prevGasUsed = prevGasUsed
        self.contractAddress = contractAddress
        self.bloom = bloom
        self.logs = logs

    @classmethod
    def createEmptyReceipt(cls):
        return cls(b'', 0, 0, Address.createEmptyAccount(0), 0, [])


class MinorBlockMeta(Serializable):
    """ Meta data that are not included in root block
    """
    FIELDS = [
        ("hashMerkleRoot", hash256),
        ("hashEvmStateRoot", hash256),
        ("hashEvmReceiptRoot", hash256),
        ("coinbaseAddress", Address),
        ("evmGasLimit", uint256),
        ("evmGasUsed", uint256),
        ("evmCrossShardReceiveGasUsed", uint256),
        ("extraData", PrependedSizeBytesSerializer(2)),
    ]

    def __init__(self,
                 hashMerkleRoot=bytes(Constant.HASH_LENGTH),
                 hashEvmStateRoot=bytes(Constant.HASH_LENGTH),
                 hashEvmReceiptRoot=bytes(Constant.HASH_LENGTH),
                 coinbaseAddress=Address.createEmptyAccount(),
                 evmGasLimit=30000 * 400,  # 400 xshard tx
                 evmGasUsed=0,
                 evmCrossShardReceiveGasUsed=0,
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
        ("coinbaseAmount", uint256),
        ("hashPrevMinorBlock", hash256),
        ("hashPrevRootBlock", hash256),
        ("hashMeta", hash256),
        ("createTime", uint64),
        ("difficulty", uint64),
        ("nonce", uint64),
    ]

    def __init__(self,
                 version=0,
                 height=0,
                 branch=Branch.create(1, 0),
                 coinbaseAmount=0,
                 hashPrevMinorBlock=bytes(Constant.HASH_LENGTH),
                 hashPrevRootBlock=bytes(Constant.HASH_LENGTH),
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
        ("txList", PrependedSizeListSerializer(4, Transaction))
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
            self.header.hashPrevRootBlock = hashPrevRootBlock
        self.meta.hashEvmStateRoot = evmState.trie.root_hash
        self.meta.evmGasUsed = evmState.gas_used
        self.meta.evmCrossShardReceiveGasUsed = evmState.xshard_receive_gas_used
        self.header.coinbaseAmount = evmState.block_fee // 2
        self.finalizeMerkleRoot()
        self.meta.hashEvmReceiptRoot = mk_receipt_sha(evmState.receipts, evmState.db)
        self.header.hashMeta = self.meta.getHash()
        return self

    def addTx(self, tx):
        self.txList.append(tx)
        return self

    def getReceipt(self, db, i) -> Optional[TransactionReceipt]:
        # ignore if no meta is set
        if self.meta.hashEvmReceiptRoot == bytes(Constant.HASH_LENGTH):
            return None

        t = trie.Trie(db, self.meta.hashEvmReceiptRoot)
        receipt = rlp.decode(t.get(rlp.encode(i)), quarkchain.evm.messages.Receipt)
        if receipt.contract_address != b'':
            contractAddress = Address(receipt.contract_address, receipt.contract_full_shard_id)
        else:
            contractAddress = Address.createEmptyAccount(fullShardId=0)

        if i > 0:
            prevGasUsed = rlp.decode(t.get(rlp.encode(i - 1)), quarkchain.evm.messages.Receipt).gas_used
        else:
            prevGasUsed = self.meta.evmCrossShardReceiveGasUsed

        logs = [Log.createFromEthLog(ethLog) for ethLog in receipt.logs]

        return TransactionReceipt(
            receipt.state_root,
            receipt.gas_used,
            prevGasUsed,
            contractAddress,
            receipt.bloom,
            logs,
        )

    def createBlockToAppend(self,
                            createTime=None,
                            address=None,
                            quarkash=0,
                            difficulty=None,
                            extraData=b'',
                            nonce=0):
        if address is None:
            address = Address.createEmptyAccount(fullShardId=self.meta.coinbaseAddress.fullShardId)
        meta = MinorBlockMeta(coinbaseAddress=address,
                              evmGasLimit=self.meta.evmGasLimit,
                              extraData=extraData)

        createTime = self.header.createTime + 1 if createTime is None else createTime
        difficulty = self.header.difficulty if difficulty is None else difficulty
        header = MinorBlockHeader(version=self.header.version,
                                  height=self.header.height + 1,
                                  branch=self.header.branch,
                                  coinbaseAmount=quarkash,
                                  hashPrevMinorBlock=self.header.getHash(),
                                  hashPrevRootBlock=self.header.hashPrevRootBlock,
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

    def createBlockToAppend(self, createTime=None, difficulty=None, address=None, nonce=0):
        createTime = self.createTime + 1 if createTime is None else createTime
        difficulty = difficulty if difficulty is not None else self.difficulty
        header = RootBlockHeader(version=self.version,
                                 height=self.height + 1,
                                 shardInfo=copy.copy(self.shardInfo),
                                 hashPrevBlock=self.getHash(),
                                 hashMerkleRoot=bytes(32),
                                 createTime=createTime,
                                 difficulty=difficulty,
                                 nonce=nonce)
        return RootBlock(header)


class RootBlock(Serializable):
    FIELDS = [
        ("header", RootBlockHeader),
        ("minorBlockHeaderList", PrependedSizeListSerializer(4, MinorBlockHeader))
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

    def createBlockToAppend(self, createTime=None, difficulty=None, address=None, nonce=0):
        return self.header.createBlockToAppend(
            createTime=createTime,
            difficulty=difficulty,
            address=address,
            nonce=nonce)


class CrossShardTransactionDeposit(Serializable):
    """ Destination of x-shard tx
    """
    FIELDS = [
        ("txHash", hash256),  # hash of quarkchain.core.Transaction
        ("fromAddress", Address),
        ("toAddress", Address),
        ("value", uint256),
        ("gasPrice", uint256),
    ]

    def __init__(self, txHash, fromAddress, toAddress, value, gasPrice):
        self.txHash = txHash
        self.fromAddress = fromAddress
        self.toAddress = toAddress
        self.value = value
        self.gasPrice = gasPrice


class CrossShardTransactionList(Serializable):
    FIELDS = [
        ("txList", PrependedSizeListSerializer(4, CrossShardTransactionDeposit))
    ]

    def __init__(self, txList):
        self.txList = txList
