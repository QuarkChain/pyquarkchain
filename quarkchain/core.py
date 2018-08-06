#!/usr/bin/python3

# Core data structures
# Use standard ecsda, but should be replaced by ethereum's secp256k1
# implementation

import argparse
import copy
import random

import ecdsa
import rlp

import quarkchain.evm.messages
from ethereum import utils
from quarkchain.evm import trie
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import int_left_most_bit, is_p2, sha3_256, check, masks_have_overlap

secpk1n = 115792089237316195423570985008687907852837564279074904382605163141518161494337


class Constant:
    KEY_LENGTH = 32
    KEY_HEX_LENGTH = KEY_LENGTH * 2
    RECIPIENT_LENGTH = 20
    SHARD_ID_LENGTH = 4
    ADDRESS_LENGTH = RECIPIENT_LENGTH + SHARD_ID_LENGTH
    ADDRESS_HEX_LENGTH = ADDRESS_LENGTH * 2
    SIGNATURE_LENGTH = 65
    SIGNATURE_HEX_LENGTH = SIGNATURE_LENGTH * 2
    TX_HASH_HEX_LENGTH = 64
    HASH_LENGTH = 32


class ByteBuffer:
    """ Java-like ByteBuffer, which wraps a bytes or bytearray with position.
    If there is no enough space during deserialization, throw exception
    """

    def __init__(self, data):
        # We don't want deserialized object to have bytearray
        # which isn't hashable
        self.bytes = bytes(data)
        self.position = 0

    def __checkSpace(self, space):
        if space > len(self.bytes) - self.position:
            raise RuntimeError("no enough space")

    def getUint(self, size):
        self.__checkSpace(size)
        value = int.from_bytes(
            self.bytes[self.position:self.position + size], byteorder="big")
        self.position += size
        return value

    def getUint8(self):
        return self.getUint(1)

    def getUint16(self):
        return self.getUint(2)

    def getUint32(self):
        return self.getUint(4)

    def getUint64(self):
        return self.getUint(8)

    def getUint256(self):
        return self.getUint(32)

    def getBytes(self, size):
        self.__checkSpace(size)
        value = self.bytes[self.position:self.position + size]
        self.position += size
        return value

    def getVarBytes(self):
        # TODO: Only support 1 byte len
        size = self.getUint8()
        return self.getBytes(size)

    def remaining(self):
        return len(self.bytes) - self.position


class UintSerializer():

    def __init__(self, size):
        self.size = size

    def serialize(self, value, barray):
        barray.extend(value.to_bytes(self.size, byteorder="big"))
        return barray

    def deserialize(self, bb):
        return bb.getUint(self.size)


class BooleanSerializer():

    def __init__(self):
        pass

    def serialize(self, value, barray):
        barray.append(1 if value else 0)
        return barray

    def deserialize(self, bb):
        return bool(bb.getUint8())


class FixedSizeBytesSerializer():

    def __init__(self, size):
        self.size = size

    def serialize(self, bs, barray):
        if len(bs) != self.size:
            raise RuntimeError("FixedSizeBytesSerializer input bytes size {} expect {}".format(len(bs), self.size))
        barray.extend(bs)
        return barray

    def deserialize(self, bb):
        return bb.getBytes(self.size)


class PrependedSizeBytesSerializer():

    def __init__(self, sizeBytes):
        self.sizeBytes = sizeBytes

    def serialize(self, bs, barray):
        if len(bs) >= (256 ** self.sizeBytes):
            raise RuntimeError("bytes size exceeds limit")
        barray.extend(len(bs).to_bytes(self.sizeBytes, byteorder="big"))
        barray.extend(bs)
        return barray

    def deserialize(self, bb):
        size = bb.getUint(self.sizeBytes)
        return bb.getBytes(size)


class PrependedSizeListSerializer():

    def __init__(self, sizeBytes, ser):
        self.sizeBytes = sizeBytes
        self.ser = ser

    def serialize(self, itemList, barray):
        barray.extend(len(itemList).to_bytes(self.sizeBytes, byteorder="big"))
        for item in itemList:
            self.ser.serialize(item, barray)
        return barray

    def deserialize(self, bb):
        size = bb.getUint(self.sizeBytes)
        return [self.ser.deserialize(bb) for i in range(size)]


class Serializable:

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def serialize(self, barray: bytearray = None):
        barray = bytearray() if barray is None else barray
        for name, ser in self.FIELDS:
            ser.serialize(getattr(self, name), barray)
        return barray

    def serializeWithout(self, excludeList, barray: bytearray = None):
        barray = bytearray() if barray is None else barray
        for name, ser in self.FIELDS:
            if name not in excludeList:
                ser.serialize(getattr(self, name), barray)
        return barray

    @classmethod
    def deserialize(cls, bb):
        if not isinstance(bb, ByteBuffer):
            bb = ByteBuffer(bb)
        kwargs = dict()
        for name, ser in cls.FIELDS:
            kwargs[name] = ser.deserialize(bb)
        return cls(**kwargs)

    def __eq__(self, other):
        for name, ser in self.FIELDS:
            if getattr(self, name) != getattr(other, name):
                return False
        return True

    def __hash__(self):
        hList = []
        for name, ser in self.FIELDS:
            hList.append(getattr(self, name))
        return hash(tuple(hList))


class Optional:

    def __init__(self, serializer):
        self.serializer = serializer

    def serialize(self, obj, barray):
        if obj is None:
            barray.append(0)
            return
        barray.append(1)
        self.serializer.serialize(obj, barray)

    def deserialize(self, bb):
        hasData = bb.getUint8()
        if hasData == 0:
            return None
        return self.serializer.deserialize(bb)


uint8 = UintSerializer(1)
uint16 = UintSerializer(2)
uint32 = UintSerializer(4)
uint64 = UintSerializer(8)
uint128 = UintSerializer(16)
uint256 = UintSerializer(32)
uint2048 = UintSerializer(2048)
hash256 = FixedSizeBytesSerializer(32)
boolean = BooleanSerializer()


def serialize_list(l: list, barray: bytearray, serializer=None) -> None:
    # TODO: use varint instead of a single byte
    barray.append(len(l))
    for item in l:
        if serializer is None:
            item.serialize(barray)
        else:
            serializer(item, barray)
    return barray


def deserialize_list(bb, der):
    size = bb.getUint8()
    ret = list()
    for i in range(size):
        ret.append(der(bb))
    return ret


def random_bytes(size):
    return bytearray([random.randint(0, 255) for i in range(size)])


def put_varbytes(barray: bytearray, bs: bytes) -> bytearray:
    # TODO: Only support 1 byte len
    barray.append(len(bs))
    barray.extend(bs)
    return barray


def normalize_bytes(data, size):
    if len(data) == size:
        return data
    if len(data) == 2 * size:
        return bytes.fromhex(data)
    raise RuntimeError("Unable to normalize bytes")


class Identity:

    @staticmethod
    def createRandomIdentity():
        sk = ecdsa.SigningKey.generate(curve=ecdsa.SECP256k1)
        key = sk.to_string()
        recipient = sha3_256(sk.verifying_key.to_string())[-20:]
        return Identity(recipient, key)

    @staticmethod
    def createFromKey(key):
        sk = ecdsa.SigningKey.from_string(key, curve=ecdsa.SECP256k1)
        recipient = sha3_256(sk.verifying_key.to_string())[-20:]
        return Identity(recipient, key)

    def __init__(self, recipient: bytes, key=None) -> None:
        self.recipient = recipient
        self.key = key

    def getRecipient(self):
        """ Get the recipient.  It is 40 bytes.
        """
        return self.recipient

    def getKey(self):
        """ Get the private key for sign.  It is 32 bytes and return none if it
        is unknown
        """
        return self.key


class Address(Serializable):
    FIELDS = [
        ("recipient", FixedSizeBytesSerializer(20)),
        ("fullShardId", uint32),
    ]

    def __init__(self, recipient: bytes, fullShardId: int) -> None:
        """
        recipient is 20 bytes SHA3 of public key
        shardId is uint32_t
        """
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def toHex(self):
        return self.serialize().hex()

    def getShardId(self, shardSize):
        if not is_p2(shardSize):
            raise RuntimeError("Invalid shard size {}".format(shardSize))
        return self.fullShardId & (shardSize - 1)

    def addressInShard(self, fullShardId):
        return Address(self.recipient, fullShardId)

    def addressInBranch(self, branch):
        return Address(self.recipient, (self.fullShardId & ~(branch.getShardSize() - 1)) + branch.getShardId())

    @staticmethod
    def createFromIdentity(identity: Identity, fullShardId=None):
        if fullShardId is None:
            fullShardId = random.randint(0, (2 ** 32) - 1)
        return Address(identity.getRecipient(), fullShardId)

    @staticmethod
    def createRandomAccount(fullShardId=None):
        """ An account is a special address with default shard that the
        account should be in.
        """
        if fullShardId is None:
            fullShardId = random.randint(0, (2 ** 32) - 1)
        return Address(Identity.createRandomIdentity().getRecipient(), fullShardId)

    @staticmethod
    def createEmptyAccount(fullShardId=0):
        return Address(bytes(20), fullShardId)

    @staticmethod
    def createFrom(bs):
        bs = normalize_bytes(bs, 24)
        return Address(bs[0:20], int.from_bytes(bs[20:], byteorder="big"))

    def isEmpty(self):
        return set(self.recipient) == {0}


class TransactionInput(Serializable):
    FIELDS = [
        ("hash", hash256),
        ("index", uint8),
    ]

    def __init__(self, hash: bytes, index: int) -> None:
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def getHashHex(self):
        return self.hash.hex()


class TransactionOutput(Serializable):
    FIELDS = [
        ("address", Address),
        ("quarkash", uint256),
    ]

    def __init__(self, address: Address, quarkash: int):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def getAddressHex(self):
        return self.address.serialize().hex()


class Branch(Serializable):
    FIELDS = [
        ("value", uint32),
    ]

    def __init__(self, value):
        self.value = value

    def getShardSize(self):
        return 1 << (int_left_most_bit(self.value) - 1)

    def getShardId(self):
        return self.value ^ self.getShardSize()

    def isInShard(self, fullShardId):
        return (fullShardId & (self.getShardSize() - 1)) == self.getShardId()

    @staticmethod
    def create(shardSize, shardId):
        assert(is_p2(shardSize))
        return Branch(shardSize | shardId)


class ShardMask(Serializable):
    ''' Represent a mask of shards, basically matches all the bits from the right until the leftmost bit is hit.
    E.g.,
    mask = 1, matches *
    mask = 0b10, matches *0
    mask = 0b101, matches *01
    '''
    FIELDS = [
        ("value", uint32),
    ]

    def __init__(self, value):
        check(value != 0)
        self.value = value

    def containShardId(self, shardId):
        bitMask = (1 << (int_left_most_bit(self.value) - 1)) - 1
        return (bitMask & shardId) == (self.value & bitMask)

    def containBranch(self, branch):
        return self.containShardId(branch.getShardId())

    def hasOverlap(self, shardMask):
        return masks_have_overlap(self.value, shardMask.value)

    def iterate(self, shardSize):
        shardBits = int_left_most_bit(shardSize)
        maskBits = int_left_most_bit(self.value) - 1
        bitMask = (1 << maskBits) - 1
        for i in range(1 << (shardBits - maskBits - 1)):
            yield (i << maskBits) + (bitMask & self.value)


class Code(Serializable):
    OP_TRANSFER = b'\1'
    OP_EVM = b'\2'
    OP_SHARD_COINBASE = b'm'
    OP_ROOT_COINBASE = b'r'
    # TODO: Replace it with vary-size bytes serializer
    FIELDS = [
        ("code", PrependedSizeBytesSerializer(4))
    ]

    def __init__(self, code=OP_TRANSFER):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    @classmethod
    def getTransferCode(cls):
        ''' Transfer quarkash from input list to output list
        If evm is None, then no balance is withdrawn from evm account.
        '''
        return Code(code=cls.OP_TRANSFER)

    @classmethod
    def createMinorBlockCoinbaseCode(cls, height, branch):
        return Code(code=cls.OP_SHARD_COINBASE + height.to_bytes(4, byteorder="big") + branch.serialize())

    @classmethod
    def createRootBlockCoinbaseCode(cls, height):
        return Code(code=cls.OP_ROOT_COINBASE + height.to_bytes(4, byteorder="big"))

    @classmethod
    def createEvmCode(cls, evmTx):
        return Code(cls.OP_EVM + rlp.encode(evmTx))

    def isValidOp(self):
        if len(self.code) == 0:
            return False
        return self.isTransfer() or self.isShardCoinbase() or self.isRootCoinbase() or self.isEvm()

    def isTransfer(self):
        return self.code[:1] == self.OP_TRANSFER

    def isShardCoinbase(self):
        return self.code[:1] == self.OP_SHARD_COINBASE

    def isRootCoinbase(self):
        return self.code[:1] == self.OP_ROOT_COINBASE

    def isEvm(self):
        return self.code[:1] == self.OP_EVM

    def getEvmTransaction(self):
        assert(self.isEvm())
        return rlp.decode(self.code[1:], EvmTransaction)


class Transaction(Serializable):
    FIELDS = [
        ("inList", PrependedSizeListSerializer(1, TransactionInput)),
        ("code", Code),
        ("outList", PrependedSizeListSerializer(1, TransactionOutput)),
        ("signList", PrependedSizeListSerializer(1, FixedSizeBytesSerializer(65)))
    ]

    def __init__(self, inList=None, code=Code(), outList=None, signList=None):
        inList = [] if inList is None else inList
        outList = [] if outList is None else outList
        signList = [] if signList is None else signList

        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def serializeUnsigned(self, barray: bytearray = None) -> bytearray:
        barray = barray if barray is not None else bytearray()
        return self.serializeWithout(["signList"], barray)

    def getHash(self):
        return sha3_256(self.serialize())

    def getHashHex(self):
        return self.getHash().hex()

    def getHashUnsigned(self):
        return sha3_256(self.serializeUnsigned())

    def sign(self, keys):
        """ Sign the transaction with keys.  It doesn't mean the transaction is valid in the chain since it doesn't
        check whether the txInput's addresses (recipents) match the keys
        """
        signList = []
        for key in keys:
            v, r, s = utils.ecsign(self.getHashUnsigned(), key)
            signList.append(
                v.to_bytes(1, byteorder="big") + r.to_bytes(32, byteorder="big") + s.to_bytes(32, byteorder="big"))
        self.signList = signList
        return self

    def verifySignature(self, recipients):
        """ Verify whether the signatures are from a list of recipients.  Doesn't verify if the transaction is valid on
        the chain
        """
        if len(recipients) != len(self.signList):
            return False

        for i in range(len(recipients)):
            bb = ByteBuffer(self.signList[i])
            v = bb.getUint8()
            r = bb.getUint256()
            s = bb.getUint256()
            if r >= secpk1n or s >= secpk1n or r == 0 or s == 0:
                return False
            pub = utils.ecrecover_to_pub(
                self.getHashUnsigned(), v, r, s)
            if sha3_256(pub)[-20:] != recipients[i]:
                return False
        return True


def calculate_merkle_root(itemList):
    if len(itemList) == 0:
        return bytes(32)

    shaTree = []
    for item in itemList:
        shaTree.append(sha3_256(item.serialize()))

    while len(shaTree) != 1:
        nextShaTree = []
        for i in range(0, len(shaTree) - 1, 2):
            nextShaTree.append(sha3_256(shaTree[i] + shaTree[i + 1]))
        if len(shaTree) % 2 != 0:
            nextShaTree.append(sha3_256(shaTree[-1] + shaTree[-1]))
        shaTree = nextShaTree
    return shaTree[0]


class ShardInfo(Serializable):
    """ Shard information contains
    - shard size (power of 2)
    - voting of increasing shards
    """
    FIELDS = [
        ("value", uint32),
    ]

    def __init__(self, value):
        self.value = value

    def getShardSize(self):
        return 1 << (self.value & 31)

    def getReshardVote(self):
        return (self.value & (1 << 31)) != 0

    @staticmethod
    def create(shardSize, reshardVote=False):
        assert(is_p2(shardSize))
        reshardVote = 1 if reshardVote else 0
        return ShardInfo(int_left_most_bit(shardSize) - 1 + (reshardVote << 31))


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
    def createFromEthLog(cls, ethLog):
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

    def getReceipt(self, db, i):
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


def test():
    rec = utils.privtoaddr(
        "208065a247edbe5df4d86fbdc0171303f23a76961be9f6013850dd2bdc759bbb")
    assert rec == b'\x0b\xedz\xbda$v5\xc1\x97>\xb3\x84t\xa2Qn\xd1\xd8\x84'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd")
    args = parser.parse_args()

    if args.cmd == "create_account":
        iden = Identity.createRandomIdentity()
        addr = Address.createFromIdentity(iden)
        print("Key: %s" % iden.getKey().hex())
        print("Account: %s" % addr.serialize().hex())


if __name__ == '__main__':
    main()
