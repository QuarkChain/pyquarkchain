#!/usr/bin/python3

# Core data structures
# Use standard ecsda, but should be replaced by ethereum's secp256k1
# implementation


import argparse
import copy
import ecdsa
from quarkchain.utils import int_left_most_bit, is_p2, sha3_256
from ethereum import utils
import random
import time


class ByteBuffer:
    """ Java-like ByteBuffer, which wraps a bytes or bytearray with position.
    If there is no enough space during deserialization, throw exception
    """

    def __init__(self, bytes):
        self.bytes = bytes
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


class FixedSizeBytesSerializer():

    def __init__(self, size):
        self.size = size

    def serialize(self, bs, barray):
        if len(bs) != self.size:
            raise RuntimeError("bytes size mismatch")
        barray.extend(bs)
        return barray

    def deserialize(self, bb):
        return bb.getBytes(self.size)


class PreprendedSizeBytesSerializer():

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


class PreprendedSizeListSerializer():

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


class Serializable():

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


uint8 = UintSerializer(1)
uint16 = UintSerializer(2)
uint32 = UintSerializer(4)
uint64 = UintSerializer(8)
uint128 = UintSerializer(16)
uint256 = UintSerializer(32)
hash256 = FixedSizeBytesSerializer(32)


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
    def createEmptyAccount():
        return Address(bytes(20), 0)

    @staticmethod
    def createFrom(bs):
        bs = normalize_bytes(bs, 24)
        return Address(bs[0:20], int.from_bytes(bs[20:], byteorder="big"))


class TransactionInput(Serializable):
    FIELDS = [
        ("hash", hash256),
        ("index", uint8),
    ]

    def __init__(self, hash: bytes, index: int) -> None:
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


class TransactionOutput(Serializable):
    FIELDS = [
        ("address", Address),
        ("quarkash", uint256),
    ]

    def __init__(self, address: Address, quarkash: int):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


class Code(Serializable):
    OP_TRANSFER = b'1'
    # TODO: Replace it with vary-size bytes serializer
    FIELDS = [
        ("code", PreprendedSizeBytesSerializer(1))
    ]

    def __init__(self, code=OP_TRANSFER):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    @staticmethod
    def getTransferCode():
        return Code()

    @staticmethod
    def createMinorBlockCoinbaseCode(height, branch):
        return Code(code=b'm' + height.to_bytes(4, byteorder="big") + branch.value.to_bytes(4, byteorder="big"))

    @staticmethod
    def createRootBlockCoinbaseCode(height):
        return Code(code=b'r' + height.to_bytes(4, byteorder="big"))


class Transaction(Serializable):
    FIELDS = [
        ("inList", PreprendedSizeListSerializer(1, TransactionInput)),
        ("code", Code),
        ("outList", PreprendedSizeListSerializer(1, TransactionOutput)),
        ("signList", PreprendedSizeListSerializer(1, FixedSizeBytesSerializer(65)))
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

    def sign(self, keys):
        """ Sign the transaction with keys.  It doesn't mean the transaction is valid in the chain since it doesn't
        check whether the txInput's addresses (recipents) match the keys
        """
        signList = []
        for key in keys:
            v, r, s = utils.ecsign(sha3_256(self.serializeUnsigned()), key)
            signList.append(
                v.to_bytes(1, byteorder="big") + r.to_bytes(32, byteorder="big") + s.to_bytes(32, byteorder="big"))
        self.signList = signList

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
            pub = utils.ecrecover_to_pub(
                sha3_256(self.serializeUnsigned()), v, r, s)
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
        return Branch(shardSize | shardId)


class MinorBlockHeader(Serializable):
    """ TODO: Add uncles
    """
    FIELDS = [
        ("version", uint32),
        ("height", uint32),
        ("branch", Branch),
        ("hashPrevRootBlock", hash256),
        ("hashPrevMinorBlock", hash256),
        ("hashMerkleRoot", hash256),
        ("createTime", uint32),
        ("difficulty", uint32),
        ("nonce", uint32)
    ]

    def __init__(self,
                 version=0,
                 height=0,
                 branch=Branch.create(1, 0),
                 hashPrevRootBlock=bytes(32),
                 hashPrevMinorBlock=bytes(32),
                 hashMerkleRoot=bytes(32),
                 createTime=0,
                 difficulty=0,
                 nonce=0):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def getHash(self):
        return sha3_256(self.serialize())

    def createBlockToAppend(self,
                            address=None,
                            quarkash=0,
                            createTime=None,
                            difficulty=None):
        createTime = self.createTime + 1 if createTime is None else createTime
        address = Address.createEmptyAccount() if address is None else address
        difficulty = difficulty if difficulty is not None else self.difficulty
        header = MinorBlockHeader(version=self.version,
                                  height=self.height + 1,
                                  branch=self.branch,
                                  hashPrevRootBlock=self.hashPrevRootBlock,
                                  hashPrevMinorBlock=self.getHash(),
                                  hashMerkleRoot=bytes(32),
                                  createTime=createTime,
                                  difficulty=difficulty,
                                  nonce=0)
        return MinorBlock(header, [Transaction(
            inList=[],
            code=Code.createMinorBlockCoinbaseCode(header.height, header.branch),
            outList=[TransactionOutput(address.addressInBranch(self.branch), quarkash)])])


class MinorBlock(Serializable):
    FIELDS = [
        ("header", MinorBlockHeader),
        ("txList", PreprendedSizeListSerializer(4, Transaction))
    ]

    def __init__(self, header, txList=None):
        self.header = header
        self.txList = [] if txList is None else txList

    def calculateMerkleRoot(self):
        return calculate_merkle_root(self.txList)

    def finalizeMerkleRoot(self):
        """ Compute merkle root hash and put it in the field
        """
        self.header.hashMerkleRoot = self.calculateMerkleRoot()
        return self

    def addTx(self, tx):
        self.txList.append(tx)
        return self

    def createBlockToAppend(self,
                            address=Address.createEmptyAccount(),
                            quarkash=0,
                            createTime=None,
                            difficulty=None):
        return self.header.createBlockToAppend(
            address=address,
            quarkash=quarkash,
            createTime=createTime,
            difficulty=difficulty)


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


class RootBlockHeader(Serializable):
    FIELDS = [
        ("version", uint32),
        ("height", uint32),
        ("shardInfo", ShardInfo),
        ("hashPrevBlock", hash256),
        ("hashMerkleRoot", hash256),
        ("hashCoinbaseTx", hash256),
        ("createTime", uint32),
        ("difficulty", uint32),
        ("nonce", uint32)]

    def __init__(self,
                 version=0,
                 height=0,
                 shardInfo=ShardInfo.create(1, False),
                 hashPrevBlock=bytes(32),
                 hashMerkleRoot=bytes(32),
                 hashCoinbaseTx=bytes(32),
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
                                 hashCoinbaseTx=bytes(32),
                                 createTime=createTime,
                                 difficulty=difficulty,
                                 nonce=0)
        return RootBlock(
            header,
            Transaction(
                inList=[],
                code=Code.createRootBlockCoinbaseCode(header.height),
                outList=[TransactionOutput(address, 0)]),
            [])


class RootBlock(Serializable):
    FIELDS = [
        ("header", RootBlockHeader),
        ("coinbaseTx", Transaction),
        ("minorBlockHeaderList", PreprendedSizeListSerializer(4, MinorBlockHeader))
    ]

    def __init__(self, header, coinbaseTx, minorBlockHeaderList=None):
        self.header = header
        self.coinbaseTx = coinbaseTx
        self.minorBlockHeaderList = [] if minorBlockHeaderList is None else minorBlockHeaderList

    def finalize(self, quarkash=0):
        self.header.hashMerkleRoot = calculate_merkle_root(
            self.minorBlockHeaderList)

        self.coinbaseTx.outList[0].quarkash = quarkash
        self.header.hashCoinbaseTx = self.coinbaseTx.getHash()

        return self

    def addMinorBlockHeader(self, header):
        self.minorBlockHeaderList.append(header)
        return self

    def extendMinorBlockHeaderList(self, headerList):
        self.minorBlockHeaderList.extend(headerList)
        return self

    def createBlockToAppend(self, createTime=None, difficulty=None, address=None):
        return self.header.createBlockToAppend(createTime=createTime, difficulty=difficulty, address=address)


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
