#!/usr/bin/python3

# Core data structures
# Use standard ecsda, but should be replaced by ethereum's secp256k1
# implementation


import ecdsa
from ethereum import utils
import random


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


uint8 = UintSerializer(1)
uint16 = UintSerializer(2)
uint32 = UintSerializer(4)
uint64 = UintSerializer(8)
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
        recipient = utils.sha3(sk.verifying_key.to_string())[-20:]
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
        ("shardId", uint32),
    ]

    def __init__(self, recipient: bytes, shardId: int) -> None:
        """
        recipient is 20 bytes SHA3 of public key
        shardId is uint32_t
        """
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    @staticmethod
    def createFromIdentity(identity: Identity, shardId=None):
        if shardId is None:
            shardId = random.randint(0, (2 ** 32) - 1)
        return Address(identity.getRecipient(), shardId)

    @staticmethod
    def createRandomAccount():
        """ An account is a special address with default shard that the
        account should be in.
        """
        shardId = random.randint(0, (2 ** 32) - 1)
        return Address(Identity.createRandomIdentity().getRecipient(), shardId)

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
        ("code", FixedSizeBytesSerializer(1))
    ]

    def __init__(self, code=OP_TRANSFER):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


class Transaction(Serializable):
    FIELDS = [
        ("inList", PreprendedSizeListSerializer(1, TransactionInput)),
        ("code", Code),
        ("outList", PreprendedSizeListSerializer(1, TransactionOutput)),
        ("signList", PreprendedSizeListSerializer(1, FixedSizeBytesSerializer(65)))
    ]

    def __init__(self, inList=[], code=[], outList=[], signList=[]):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def serializeUnsigned(self, barray: bytearray = bytearray()) -> bytearray:
        return self.serializeWithout(["signList"], barray)

    def sign(self, keys):
        """ Sign the transaction with keys.  It doesn't mean the transaction is valid in the chain since it doesn't
        check whether the txInput's addresses (recipents) match the keys
        """
        signList = []
        for key in keys:
            v, r, s = utils.ecsign(utils.sha3(self.serializeUnsigned()), key)
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
                utils.sha3(self.serializeUnsigned()), v, r, s)
            if utils.sha3(pub)[-20:] != recipients[i]:
                return False
        return True


def calculate_merkle_root(itemList):
    shaTree = []
    for item in itemList:
        shaTree.append(utils.sha3(item.serialize()))

    while len(shaTree) != 1:
        nextShaTree = []
        for i in range(0, len(shaTree) - 1, 2):
            nextShaTree.append(utils.sha3(shaTree[i] + shaTree[i + 1]))
        if len(shaTree) % 2 != 0:
            nextShaTree.append(utils.sha3(shaTree[-1] + shaTree[-1]))
        shaTree = nextShaTree
    return shaTree[0]


class MinorBlockHeader(Serializable):
    """ TODO: Add uncles
    """
    FIELDS = [
        ("version", uint32),
        ("height", uint32),
        ("branch", uint32),
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
                 branch=1,      # Left most bit indicate # of shards
                 hashPrevRootBlock=bytes(256),
                 hashPrevMinorBlock=bytes(256),
                 hashMerkleRoot=bytes(256),
                 createTime=0,
                 difficulty=0,
                 nonce=0):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def getHash(self):
        return utils.sha3(self.serialize())


class MinorBlock():

    def __init__(self, header, txList=[]):
        self.header = header
        self.txList = txList

    def calculateMerkleRoot(self):
        return calculate_merkle_root(self.txList)

    def serialize(self, barray: bytearray = bytearray()) -> bytearray:
        self.header.serialize(barray)
        serialize_list(self.txList, barray)
        return barray

    @staticmethod
    def deserialize(bb: ByteBuffer):
        return MinorBlock(
            MinorBlockHeader.deserialize(bb), deserialize_list(bb, lambda bb: Transaction.deserialize(bb)))

    def __eq__(self, other):
        return self.header == other.header and self.txList == other.txList


class RootBlockHeader(Serializable):
    FIELDS = [
        ("version", uint32),
        ("height", uint32),
        ("shardInfo", uint32),
        ("hashPrevBlock", hash256),
        ("hashMerkleRoot", hash256),
        ("coinbaseAddress", Address),
        ("coinbaseValue", uint256),
        ("createTime", uint32),
        ("difficulty", uint32),
        ("nonce", uint32)]

    def __init__(self,
                 version=0,
                 height=0,
                 shardInfo=1,
                 hashPrevBlock=bytes(32),
                 hashMerkleRoot=bytes(32),
                 coinbaseAddress=Address.createEmptyAccount(),
                 coinbaseValue=0,
                 createTime=0,
                 difficulty=0,
                 nonce=0):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


class RootBlock(Serializable):
    FIELDS = [
        ("header", RootBlockHeader),
        ("minorBlockHeaderList", PreprendedSizeListSerializer(1, MinorBlockHeader))
    ]

    def __init__(self, header, minorBlockHeaderList=[]):
        self.header = header
        self.minorBlockHeaderList = minorBlockHeaderList


def main():
    rec = utils.privtoaddr(
        "208065a247edbe5df4d86fbdc0171303f23a76961be9f6013850dd2bdc759bbb")
    assert rec == b'\x0b\xedz\xbda$v5\xc1\x97>\xb3\x84t\xa2Qn\xd1\xd8\x84'

    header = RootBlockHeader(
        version=0,
        height=1,
        shardInfo=2,
        hashPrevBlock=random_bytes(32),
        hashMerkleRoot=random_bytes(32),
        createTime=1234,
        difficulty=4,
        nonce=5)
    barray = header.serialize()
    bb = ByteBuffer(barray)
    header1 = RootBlockHeader.deserialize(bb)
    assert bb.remaining() == 0
    assert header == header1


if __name__ == '__main__':
    main()
