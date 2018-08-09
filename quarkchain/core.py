#!/usr/bin/python3

# Core data structures
# Use standard ecsda, but should be replaced by ethereum's secp256k1
# implementation

import argparse
import copy
import random

import ecdsa
import rlp
from eth_keys import KeyAPI

import quarkchain.evm.messages
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

    def __check_space(self, space):
        if space > len(self.bytes) - self.position:
            raise RuntimeError("no enough space")

    def get_uint(self, size):
        self.__check_space(size)
        value = int.from_bytes(
            self.bytes[self.position:self.position + size], byteorder="big")
        self.position += size
        return value

    def get_uint8(self):
        return self.get_uint(1)

    def get_uint16(self):
        return self.get_uint(2)

    def get_uint32(self):
        return self.get_uint(4)

    def get_uint64(self):
        return self.get_uint(8)

    def get_uint256(self):
        return self.get_uint(32)

    def get_bytes(self, size):
        self.__check_space(size)
        value = self.bytes[self.position:self.position + size]
        self.position += size
        return value

    def get_var_bytes(self):
        # TODO: Only support 1 byte len
        size = self.get_uint8()
        return self.get_bytes(size)

    def remaining(self):
        return len(self.bytes) - self.position


class UintSerializer():

    def __init__(self, size):
        self.size = size

    def serialize(self, value, barray):
        barray.extend(value.to_bytes(self.size, byteorder="big"))
        return barray

    def deserialize(self, bb):
        return bb.get_uint(self.size)


class BooleanSerializer():

    def __init__(self):
        pass

    def serialize(self, value, barray):
        barray.append(1 if value else 0)
        return barray

    def deserialize(self, bb):
        return bool(bb.get_uint8())


class FixedSizeBytesSerializer():

    def __init__(self, size):
        self.size = size

    def serialize(self, bs, barray):
        if len(bs) != self.size:
            raise RuntimeError("FixedSizeBytesSerializer input bytes size {} expect {}".format(len(bs), self.size))
        barray.extend(bs)
        return barray

    def deserialize(self, bb):
        return bb.get_bytes(self.size)


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
        size = bb.get_uint(self.sizeBytes)
        return bb.get_bytes(size)


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
        size = bb.get_uint(self.sizeBytes)
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

    def serialize_without(self, excludeList, barray: bytearray = None):
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
        hasData = bb.get_uint8()
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
    size = bb.get_uint8()
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
    def create_random_identity():
        sk = ecdsa.SigningKey.generate(curve=ecdsa.SECP256k1)
        key = sk.to_string()
        recipient = sha3_256(sk.verifying_key.to_string())[-20:]
        return Identity(recipient, key)

    @staticmethod
    def create_from_key(key):
        sk = ecdsa.SigningKey.from_string(key, curve=ecdsa.SECP256k1)
        recipient = sha3_256(sk.verifying_key.to_string())[-20:]
        return Identity(recipient, key)

    def __init__(self, recipient: bytes, key=None) -> None:
        self.recipient = recipient
        self.key = key

    def get_recipient(self):
        """ Get the recipient.  It is 40 bytes.
        """
        return self.recipient

    def get_key(self):
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

    def to_hex(self):
        return self.serialize().hex()

    def get_shard_id(self, shardSize):
        if not is_p2(shardSize):
            raise RuntimeError("Invalid shard size {}".format(shardSize))
        return self.fullShardId & (shardSize - 1)

    def address_in_shard(self, fullShardId):
        return Address(self.recipient, fullShardId)

    def address_in_branch(self, branch):
        return Address(self.recipient, (self.fullShardId & ~(branch.get_shard_size() - 1)) + branch.get_shard_id())

    @staticmethod
    def create_from_identity(identity: Identity, fullShardId=None):
        if fullShardId is None:
            fullShardId = random.randint(0, (2 ** 32) - 1)
        return Address(identity.get_recipient(), fullShardId)

    @staticmethod
    def create_random_account(fullShardId=None):
        """ An account is a special address with default shard that the
        account should be in.
        """
        if fullShardId is None:
            fullShardId = random.randint(0, (2 ** 32) - 1)
        return Address(Identity.create_random_identity().get_recipient(), fullShardId)

    @staticmethod
    def create_empty_account(fullShardId=0):
        return Address(bytes(20), fullShardId)

    @staticmethod
    def create_from(bs):
        bs = normalize_bytes(bs, 24)
        return Address(bs[0:20], int.from_bytes(bs[20:], byteorder="big"))

    def is_empty(self):
        return set(self.recipient) == {0}


class TransactionInput(Serializable):
    FIELDS = [
        ("hash", hash256),
        ("index", uint8),
    ]

    def __init__(self, hash: bytes, index: int) -> None:
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def get_hash_hex(self):
        return self.hash.hex()


class TransactionOutput(Serializable):
    FIELDS = [
        ("address", Address),
        ("quarkash", uint256),
    ]

    def __init__(self, address: Address, quarkash: int):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def get_address_hex(self):
        return self.address.serialize().hex()


class Branch(Serializable):
    FIELDS = [
        ("value", uint32),
    ]

    def __init__(self, value):
        self.value = value

    def get_shard_size(self):
        return 1 << (int_left_most_bit(self.value) - 1)

    def get_shard_id(self):
        return self.value ^ self.get_shard_size()

    def is_in_shard(self, fullShardId):
        return (fullShardId & (self.get_shard_size() - 1)) == self.get_shard_id()

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

    def contain_shard_id(self, shardId):
        bitMask = (1 << (int_left_most_bit(self.value) - 1)) - 1
        return (bitMask & shardId) == (self.value & bitMask)

    def contain_branch(self, branch):
        return self.contain_shard_id(branch.get_shard_id())

    def has_overlap(self, shardMask):
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
    def get_transfer_code(cls):
        ''' Transfer quarkash from input list to output list
        If evm is None, then no balance is withdrawn from evm account.
        '''
        return Code(code=cls.OP_TRANSFER)

    @classmethod
    def create_minor_block_coinbase_code(cls, height, branch):
        return Code(code=cls.OP_SHARD_COINBASE + height.to_bytes(4, byteorder="big") + branch.serialize())

    @classmethod
    def create_root_block_coinbase_code(cls, height):
        return Code(code=cls.OP_ROOT_COINBASE + height.to_bytes(4, byteorder="big"))

    @classmethod
    def create_evm_code(cls, evmTx):
        return Code(cls.OP_EVM + rlp.encode(evmTx))

    def is_valid_op(self):
        if len(self.code) == 0:
            return False
        return self.is_transfer() or self.is_shard_coinbase() or self.is_root_coinbase() or self.is_evm()

    def is_transfer(self):
        return self.code[:1] == self.OP_TRANSFER

    def is_shard_coinbase(self):
        return self.code[:1] == self.OP_SHARD_COINBASE

    def is_root_coinbase(self):
        return self.code[:1] == self.OP_ROOT_COINBASE

    def is_evm(self):
        return self.code[:1] == self.OP_EVM

    def get_evm_transaction(self):
        assert(self.is_evm())
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

    def serialize_unsigned(self, barray: bytearray = None) -> bytearray:
        barray = barray if barray is not None else bytearray()
        return self.serialize_without(["signList"], barray)

    def get_hash(self):
        return sha3_256(self.serialize())

    def get_hash_hex(self):
        return self.get_hash().hex()

    def get_hash_unsigned(self):
        return sha3_256(self.serialize_unsigned())

    def sign(self, keys):
        """ Sign the transaction with keys.  It doesn't mean the transaction is valid in the chain since it doesn't
        check whether the txInput's addresses (recipents) match the keys
        """
        signList = []
        for key in keys:
            sig = KeyAPI.PrivateKey(key).sign_msg(self.get_hash_unsigned())
            signList.append(
                sig.r.to_bytes(32, byteorder="big") +
                sig.s.to_bytes(32, byteorder="big") +
                sig.v.to_bytes(1, byteorder="big"))
        self.signList = signList
        return self

    def verify_signature(self, recipients):
        """ Verify whether the signatures are from a list of recipients.  Doesn't verify if the transaction is valid on
        the chain
        """
        if len(recipients) != len(self.signList):
            return False

        for i in range(len(recipients)):
            sig = KeyAPI.Signature(signature_bytes=self.signList[i])
            pub = sig.recover_public_key_from_msg(self.get_hash_unsigned())
            if pub.to_canonical_address() != recipients[i]:
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

    def get_shard_size(self):
        return 1 << (self.value & 31)

    def get_reshard_vote(self):
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
    def create_fromEthLog(cls, ethLog):
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
    def create_empty_receipt(cls):
        return cls(b'', 0, 0, Address.create_empty_account(0), 0, [])


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
                 coinbaseAddress=Address.create_empty_account(),
                 evmGasLimit=30000 * 400,  # 400 xshard tx
                 evmGasUsed=0,
                 evmCrossShardReceiveGasUsed=0,
                 extraData=b''):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def get_hash(self):
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

    def get_hash(self):
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

    def calculate_merkle_root(self):
        return calculate_merkle_root(self.txList)

    def finalize_merkle_root(self):
        """ Compute merkle root hash and put it in the field
        """
        self.meta.hashMerkleRoot = self.calculate_merkle_root()
        return self

    def finalize(self, evmState, hashPrevRootBlock=None):
        if hashPrevRootBlock is not None:
            self.header.hashPrevRootBlock = hashPrevRootBlock
        self.meta.hashEvmStateRoot = evmState.trie.root_hash
        self.meta.evmGasUsed = evmState.gas_used
        self.meta.evmCrossShardReceiveGasUsed = evmState.xshard_receive_gas_used
        self.header.coinbaseAmount = evmState.block_fee // 2
        self.finalize_merkle_root()
        self.meta.hashEvmReceiptRoot = mk_receipt_sha(evmState.receipts, evmState.db)
        self.header.hashMeta = self.meta.get_hash()
        return self

    def add_tx(self, tx):
        self.txList.append(tx)
        return self

    def get_receipt(self, db, i):
        # ignore if no meta is set
        if self.meta.hashEvmReceiptRoot == bytes(Constant.HASH_LENGTH):
            return None

        t = trie.Trie(db, self.meta.hashEvmReceiptRoot)
        receipt = rlp.decode(t.get(rlp.encode(i)), quarkchain.evm.messages.Receipt)
        if receipt.contract_address != b'':
            contractAddress = Address(receipt.contract_address, receipt.contract_full_shard_id)
        else:
            contractAddress = Address.create_empty_account(fullShardId=0)

        if i > 0:
            prevGasUsed = rlp.decode(t.get(rlp.encode(i - 1)), quarkchain.evm.messages.Receipt).gas_used
        else:
            prevGasUsed = self.meta.evmCrossShardReceiveGasUsed

        logs = [Log.create_fromEthLog(ethLog) for ethLog in receipt.logs]

        return TransactionReceipt(
            receipt.state_root,
            receipt.gas_used,
            prevGasUsed,
            contractAddress,
            receipt.bloom,
            logs,
        )

    def create_block_to_append(self,
                            createTime=None,
                            address=None,
                            quarkash=0,
                            difficulty=None,
                            extraData=b'',
                            nonce=0):
        if address is None:
            address = Address.create_empty_account(fullShardId=self.meta.coinbaseAddress.fullShardId)
        meta = MinorBlockMeta(coinbaseAddress=address,
                              evmGasLimit=self.meta.evmGasLimit,
                              extraData=extraData)

        createTime = self.header.createTime + 1 if createTime is None else createTime
        difficulty = self.header.difficulty if difficulty is None else difficulty
        header = MinorBlockHeader(version=self.header.version,
                                  height=self.header.height + 1,
                                  branch=self.header.branch,
                                  coinbaseAmount=quarkash,
                                  hashPrevMinorBlock=self.header.get_hash(),
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
                 coinbaseAddress=Address.create_empty_account(),
                 coinbaseAmount=0,
                 createTime=0,
                 difficulty=0,
                 nonce=0):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)

    def get_hash(self):
        return sha3_256(self.serialize())

    def create_block_to_append(self, createTime=None, difficulty=None, address=None, nonce=0):
        createTime = self.createTime + 1 if createTime is None else createTime
        difficulty = difficulty if difficulty is not None else self.difficulty
        header = RootBlockHeader(version=self.version,
                                 height=self.height + 1,
                                 shardInfo=copy.copy(self.shardInfo),
                                 hashPrevBlock=self.get_hash(),
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

    def finalize(self, quarkash=0, coinbaseAddress=Address.create_empty_account()):
        self.header.hashMerkleRoot = calculate_merkle_root(
            self.minorBlockHeaderList)

        self.header.coinbaseAmount = quarkash
        self.header.coinbaseAddress = coinbaseAddress
        return self

    def add_minor_block_header(self, header):
        self.minorBlockHeaderList.append(header)
        return self

    def extend_minor_block_header_list(self, headerList):
        self.minorBlockHeaderList.extend(headerList)
        return self

    def create_block_to_append(self, createTime=None, difficulty=None, address=None, nonce=0):
        return self.header.create_block_to_append(
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
    priv = KeyAPI.PrivateKey(bytes.fromhex("208065a247edbe5df4d86fbdc0171303f23a76961be9f6013850dd2bdc759bbb"))
    addr = priv.public_key.to_canonical_address()
    assert addr == b'\x0b\xedz\xbda$v5\xc1\x97>\xb3\x84t\xa2Qn\xd1\xd8\x84'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd")
    args = parser.parse_args()

    if args.cmd == "create_account":
        iden = Identity.create_random_identity()
        addr = Address.create_from_identity(iden)
        print("Key: %s" % iden.get_key().hex())
        print("Account: %s" % addr.serialize().hex())


if __name__ == '__main__':
    main()
