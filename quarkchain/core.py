#!/usr/bin/python3

# Core data structures
# Use standard ecsda, but should be replaced by ethereum's secp256k1
# implementation

import argparse
import copy
import random
from typing import List

import ecdsa
import rlp
from eth_keys import KeyAPI

import quarkchain.evm.messages
from quarkchain.evm import trie
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import (
    int_left_most_bit,
    is_p2,
    sha3_256,
    check,
    masks_have_overlap,
)

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
            raise RuntimeError("buffer is shorter than expected")

    def get_uint(self, size):
        self.__check_space(size)
        value = int.from_bytes(
            self.bytes[self.position : self.position + size], byteorder="big"
        )
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
        value = self.bytes[self.position : self.position + size]
        self.position += size
        return value

    def get_var_bytes(self):
        # TODO: Only support 1 byte len
        size = self.get_uint8()
        return self.get_bytes(size)

    def remaining(self):
        return len(self.bytes) - self.position


class UintSerializer:
    def __init__(self, size):
        self.size = size

    def serialize(self, value, barray):
        barray.extend(value.to_bytes(self.size, byteorder="big"))
        return barray

    def deserialize(self, bb):
        return bb.get_uint(self.size)


class BooleanSerializer:
    def __init__(self):
        pass

    def serialize(self, value, barray):
        barray.append(1 if value else 0)
        return barray

    def deserialize(self, bb):
        return bool(bb.get_uint8())


class FixedSizeBytesSerializer:
    def __init__(self, size):
        self.size = size

    def serialize(self, bs, barray):
        if len(bs) != self.size:
            raise RuntimeError(
                "FixedSizeBytesSerializer input bytes size {} expect {}".format(
                    len(bs), self.size
                )
            )
        barray.extend(bs)
        return barray

    def deserialize(self, bb):
        return bb.get_bytes(self.size)


class PrependedSizeBytesSerializer:
    def __init__(self, size_bytes):
        self.size_bytes = size_bytes

    def serialize(self, bs, barray):
        if len(bs) >= (256 ** self.size_bytes):
            raise RuntimeError("bytes size exceeds limit")
        barray.extend(len(bs).to_bytes(self.size_bytes, byteorder="big"))
        barray.extend(bs)
        return barray

    def deserialize(self, bb):
        size = bb.get_uint(self.size_bytes)
        return bb.get_bytes(size)


class PrependedSizeListSerializer:
    def __init__(self, size_bytes, ser):
        self.size_bytes = size_bytes
        self.ser = ser

    def serialize(self, item_list, barray):
        barray.extend(len(item_list).to_bytes(self.size_bytes, byteorder="big"))
        for item in item_list:
            self.ser.serialize(item, barray)
        return barray

    def deserialize(self, bb):
        size = bb.get_uint(self.size_bytes)
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

    def serialize_without(self, exclude_list, barray: bytearray = None):
        barray = bytearray() if barray is None else barray
        for name, ser in self.FIELDS:
            if name not in exclude_list:
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
        h_list = []
        for name, ser in self.FIELDS:
            h_list.append(getattr(self, name))
        return hash(tuple(h_list))


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
        has_data = bb.get_uint8()
        if has_data == 0:
            return None
        return self.serializer.deserialize(bb)


uint8 = UintSerializer(1)
uint16 = UintSerializer(2)
uint32 = UintSerializer(4)
uint64 = UintSerializer(8)
uint128 = UintSerializer(16)
uint256 = UintSerializer(32)
uint2048 = UintSerializer(256)
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
    FIELDS = [("recipient", FixedSizeBytesSerializer(20)), ("full_shard_id", uint32)]

    def __init__(self, recipient: bytes, full_shard_id: int) -> None:
        """
        recipient is 20 bytes SHA3 of public key
        shard_id is uint32_t
        """
        self.recipient = recipient
        self.full_shard_id = full_shard_id

    def to_hex(self):
        return self.serialize().hex()

    def get_shard_id(self, shard_size):
        if not is_p2(shard_size):
            raise RuntimeError("Invalid shard size {}".format(shard_size))
        return self.full_shard_id & (shard_size - 1)

    def address_in_shard(self, full_shard_id):
        return Address(self.recipient, full_shard_id)

    def address_in_branch(self, branch):
        return Address(
            self.recipient,
            (self.full_shard_id & ~(branch.get_shard_size() - 1))
            + branch.get_shard_id(),
        )

    @staticmethod
    def create_from_identity(identity: Identity, full_shard_id=None):
        if full_shard_id is None:
            r = identity.get_recipient()
            full_shard_id = int.from_bytes(r[0:1] + r[5:6] + r[10:11] + r[15:16], "big")
        return Address(identity.get_recipient(), full_shard_id)

    @staticmethod
    def create_random_account(full_shard_id=None):
        """ An account is a special address with default shard that the
        account should be in.
        """
        return Address.create_from_identity(
            Identity.create_random_identity(), full_shard_id
        )

    @staticmethod
    def create_empty_account(full_shard_id=0):
        return Address(bytes(20), full_shard_id)

    @staticmethod
    def create_from(bs):
        bs = normalize_bytes(bs, 24)
        return Address(bs[0:20], int.from_bytes(bs[20:], byteorder="big"))

    def is_empty(self):
        return set(self.recipient) == {0}


class TransactionInput(Serializable):
    FIELDS = [("hash", hash256), ("index", uint8)]

    def __init__(self, hash: bytes, index: int) -> None:
        fields = {k: v for k, v in locals().items() if k != "self"}
        super(type(self), self).__init__(**fields)

    def get_hash_hex(self):
        return self.hash.hex()


class TransactionOutput(Serializable):
    FIELDS = [("address", Address), ("quarkash", uint256)]

    def __init__(self, address: Address, quarkash: int):
        fields = {k: v for k, v in locals().items() if k != "self"}
        super(type(self), self).__init__(**fields)

    def get_address_hex(self):
        return self.address.serialize().hex()


class Branch(Serializable):
    FIELDS = [("value", uint32)]

    def __init__(self, value: int):
        self.value = value

    def get_shard_size(self):
        return 1 << (int_left_most_bit(self.value) - 1)

    def get_shard_id(self):
        return self.value ^ self.get_shard_size()

    def is_in_shard(self, full_shard_id):
        return (full_shard_id & (self.get_shard_size() - 1)) == self.get_shard_id()

    @staticmethod
    def create(shard_size, shard_id):
        assert is_p2(shard_size)
        return Branch(shard_size | shard_id)


class ShardMask(Serializable):
    """ Represent a mask of shards, basically matches all the bits from the right until the leftmost bit is hit.
    E.g.,
    mask = 1, matches *
    mask = 0b10, matches *0
    mask = 0b101, matches *01
    """

    FIELDS = [("value", uint32)]

    def __init__(self, value):
        check(value != 0)
        self.value = value

    def contain_shard_id(self, shard_id):
        bit_mask = (1 << (int_left_most_bit(self.value) - 1)) - 1
        return (bit_mask & shard_id) == (self.value & bit_mask)

    def contain_branch(self, branch):
        return self.contain_shard_id(branch.get_shard_id())

    def has_overlap(self, shard_mask):
        return masks_have_overlap(self.value, shard_mask.value)

    def iterate(self, shard_size):
        shard_bits = int_left_most_bit(shard_size)
        mask_bits = int_left_most_bit(self.value) - 1
        bit_mask = (1 << mask_bits) - 1
        for i in range(1 << (shard_bits - mask_bits - 1)):
            yield (i << mask_bits) + (bit_mask & self.value)


class Code(Serializable):
    OP_TRANSFER = b"\1"
    OP_EVM = b"\2"
    OP_SHARD_COINBASE = b"m"
    OP_ROOT_COINBASE = b"r"
    # TODO: Replace it with vary-size bytes serializer
    FIELDS = [("code", PrependedSizeBytesSerializer(4))]

    def __init__(self, code=OP_TRANSFER):
        fields = {k: v for k, v in locals().items() if k != "self"}
        super(type(self), self).__init__(**fields)

    @classmethod
    def get_transfer_code(cls):
        """ Transfer quarkash from input list to output list
        If evm is None, then no balance is withdrawn from evm account.
        """
        return Code(code=cls.OP_TRANSFER)

    @classmethod
    def create_minor_block_coinbase_code(cls, height, branch):
        return Code(
            code=cls.OP_SHARD_COINBASE
            + height.to_bytes(4, byteorder="big")
            + branch.serialize()
        )

    @classmethod
    def create_root_block_coinbase_code(cls, height):
        return Code(code=cls.OP_ROOT_COINBASE + height.to_bytes(4, byteorder="big"))

    @classmethod
    def create_evm_code(cls, evm_tx):
        return Code(cls.OP_EVM + rlp.encode(evm_tx))

    def is_valid_op(self):
        if len(self.code) == 0:
            return False
        return (
            self.is_transfer()
            or self.is_shard_coinbase()
            or self.is_root_coinbase()
            or self.is_evm()
        )

    def is_transfer(self):
        return self.code[:1] == self.OP_TRANSFER

    def is_shard_coinbase(self):
        return self.code[:1] == self.OP_SHARD_COINBASE

    def is_root_coinbase(self):
        return self.code[:1] == self.OP_ROOT_COINBASE

    def is_evm(self):
        return self.code[:1] == self.OP_EVM

    def get_evm_transaction(self) -> EvmTransaction:
        assert self.is_evm()
        return rlp.decode(self.code[1:], EvmTransaction)


class Transaction(Serializable):
    FIELDS = [
        ("in_list", PrependedSizeListSerializer(1, TransactionInput)),
        ("code", Code),
        ("out_list", PrependedSizeListSerializer(1, TransactionOutput)),
        ("sign_list", PrependedSizeListSerializer(1, FixedSizeBytesSerializer(65))),
    ]

    def __init__(self, in_list=None, code=Code(), out_list=None, sign_list=None):
        self.in_list = [] if in_list is None else in_list
        self.out_list = [] if out_list is None else out_list
        self.sign_list = [] if sign_list is None else sign_list
        self.code = code

    def serialize_unsigned(self, barray: bytearray = None) -> bytearray:
        barray = barray if barray is not None else bytearray()
        return self.serialize_without(["sign_list"], barray)

    def get_hash(self):
        return sha3_256(self.serialize())

    def get_hash_hex(self):
        return self.get_hash().hex()

    def get_hash_unsigned(self):
        return sha3_256(self.serialize_unsigned())

    def sign(self, keys):
        """ Sign the transaction with keys.  It doesn't mean the transaction is valid in the chain since it doesn't
        check whether the tx_input's addresses (recipents) match the keys
        """
        sign_list = []
        for key in keys:
            sig = KeyAPI.PrivateKey(key).sign_msg(self.get_hash_unsigned())
            sign_list.append(
                sig.r.to_bytes(32, byteorder="big")
                + sig.s.to_bytes(32, byteorder="big")
                + sig.v.to_bytes(1, byteorder="big")
            )
        self.sign_list = sign_list
        return self

    def verify_signature(self, recipients):
        """ Verify whether the signatures are from a list of recipients.  Doesn't verify if the transaction is valid on
        the chain
        """
        if len(recipients) != len(self.sign_list):
            return False

        for i in range(len(recipients)):
            sig = KeyAPI.Signature(signature_bytes=self.sign_list[i])
            pub = sig.recover_public_key_from_msg(self.get_hash_unsigned())
            if pub.to_canonical_address() != recipients[i]:
                return False
        return True


def calculate_merkle_root(item_list):
    if len(item_list) == 0:
        return bytes(32)

    sha_tree = []
    for item in item_list:
        sha_tree.append(sha3_256(item.serialize()))

    while len(sha_tree) != 1:
        next_sha_tree = []
        for i in range(0, len(sha_tree) - 1, 2):
            next_sha_tree.append(sha3_256(sha_tree[i] + sha_tree[i + 1]))
        if len(sha_tree) % 2 != 0:
            next_sha_tree.append(sha3_256(sha_tree[-1] + sha_tree[-1]))
        sha_tree = next_sha_tree
    return sha_tree[0]


class ShardInfo(Serializable):
    """ Shard information contains
    - shard size (power of 2)
    - voting of increasing shards
    """

    FIELDS = [("value", uint32)]

    def __init__(self, value):
        self.value = value

    def get_shard_size(self):
        return 1 << (self.value & 31)

    def get_reshard_vote(self):
        return (self.value & (1 << 31)) != 0

    @staticmethod
    def create(shard_size, reshard_vote=False):
        assert is_p2(shard_size)
        reshard_vote = 1 if reshard_vote else 0
        return ShardInfo(int_left_most_bit(shard_size) - 1 + (reshard_vote << 31))


def mk_receipt_sha(receipts, db):
    t = trie.Trie(db)
    for i, receipt in enumerate(receipts):
        t.update(rlp.encode(i), rlp.encode(receipt))
    return t.root_hash


class MinorBlockMeta(Serializable):
    """ Meta data that are not included in root block
    """

    FIELDS = [
        ("hash_merkle_root", hash256),
        ("hash_evm_state_root", hash256),
        ("hash_evm_receipt_root", hash256),
        ("coinbase_address", Address),
        ("evm_gas_used", uint256),
        ("evm_cross_shard_receive_gas_used", uint256),
        ("extra_data", PrependedSizeBytesSerializer(2)),
    ]

    def __init__(
        self,
        hash_merkle_root: bytes = bytes(Constant.HASH_LENGTH),
        hash_evm_state_root: bytes = bytes(Constant.HASH_LENGTH),
        hash_evm_receipt_root: bytes = bytes(Constant.HASH_LENGTH),
        coinbase_address: Address = Address.create_empty_account(),
        evm_gas_used: int = 0,
        evm_cross_shard_receive_gas_used: int = 0,
        extra_data: bytes = b"",
    ):
        self.hash_merkle_root = hash_merkle_root
        self.hash_evm_state_root = hash_evm_state_root
        self.hash_evm_receipt_root = hash_evm_receipt_root
        self.coinbase_address = coinbase_address
        self.evm_gas_used = evm_gas_used
        self.evm_cross_shard_receive_gas_used = evm_cross_shard_receive_gas_used
        self.extra_data = extra_data

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
        ("coinbase_amount", uint256),
        ("hash_prev_minor_block", hash256),
        ("hash_prev_root_block", hash256),
        ("evm_gas_limit", uint256),
        ("hash_meta", hash256),
        ("create_time", uint64),
        ("difficulty", uint64),
        ("nonce", uint64),
        ("bloom", uint2048),
        ("mixhash", hash256),
    ]

    def __init__(
        self,
        version: int = 0,
        height: int = 0,
        branch: Branch = Branch.create(1, 0),
        coinbase_amount: int = 0,
        hash_prev_minor_block: bytes = bytes(Constant.HASH_LENGTH),
        hash_prev_root_block: bytes = bytes(Constant.HASH_LENGTH),
        evm_gas_limit: int = 30000 * 400,  # 400 xshard tx
        hash_meta: bytes = bytes(Constant.HASH_LENGTH),
        create_time: int = 0,
        difficulty: int = 0,
        nonce: int = 0,
        bloom: int = 0,
        mixhash: bytes = bytes(Constant.HASH_LENGTH),
    ):
        self.version = version
        self.height = height
        self.branch = branch
        self.coinbase_amount = coinbase_amount
        self.hash_prev_minor_block = hash_prev_minor_block
        self.hash_prev_root_block = hash_prev_root_block
        self.evm_gas_limit = evm_gas_limit
        self.hash_meta = hash_meta
        self.create_time = create_time
        self.difficulty = difficulty
        self.nonce = nonce
        self.bloom = bloom
        self.mixhash = mixhash

    def get_hash(self):
        return sha3_256(self.serialize())


class MinorBlock(Serializable):
    FIELDS = [
        ("header", MinorBlockHeader),
        ("meta", MinorBlockMeta),
        ("tx_list", PrependedSizeListSerializer(4, Transaction)),
    ]

    def __init__(
        self,
        header: MinorBlockHeader,
        meta: MinorBlockMeta,
        tx_list: List[Transaction] = None,
    ):
        self.header = header
        self.meta = meta
        self.tx_list = [] if tx_list is None else tx_list

    def calculate_merkle_root(self):
        return calculate_merkle_root(self.tx_list)

    def finalize_merkle_root(self):
        """ Compute merkle root hash and put it in the field
        """
        self.meta.hash_merkle_root = self.calculate_merkle_root()
        return self

    def finalize(self, evm_state, hash_prev_root_block=None):
        if hash_prev_root_block is not None:
            self.header.hash_prev_root_block = hash_prev_root_block
        self.meta.hash_evm_state_root = evm_state.trie.root_hash
        self.meta.evm_gas_used = evm_state.gas_used
        self.meta.evm_cross_shard_receive_gas_used = evm_state.xshard_receive_gas_used
        self.header.coinbase_amount = evm_state.block_fee // 2
        self.finalize_merkle_root()
        self.meta.hash_evm_receipt_root = mk_receipt_sha(
            evm_state.receipts, evm_state.db
        )
        self.header.hash_meta = self.meta.get_hash()
        self.header.bloom = evm_state.bloom
        return self

    def add_tx(self, tx):
        self.tx_list.append(tx)
        return self

    def get_receipt(self, db, i):
        t = trie.Trie(db, self.meta.hash_evm_receipt_root)
        receipt = rlp.decode(t.get(rlp.encode(i)), quarkchain.evm.messages.Receipt)
        if receipt.contract_address != b"":
            contract_address = Address(
                receipt.contract_address, receipt.contract_full_shard_id
            )
        else:
            contract_address = Address.create_empty_account(full_shard_id=0)

        if i > 0:
            prev_gas_used = rlp.decode(
                t.get(rlp.encode(i - 1)), quarkchain.evm.messages.Receipt
            ).gas_used
        else:
            prev_gas_used = self.meta.evm_cross_shard_receive_gas_used

        logs = [
            Log.create_from_eth_log(eth_log, self, tx_idx=i, log_idx=j)
            for j, eth_log in enumerate(receipt.logs)
        ]

        return TransactionReceipt(
            receipt.state_root,
            receipt.gas_used,
            prev_gas_used,
            contract_address,
            receipt.bloom,
            logs,
        )

    def get_block_prices(self) -> List[int]:
        return [tx.code.get_evm_transaction().gasprice for tx in self.tx_list]

    def create_block_to_append(
        self,
        create_time=None,
        address=None,
        quarkash=0,
        difficulty=None,
        extra_data=b"",
        nonce=0,
    ):
        if address is None:
            address = Address.create_empty_account(
                full_shard_id=self.meta.coinbase_address.full_shard_id
            )
        meta = MinorBlockMeta(coinbase_address=address, extra_data=extra_data)

        create_time = (
            self.header.create_time + 1 if create_time is None else create_time
        )
        difficulty = self.header.difficulty if difficulty is None else difficulty
        header = MinorBlockHeader(
            version=self.header.version,
            height=self.header.height + 1,
            branch=self.header.branch,
            coinbase_amount=quarkash,
            hash_prev_minor_block=self.header.get_hash(),
            hash_prev_root_block=self.header.hash_prev_root_block,
            evm_gas_limit=self.header.evm_gas_limit,
            create_time=create_time,
            difficulty=difficulty,
            nonce=nonce,
        )

        return MinorBlock(header, meta, [])


class RootBlockHeader(Serializable):
    FIELDS = [
        ("version", uint32),
        ("height", uint32),
        ("shard_info", ShardInfo),
        ("hash_prev_block", hash256),
        ("hash_merkle_root", hash256),
        ("coinbase_address", Address),
        ("coinbase_amount", uint256),
        ("create_time", uint32),
        ("difficulty", uint32),
        ("nonce", uint32),
        ("extra_data", PrependedSizeBytesSerializer(2)),
        ("mixhash", hash256),
    ]

    def __init__(
        self,
        version=0,
        height=0,
        shard_info=ShardInfo.create(1, False),
        hash_prev_block=bytes(Constant.HASH_LENGTH),
        hash_merkle_root=bytes(Constant.HASH_LENGTH),
        coinbase_address=Address.create_empty_account(),
        coinbase_amount=0,
        create_time=0,
        difficulty=0,
        nonce=0,
        extra_data: bytes = b"",
        mixhash: bytes = bytes(Constant.HASH_LENGTH),
    ):
        self.version = version
        self.height = height
        self.shard_info = shard_info
        self.hash_prev_block = hash_prev_block
        self.hash_merkle_root = hash_merkle_root
        self.coinbase_address = coinbase_address
        self.coinbase_amount = coinbase_amount
        self.create_time = create_time
        self.difficulty = difficulty
        self.nonce = nonce
        self.extra_data = extra_data
        self.mixhash = mixhash

    def get_hash(self):
        return sha3_256(self.serialize())

    def create_block_to_append(
        self,
        create_time=None,
        difficulty=None,
        address=None,
        nonce=0,
        extra_data: bytes = b"",
    ):
        create_time = self.create_time + 1 if create_time is None else create_time
        difficulty = difficulty if difficulty is not None else self.difficulty
        header = RootBlockHeader(
            version=self.version,
            height=self.height + 1,
            shard_info=copy.copy(self.shard_info),
            hash_prev_block=self.get_hash(),
            hash_merkle_root=bytes(32),
            coinbase_address=address if address else Address.create_empty_account(),
            create_time=create_time,
            difficulty=difficulty,
            nonce=nonce,
            extra_data=extra_data,
        )
        return RootBlock(header)


class RootBlock(Serializable):
    FIELDS = [
        ("header", RootBlockHeader),
        ("minor_block_header_list", PrependedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, header, minor_block_header_list=None):
        self.header = header
        self.minor_block_header_list = (
            [] if minor_block_header_list is None else minor_block_header_list
        )

    def finalize(self, quarkash=0, coinbase_address=Address.create_empty_account()):
        self.header.hash_merkle_root = calculate_merkle_root(
            self.minor_block_header_list
        )

        self.header.coinbase_amount = quarkash
        self.header.coinbase_address = coinbase_address
        return self

    def add_minor_block_header(self, header):
        self.minor_block_header_list.append(header)
        return self

    def extend_minor_block_header_list(self, header_list):
        self.minor_block_header_list.extend(header_list)
        return self

    def create_block_to_append(
        self, create_time=None, difficulty=None, address=None, nonce=0
    ):
        return self.header.create_block_to_append(
            create_time=create_time, difficulty=difficulty, address=address, nonce=nonce
        )


class CrossShardTransactionDeposit(Serializable):
    """ Destination of x-shard tx
    """

    FIELDS = [
        ("tx_hash", hash256),  # hash of quarkchain.core.Transaction
        ("from_address", Address),
        ("to_address", Address),
        ("value", uint256),
        ("gas_price", uint256),
    ]

    def __init__(self, tx_hash, from_address, to_address, value, gas_price):
        self.tx_hash = tx_hash
        self.from_address = from_address
        self.to_address = to_address
        self.value = value
        self.gas_price = gas_price


class CrossShardTransactionList(Serializable):
    FIELDS = [("tx_list", PrependedSizeListSerializer(4, CrossShardTransactionDeposit))]

    def __init__(self, tx_list):
        self.tx_list = tx_list


class Log(Serializable):

    FIELDS = [
        ("recipient", FixedSizeBytesSerializer(20)),
        ("topics", PrependedSizeListSerializer(1, FixedSizeBytesSerializer(32))),
        ("data", PrependedSizeBytesSerializer(4)),
        # derived fields
        ("block_number", uint64),
        ("block_hash", hash256),
        ("tx_idx", uint32),
        ("tx_hash", hash256),
        ("log_idx", uint32),
    ]

    def __init__(
        self,
        recipient: bytes,
        topics: List[bytes],
        data: bytes,
        block_number: int = 0,
        block_hash: bytes = bytes(Constant.HASH_LENGTH),
        tx_idx: int = 0,
        tx_hash: bytes = bytes(Constant.HASH_LENGTH),
        log_idx: int = 0,
    ):
        self.recipient = recipient
        self.topics = topics
        self.data = data
        self.block_number = block_number
        self.block_hash = block_hash
        self.tx_idx = tx_idx
        self.tx_hash = tx_hash
        self.log_idx = log_idx

    @classmethod
    def create_from_eth_log(cls, eth_log, block: MinorBlock, tx_idx: int, log_idx: int):
        recipient = eth_log.address
        data = eth_log.data
        tx = block.tx_list[tx_idx]  # type: Transaction

        topics = []
        for topic in eth_log.topics:
            topics.append(topic.to_bytes(32, "big"))
        return Log(
            recipient,
            topics,
            data,
            block_number=block.header.height,
            block_hash=block.header.get_hash(),
            tx_idx=tx_idx,
            tx_hash=tx.get_hash(),
            log_idx=log_idx,
        )

    def to_dict(self):
        return {
            "recipient": self.recipient.hex(),
            "topics": [t.hex() for t in self.topics],
            "data": self.data.hex(),
        }


class TransactionReceipt(Serializable):
    """ Wrapper over tx receipts from EVM """

    FIELDS = [
        ("success", PrependedSizeBytesSerializer(1)),
        ("gas_used", uint64),
        ("prev_gas_used", uint64),
        ("bloom", uint2048),
        ("contract_address", Address),
        ("logs", PrependedSizeListSerializer(4, Log)),
    ]

    def __init__(
        self,
        success: bytes,
        gas_used: int,
        prev_gas_used: int,
        contract_address: Address,
        bloom: int,
        logs: List[Log],
    ):
        self.success = success
        self.gas_used = gas_used
        self.prev_gas_used = prev_gas_used
        self.contract_address = contract_address
        self.bloom = bloom
        self.logs = logs

    @classmethod
    def create_empty_receipt(cls):
        return cls(b"", 0, 0, Address.create_empty_account(0), 0, [])


def test():
    priv = KeyAPI.PrivateKey(
        bytes.fromhex(
            "208065a247edbe5df4d86fbdc0171303f23a76961be9f6013850dd2bdc759bbb"
        )
    )
    addr = priv.public_key.to_canonical_address()
    assert addr == b"\x0b\xedz\xbda$v5\xc1\x97>\xb3\x84t\xa2Qn\xd1\xd8\x84"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd")
    args = parser.parse_args()

    if args.cmd == "create_account":
        iden = Identity.create_random_identity()
        addr = Address.create_from_identity(iden)
        print("Key: %s" % iden.get_key().hex())
        print("Account: %s" % addr.serialize().hex())


if __name__ == "__main__":
    main()
