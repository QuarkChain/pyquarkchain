#!/usr/bin/python3

# Core data structures
# Use standard ecsda, but should be replaced by ethereum's secp256k1
# implementation

import argparse
import random
import typing
from typing import List, Dict

import ecdsa
import rlp
import eth_keys
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
    TX_ID_HEX_LENGTH = 72
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
        self.marked_position = 0

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

    def mark(self):
        self.marked_position = self.position

    def reset(self):
        self.position = self.marked_position


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


class PrependedSizeMapSerializer:
    """
    does not serialize/deserialize key/value pairs if skip_func(key, value) == True
    """

    def __init__(self, size_bytes, key_ser, value_ser, skip_func=None):
        self.size_bytes = size_bytes
        self.key_ser = key_ser
        self.value_ser = value_ser
        self.skip_func = skip_func

    def serialize(self, item_map: Dict, barray):
        if self.skip_func:
            item_map = {k: v for k, v in item_map.items() if not self.skip_func(k, v)}
        barray.extend(len(item_map).to_bytes(self.size_bytes, byteorder="big"))
        # keep keys sorted to maintain deterministic serialization
        for k in sorted(item_map):
            self.key_ser.serialize(k, barray)
            self.value_ser.serialize(item_map[k], barray)
        return barray

    def deserialize(self, bb):
        size = bb.get_uint(self.size_bytes)
        item_map = dict()
        for i in range(size):
            k = self.key_ser.deserialize(bb)
            item_map[k] = self.value_ser.deserialize(bb)
        if self.skip_func:
            item_map = {k: v for k, v in item_map.items() if not self.skip_func(k, v)}
        return item_map


class BigUintSerializer:
    """Not infinity, but pretty big: 2^2048-1"""

    def __init__(self):
        self.ser = PrependedSizeBytesSerializer(1)

    def serialize(self, value, barray):
        value_size_in_byte = (value.bit_length() - 1) // 8 + 1
        bs = value.to_bytes(value_size_in_byte, byteorder="big")
        return self.ser.serialize(bs, barray)

    def deserialize(self, bb):
        bs = self.ser.deserialize(bb)
        return int.from_bytes(bs, byteorder="big")


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

    def to_dict(self):
        d = dict()
        for name, ser in self.FIELDS:
            v = getattr(self, name)
            if isinstance(v, Serializable):
                d[name] = v.to_dict()
            else:
                d[name] = v
        return d


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


class EnumSerializer:
    """ EnumSerializer.
    The enum field must be the first field to be serialized/deserialized.
    """

    def __init__(self, enum_field, enum_dict, size=4):
        self.enum_field = enum_field
        self.enum_dict = enum_dict
        self.size = size

    def serialize(self, obj, barray):
        enum = getattr(obj, self.enum_field)
        if enum not in self.enum_dict:
            raise ValueError("Cannot recognize enum value: " + str((enum)))
        if type(obj) != self.enum_dict[enum]:
            raise ValueError(
                "Incorrect enum value, expect %s, actual %s: "
                % (type(self.enum_dict[enum]), type(obj))
            )
        obj.serialize(barray)

    def deserialize(self, bb):
        bb.mark()
        enum = bb.get_uint(self.size)
        if enum not in self.enum_dict:
            raise ValueError("Cannot recognize enum value: " + str(enum))
        bb.reset()
        return self.enum_dict[enum].deserialize(bb)


uint8 = UintSerializer(1)
uint16 = UintSerializer(2)
uint32 = UintSerializer(4)
uint64 = UintSerializer(8)
uint128 = UintSerializer(16)
uint256 = UintSerializer(32)
uint2048 = UintSerializer(256)
hash256 = FixedSizeBytesSerializer(32)
boolean = BooleanSerializer()
biguint = BigUintSerializer()
signature65 = FixedSizeBytesSerializer(65)


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
    FIELDS = [("recipient", FixedSizeBytesSerializer(20)), ("full_shard_key", uint32)]

    def __init__(self, recipient: bytes, full_shard_key: int) -> None:
        """
        recipient is 20 bytes SHA3 of public key
        shard_id is uint32_t
        """
        self.recipient = recipient
        self.full_shard_key = full_shard_key

    def to_hex(self):
        return self.serialize().hex()

    def get_full_shard_id(self, shard_size: int):
        if not is_p2(shard_size):
            raise RuntimeError("Invalid shard size {}".format(shard_size))
        chain_id = self.full_shard_key >> 16
        shard_id = self.full_shard_key & (shard_size - 1)
        return (chain_id << 16) | shard_size | shard_id

    def address_in_shard(self, full_shard_key: int):
        return Address(self.recipient, full_shard_key)

    def address_in_branch(self, branch):
        shard_key = self.full_shard_key & ((1 << 16) - 1)
        new_shard_key = (
            shard_key & ~(branch.get_shard_size() - 1)
        ) + branch.get_shard_id()
        new_full_shard_key = (branch.get_chain_id() << 16) | new_shard_key
        return Address(self.recipient, new_full_shard_key)

    # TODO: chain_id
    @staticmethod
    def create_from_identity(identity: Identity, full_shard_key: int = None):
        if full_shard_key is None:
            r = identity.get_recipient()
            full_shard_key = int.from_bytes(r[0:1] + r[10:11], "big")
        return Address(identity.get_recipient(), full_shard_key)

    @staticmethod
    def create_random_account(full_shard_key=None):
        """ An account is a special address with default shard that the
        account should be in.
        """
        return Address.create_from_identity(
            Identity.create_random_identity(), full_shard_key
        )

    @staticmethod
    def create_empty_account(full_shard_key=0):
        return Address(bytes(20), full_shard_key)

    @staticmethod
    def create_from(bs):
        bs = normalize_bytes(bs, 24)
        return Address(bs[0:20], int.from_bytes(bs[20:], byteorder="big"))

    def is_empty(self):
        return set(self.recipient) == {0}


class Branch(Serializable):
    FIELDS = [("value", uint32)]

    def __init__(self, value: int):
        self.value = value

    def get_chain_id(self):
        return self.value >> 16

    def get_shard_size(self):
        branch_value = self.value & ((1 << 16) - 1)
        return 1 << (int_left_most_bit(branch_value) - 1)

    def get_full_shard_id(self):
        return self.value

    def get_shard_id(self):
        branch_value = self.value & ((1 << 16) - 1)
        return branch_value ^ self.get_shard_size()

    def is_in_branch(self, full_shard_key: int):
        chain_id_match = (full_shard_key >> 16) == self.get_chain_id()
        if not chain_id_match:
            return False
        return (full_shard_key & (self.get_shard_size() - 1)) == self.get_shard_id()

    @staticmethod
    def create(chain_id: int, shard_size: int, shard_id: int):
        assert is_p2(shard_size)
        return Branch((chain_id << 16) + shard_size | shard_id)

    def to_str(self):
        return "{}/{}".format(self.get_chain_id(), self.get_shard_id())


class ChainMask(Serializable):
    """ Represent a mask of chains, basically matches all the bits from the right until the leftmost bit is hit.
    E.g.,
    mask = 1, matches *
    mask = 0b10, matches *0
    mask = 0b101, matches *01
    """

    FIELDS = [("value", uint16)]

    def __init__(self, value):
        check(value != 0)
        self.value = value

    def contain_full_shard_id(self, full_shard_id: int):
        chain_id = full_shard_id >> 16
        bit_mask = (1 << (int_left_most_bit(self.value) - 1)) - 1
        return (bit_mask & chain_id) == (self.value & bit_mask)

    def contain_branch(self, branch: Branch):
        return self.contain_full_shard_id(branch.get_full_shard_id())

    def has_overlap(self, chain_mask):
        return masks_have_overlap(self.value, chain_mask.value)


class TransactionType:
    SERIALIZED_EVM = 0


class SerializedEvmTransaction(Serializable):
    FIELDS = [("type", uint8), ("serialized_tx", PrependedSizeBytesSerializer(4))]

    def __init__(self, type, serialized_tx):
        check(type == TransactionType.SERIALIZED_EVM)
        self.type = TransactionType.SERIALIZED_EVM
        self.serialized_tx = serialized_tx

    @classmethod
    def from_evm_tx(cls, evm_tx: EvmTransaction):
        return SerializedEvmTransaction(
            TransactionType.SERIALIZED_EVM, rlp.encode(evm_tx)
        )

    def to_evm_tx(self) -> EvmTransaction:
        return rlp.decode(self.serialized_tx, EvmTransaction)


class TypedTransaction(Serializable):
    # The wrapped tx can be of any transaction type
    # To add new transaction type:
    #     1. Add a new type in TransactionType
    #     2. Create the new transaction class and make sure the first field is ("type", uint8)
    #     3. Add the new tx type to the EnumSerializer
    #     4. Add a new test to TestTypedTransaction in test_core.py
    FIELDS = [
        (
            "tx",
            EnumSerializer(
                "type",
                {TransactionType.SERIALIZED_EVM: SerializedEvmTransaction},
                size=1,
            ),
        )
    ]

    def __init__(self, tx: SerializedEvmTransaction):
        self.tx = tx

    def get_hash(self):
        return sha3_256(self.serialize())


class TokenBalanceMap(Serializable):
    FIELDS = [
        (
            "balance_map",
            PrependedSizeMapSerializer(4, biguint, biguint, lambda k, v: v == 0),
        )
    ]

    def __init__(self, balance_map: Dict):
        if not isinstance(balance_map, Dict):
            raise TypeError("TokenBalanceMap can only accept dict object")
        self.balance_map = balance_map

    def add(self, other: Dict):
        for k, v in other.items():
            self.balance_map[k] = self.balance_map.get(k, 0) + v

    def get_hash(self):
        return sha3_256(self.serialize())


def calculate_merkle_root(item_list):
    """ Using ETH2.0 SSZ style hash tree
    """
    if len(item_list) == 0:
        sha_tree = [bytes(32)]
    else:
        sha_tree = [sha3_256(item.serialize()) for item in item_list]

    zbytes = bytes(32)
    while len(sha_tree) != 1:
        if len(sha_tree) % 2 != 0:
            sha_tree.append(zbytes)
        sha_tree = [
            sha3_256(sha_tree[i] + sha_tree[i + 1]) for i in range(0, len(sha_tree), 2)
        ]
        zbytes = sha3_256(zbytes + zbytes)
    return sha3_256(sha_tree[0] + len(item_list).to_bytes(8, "big"))


def mk_receipt_sha(receipts, db):
    t = trie.Trie(db)
    for i, receipt in enumerate(receipts):
        t.update(rlp.encode(i), rlp.encode(receipt))
    return t.root_hash


class XshardTxCursorInfo(Serializable):

    FIELDS = [
        ("root_block_height", uint64),
        ("minor_block_index", uint64),  # 0 root block x shard, >= 1 minor blocks
        ("xshard_deposit_index", uint64),
    ]

    def __init__(self, root_block_height, minor_block_index, xshard_deposit_index):
        self.root_block_height = root_block_height
        self.minor_block_index = minor_block_index
        self.xshard_deposit_index = xshard_deposit_index


class MinorBlockMeta(Serializable):
    """ Meta data that are not included in root block
    """

    FIELDS = [
        ("hash_merkle_root", hash256),
        ("hash_evm_state_root", hash256),
        ("hash_evm_receipt_root", hash256),
        ("evm_gas_used", uint256),
        ("evm_cross_shard_receive_gas_used", uint256),
        ("xshard_tx_cursor_info", XshardTxCursorInfo),
        ("evm_xshard_gas_limit", uint256),
    ]

    def __init__(
        self,
        hash_merkle_root: bytes = bytes(Constant.HASH_LENGTH),
        hash_evm_state_root: bytes = bytes(Constant.HASH_LENGTH),
        hash_evm_receipt_root: bytes = bytes(Constant.HASH_LENGTH),
        evm_gas_used: int = 0,
        evm_cross_shard_receive_gas_used: int = 0,
        xshard_tx_cursor_info: XshardTxCursorInfo = XshardTxCursorInfo(0, 0, 0),
        evm_xshard_gas_limit: int = 30000 * 200,
    ):
        self.hash_merkle_root = hash_merkle_root
        self.hash_evm_state_root = hash_evm_state_root
        self.hash_evm_receipt_root = hash_evm_receipt_root
        self.evm_gas_used = evm_gas_used
        self.evm_cross_shard_receive_gas_used = evm_cross_shard_receive_gas_used
        self.xshard_tx_cursor_info = xshard_tx_cursor_info
        self.evm_xshard_gas_limit = evm_xshard_gas_limit

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
        ("coinbase_address", Address),
        ("coinbase_amount_map", TokenBalanceMap),
        ("hash_prev_minor_block", hash256),
        ("hash_prev_root_block", hash256),
        ("evm_gas_limit", uint256),
        ("hash_meta", hash256),
        ("create_time", uint64),
        ("difficulty", biguint),
        ("nonce", uint64),
        ("bloom", uint2048),
        ("extra_data", PrependedSizeBytesSerializer(2)),
        ("mixhash", hash256),
    ]

    def __init__(
        self,
        version: int = 0,
        height: int = 0,
        branch: Branch = Branch(1),
        coinbase_address: Address = Address.create_empty_account(),
        coinbase_amount_map: TokenBalanceMap = None,
        hash_prev_minor_block: bytes = bytes(Constant.HASH_LENGTH),
        hash_prev_root_block: bytes = bytes(Constant.HASH_LENGTH),
        evm_gas_limit: int = 30000 * 400,  # 400 transfer tx
        hash_meta: bytes = bytes(Constant.HASH_LENGTH),
        create_time: int = 0,
        difficulty: int = 0,
        nonce: int = 0,
        bloom: int = 0,
        extra_data: bytes = b"",
        mixhash: bytes = bytes(Constant.HASH_LENGTH),
    ):
        self.version = version
        self.height = height
        self.branch = branch
        self.coinbase_address = coinbase_address
        self.coinbase_amount_map = coinbase_amount_map or TokenBalanceMap({})
        self.hash_prev_minor_block = hash_prev_minor_block
        self.hash_prev_root_block = hash_prev_root_block
        self.evm_gas_limit = evm_gas_limit
        self.hash_meta = hash_meta
        self.create_time = create_time
        self.difficulty = difficulty
        self.nonce = nonce
        self.bloom = bloom
        self.extra_data = extra_data
        self.mixhash = mixhash

    def get_hash(self):
        return sha3_256(self.serialize())

    def get_hash_for_mining(self):
        return sha3_256(self.serialize_without(["nonce", "mixhash"]))


class MinorBlock(Serializable):
    FIELDS = [
        ("header", MinorBlockHeader),
        ("meta", MinorBlockMeta),
        ("tx_list", PrependedSizeListSerializer(4, TypedTransaction)),
        (
            "tracking_data",
            PrependedSizeBytesSerializer(2),
        ),  # for logging purpose, not signed
    ]

    def __init__(
        self,
        header: MinorBlockHeader,
        meta: MinorBlockMeta,
        tx_list: List[TypedTransaction] = None,
        tracking_data: bytes = b"",
    ):
        self.header = header
        self.meta = meta
        self.tx_list = [] if tx_list is None else tx_list
        self.tracking_data = tracking_data

    def calculate_merkle_root(self):
        return calculate_merkle_root(self.tx_list)

    def finalize_merkle_root(self):
        """ Compute merkle root hash and put it in the field
        """
        self.meta.hash_merkle_root = self.calculate_merkle_root()
        return self

    def finalize(
        self, evm_state, coinbase_amount_map: TokenBalanceMap, hash_prev_root_block=None
    ):
        if hash_prev_root_block is not None:
            self.header.hash_prev_root_block = hash_prev_root_block
        self.meta.xshard_tx_cursor_info = evm_state.xshard_tx_cursor_info
        self.meta.hash_evm_state_root = evm_state.trie.root_hash
        self.meta.evm_gas_used = evm_state.gas_used
        self.meta.evm_cross_shard_receive_gas_used = evm_state.xshard_receive_gas_used
        self.header.coinbase_amount_map = coinbase_amount_map
        self.finalize_merkle_root()
        self.meta.hash_evm_receipt_root = mk_receipt_sha(
            evm_state.receipts[:] + evm_state.xshard_deposit_receipts[:], evm_state.db
        )
        self.header.hash_meta = self.meta.get_hash()
        self.header.bloom = evm_state.get_bloom()
        return self

    def add_tx(self, tx):
        self.tx_list.append(tx)
        return self

    def get_receipt(self, db, i):
        t = trie.Trie(db, self.meta.hash_evm_receipt_root)
        receipt = rlp.decode(t.get(rlp.encode(i)), quarkchain.evm.messages.Receipt)
        if receipt.contract_address != b"":
            contract_address = Address(
                receipt.contract_address, receipt.contract_full_shard_key
            )
        else:
            contract_address = Address.create_empty_account(full_shard_key=0)

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

    def get_block_prices(self) -> Dict[int, list]:
        prices = {}
        for typed_tx in self.tx_list:
            evm_tx = typed_tx.tx.to_evm_tx()
            prices.setdefault(evm_tx.gas_token_id, []).append(evm_tx.gasprice)

        return prices

    def create_block_to_append(
        self,
        create_time=None,
        address=None,
        coinbase_tokens: Dict = None,
        difficulty=None,
        extra_data=b"",
        nonce=0,
    ):
        if address is None:
            address = Address.create_empty_account(
                full_shard_key=self.header.coinbase_address.full_shard_key
            )
        meta = MinorBlockMeta(xshard_tx_cursor_info=self.meta.xshard_tx_cursor_info)

        create_time = (
            self.header.create_time + 1 if create_time is None else create_time
        )
        difficulty = self.header.difficulty if difficulty is None else difficulty
        header = MinorBlockHeader(
            version=self.header.version,
            height=self.header.height + 1,
            branch=self.header.branch,
            coinbase_address=address,
            coinbase_amount_map=TokenBalanceMap(coinbase_tokens or {}),
            hash_prev_minor_block=self.header.get_hash(),
            hash_prev_root_block=self.header.hash_prev_root_block,
            evm_gas_limit=self.header.evm_gas_limit,
            create_time=create_time,
            difficulty=difficulty,
            nonce=nonce,
            extra_data=extra_data,
        )

        return MinorBlock(header, meta, [], b"")

    def __hash__(self):
        return int.from_bytes(self.header.get_hash(), byteorder="big")

    def __eq__(self, other):
        return isinstance(other, MinorBlock) and hash(other) == hash(self)


class RootBlockHeader(Serializable):
    FIELDS = [
        ("version", uint32),
        ("height", uint32),
        ("hash_prev_block", hash256),
        ("hash_merkle_root", hash256),
        ("hash_evm_state_root", hash256),
        ("coinbase_address", Address),
        ("coinbase_amount_map", TokenBalanceMap),
        ("create_time", uint64),
        ("difficulty", biguint),
        ("total_difficulty", biguint),
        ("nonce", uint64),
        ("extra_data", PrependedSizeBytesSerializer(2)),
        ("mixhash", hash256),
        ("signature", FixedSizeBytesSerializer(65)),
    ]

    def __init__(
        self,
        version=0,
        height=0,
        hash_prev_block=bytes(Constant.HASH_LENGTH),
        hash_merkle_root=bytes(Constant.HASH_LENGTH),
        hash_evm_state_root=bytes(Constant.HASH_LENGTH),
        coinbase_address=Address.create_empty_account(),
        coinbase_amount_map: TokenBalanceMap = None,
        create_time=0,
        difficulty=0,
        total_difficulty=0,
        nonce=0,
        extra_data: bytes = b"",
        mixhash: bytes = bytes(Constant.HASH_LENGTH),
        signature: bytes = bytes(65),
    ):
        self.version = version
        self.height = height
        self.hash_prev_block = hash_prev_block
        self.hash_merkle_root = hash_merkle_root
        self.hash_evm_state_root = hash_evm_state_root
        self.coinbase_address = coinbase_address
        self.coinbase_amount_map = coinbase_amount_map or TokenBalanceMap({})
        self.create_time = create_time
        self.difficulty = difficulty
        self.total_difficulty = total_difficulty
        self.nonce = nonce
        self.extra_data = extra_data
        self.mixhash = mixhash
        self.signature = signature

    def get_hash(self):
        return sha3_256(self.serialize())

    def get_hash_for_mining(self):
        return sha3_256(self.serialize_without(["nonce", "mixhash", "signature"]))

    def sign_with_private_key(self, private_key: KeyAPI.PrivateKey):
        self.signature = private_key.sign_msg_hash(
            self.get_hash_for_mining()
        ).to_bytes()

    def verify_signature(self, public_key: KeyAPI.PublicKey):
        try:
            return public_key.verify_msg_hash(
                self.get_hash_for_mining(),
                KeyAPI.Signature(signature_bytes=self.signature),
            )
        except eth_keys.exceptions.BadSignature:
            return False

    def is_signed(self):
        return self.signature != bytes(65)

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
        total_difficulty = self.total_difficulty + difficulty
        header = RootBlockHeader(
            version=self.version,
            height=self.height + 1,
            hash_prev_block=self.get_hash(),
            hash_merkle_root=bytes(Constant.HASH_LENGTH),
            hash_evm_state_root=trie.BLANK_ROOT,
            coinbase_address=address if address else Address.create_empty_account(),
            coinbase_amount_map=None,
            create_time=create_time,
            difficulty=difficulty,
            total_difficulty=total_difficulty,
            nonce=nonce,
            extra_data=extra_data,
        )
        return RootBlock(header)


class RootBlock(Serializable):
    FIELDS = [
        ("header", RootBlockHeader),
        ("minor_block_header_list", PrependedSizeListSerializer(4, MinorBlockHeader)),
        (
            "tracking_data",
            PrependedSizeBytesSerializer(2),
        ),  # for logging purpose, not signed
    ]

    def __init__(
        self,
        header: RootBlockHeader,
        minor_block_header_list: typing.Optional[List[MinorBlockHeader]] = None,
        tracking_data: bytes = b"",
    ):
        self.header = header
        self.minor_block_header_list = (
            [] if minor_block_header_list is None else minor_block_header_list
        )  # type: List[MinorBlockHeader]
        self.tracking_data = tracking_data

    def finalize(
        self,
        coinbase_tokens: Dict = None,
        coinbase_address=Address.create_empty_account(),
        hash_evm_state_root=None,
    ):
        self.header.hash_merkle_root = calculate_merkle_root(
            self.minor_block_header_list
        )

        self.header.coinbase_amount_map = TokenBalanceMap(coinbase_tokens or {})
        self.header.coinbase_address = coinbase_address
        self.hash_evm_state_root = (
            trie.BLANK_ROOT if hash_evm_state_root is None else hash_evm_state_root
        )
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


CROSS_SHARD_TRANSACTION_DEPOSIT_DEPRECATED_SIZE = (
    Constant.HASH_LENGTH + Constant.ADDRESS_LENGTH * 2 + 32 * 2 + 8 * 2 + 1
)


class CrossShardTransactionDepositDeprecated(Serializable):
    """ Destination of x-shard tx
    """

    FIELDS = [
        ("tx_hash", hash256),  # hash of quarkchain.core.Transaction
        ("from_address", Address),
        ("to_address", Address),
        ("value", uint256),
        ("gas_price", uint256),
        ("gas_token_id", uint64),
        ("transfer_token_id", uint64),
        ("is_from_root_chain", boolean),
    ]

    def __init__(
        self,
        tx_hash,
        from_address,
        to_address,
        value,
        gas_price,
        gas_token_id,
        transfer_token_id,
        is_from_root_chain=False,
    ):
        self.tx_hash = tx_hash
        self.from_address = from_address
        self.to_address = to_address
        self.value = value
        self.gas_price = gas_price
        self.gas_token_id = gas_token_id
        self.transfer_token_id = transfer_token_id
        self.is_from_root_chain = is_from_root_chain


class CrossShardTransactionDeposit(Serializable):
    """ Destination of x-shard tx
    """

    FIELDS = [
        ("tx_hash", hash256),  # hash of quarkchain.core.Transaction
        ("from_address", Address),
        ("to_address", Address),
        ("value", uint256),
        ("gas_price", uint256),
        ("gas_token_id", uint64),
        ("transfer_token_id", uint64),
        ("gas_remained", uint256),
        ("message_data", PrependedSizeBytesSerializer(4)),
        ("create_contract", boolean),
        ("is_from_root_chain", boolean),
    ]

    def __init__(
        self,
        tx_hash,
        from_address,
        to_address,
        value,
        gas_price,
        gas_token_id,
        transfer_token_id,
        gas_remained=0,
        message_data=b"",
        create_contract=False,
        is_from_root_chain=False,
    ):
        self.tx_hash = tx_hash
        self.from_address = from_address
        self.to_address = to_address
        self.value = value
        self.gas_price = gas_price
        self.gas_token_id = gas_token_id
        self.transfer_token_id = transfer_token_id
        self.gas_remained = gas_remained
        self.message_data = message_data
        self.create_contract = create_contract
        self.is_from_root_chain = is_from_root_chain


class CrossShardTransactionDeprecatedList(Serializable):
    FIELDS = [
        (
            "tx_list",
            PrependedSizeListSerializer(4, CrossShardTransactionDepositDeprecated),
        )
    ]

    def __init__(self, tx_list):
        self.tx_list = tx_list


class CrossShardTransactionList(Serializable):
    FIELDS = [
        ("tx_list", PrependedSizeListSerializer(4, CrossShardTransactionDeposit)),
        ("version", uint32),
    ]

    def __init__(self, tx_list, version=0):
        self.tx_list = tx_list
        self.version = version

    @staticmethod
    def is_old_list(data):
        bb = ByteBuffer(data)
        size = bb.get_uint(4)
        if size == 0:
            return True
        return (len(data) - 4) == CROSS_SHARD_TRANSACTION_DEPOSIT_DEPRECATED_SIZE * size

    @staticmethod
    def from_data(data):
        if not CrossShardTransactionList.is_old_list(data):
            return CrossShardTransactionList.deserialize(data)

        old_list = CrossShardTransactionDeprecatedList.deserialize(data)
        return CrossShardTransactionList(
            [
                CrossShardTransactionDeposit(
                    tx_hash=tx.tx_hash,
                    from_address=tx.from_address,
                    to_address=tx.to_address,
                    value=tx.value,
                    gas_price=tx.gas_price,
                    gas_token_id=tx.gas_token_id,
                    transfer_token_id=tx.transfer_token_id,
                    gas_remained=0,
                    message_data=b"",
                    create_contract=False,
                    is_from_root_chain=tx.is_from_root_chain,
                )
                for tx in old_list.tx_list
            ]
        )


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
