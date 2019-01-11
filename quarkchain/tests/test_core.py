import unittest

from eth_keys import KeyAPI

from quarkchain.core import (
    Branch,
    biguint,
    Identity,
    Address,
    RootBlockHeader,
    MinorBlockHeader,
    MinorBlockMeta,
    ShardMask,
    Optional,
    Serializable,
    uint32,
    Transaction,
    ByteBuffer,
    hash256,
    EnumSerializer,
)
from quarkchain.tests.test_utils import create_random_test_transaction
from quarkchain.utils import check

SIZE_LIST = [(RootBlockHeader, 244), (MinorBlockHeader, 507), (MinorBlockMeta, 160)]


class TestDataSize(unittest.TestCase):
    def test_data_size(self):
        for tup in SIZE_LIST:
            cls = tup[0]
            size = tup[1]
            with self.subTest(cls.__name__):
                self.assertEqual(len(cls().serialize()), size)


class TestTransaction(unittest.TestCase):
    def test_transaction(self):
        id1 = Identity.create_random_identity()
        acc2 = Address.create_random_account()

        tx = create_random_test_transaction(id1, acc2)

        barray = tx.serialize(bytearray())

        bb = ByteBuffer(barray)
        tx1 = Transaction.deserialize(bb)
        self.assertEqual(bb.remaining(), 0)
        self.assertEqual(tx, tx1)
        self.assertTrue(tx1.verify_signature([id1.get_recipient()]))


class TestBranch(unittest.TestCase):
    def test_branch(self):
        b = Branch.create(8, 6)
        self.assertEqual(b.get_shard_size(), 8)
        self.assertEqual(b.get_full_shard_id(), 14)


class TestIdentity(unittest.TestCase):
    def test_identity(self):
        id1 = Identity.create_random_identity()
        id2 = id1.create_from_key(id1.get_key())
        self.assertEqual(id1.get_recipient(), id2.get_recipient())
        self.assertEqual(id1.get_key(), id2.get_key())


class TestShardMask(unittest.TestCase):
    def test_shard_mask(self):
        sm0 = ShardMask(0b1)
        self.assertTrue(sm0.contain_shard_id(0))
        self.assertTrue(sm0.contain_shard_id(1))
        self.assertTrue(sm0.contain_shard_id(0b1111111))

        sm1 = ShardMask(0b101)
        self.assertFalse(sm1.contain_shard_id(0))
        self.assertTrue(sm1.contain_shard_id(0b1))
        self.assertFalse(sm1.contain_shard_id(0b10))
        self.assertFalse(sm1.contain_shard_id(0b11))
        self.assertFalse(sm1.contain_shard_id(0b100))
        self.assertTrue(sm1.contain_shard_id(0b101))
        self.assertFalse(sm1.contain_shard_id(0b110))
        self.assertFalse(sm1.contain_shard_id(0b111))
        self.assertFalse(sm1.contain_shard_id(0b1000))
        self.assertTrue(sm1.contain_shard_id(0b1001))

    def test_shard_mask_iterate(self):
        sm0 = ShardMask(0b11)
        self.assertEqual(sorted(l for l in sm0.iterate(4)), [1, 0b11])
        self.assertEqual(sorted(l for l in sm0.iterate(8)), [1, 0b11, 0b101, 0b111])

        sm1 = ShardMask(0b101)
        self.assertEqual(sorted(l for l in sm1.iterate(8)), [1, 0b101])
        self.assertEqual(sorted(l for l in sm1.iterate(16)), [1, 0b101, 0b1001, 0b1101])


class Uint32Optional(Serializable):
    FIELDS = [("value", Optional(uint32))]

    def __init__(self, value):
        self.value = value


class TestOptional(unittest.TestCase):
    def test_optional(self):
        v = Uint32Optional(123)
        b = v.serialize()
        v1 = Uint32Optional.deserialize(b)
        self.assertEqual(v.value, v1.value)

        v = Uint32Optional(None)
        b = v.serialize()
        v1 = Uint32Optional.deserialize(b)
        self.assertEqual(v.value, v1.value)


class ABigUint(Serializable):
    FIELDS = [("value", biguint)]

    def __init__(self, value: int):
        self.value = value


class TestBigUint(unittest.TestCase):
    def test_big_uint(self):
        testcases = [9 ** 10, 2 ** (255 * 8) - 1, 0]
        for tc in testcases:
            v = ABigUint(tc)
            b = v.serialize()
            vv = ABigUint.deserialize(b)
            self.assertEqual(vv.value, v.value)

        testcases = [9 ** 1000, 2 ** (255 * 8)]
        for tc in testcases:
            v = ABigUint(tc)
            self.assertRaises(RuntimeError, v.serialize)


class TestRootBlockHeaderSignature(unittest.TestCase):
    def test_signature(self):
        header = RootBlockHeader()
        private_key = KeyAPI.PrivateKey(Identity.create_random_identity().get_key())
        self.assertEqual(header.signature, bytes(65))
        self.assertFalse(header.is_signed())
        self.assertFalse(header.verify_signature(private_key.public_key))

        header.sign_with_private_key(private_key)
        self.assertNotEqual(header.signature, bytes(65))
        self.assertTrue(header.is_signed())
        self.assertTrue(header.verify_signature(private_key.public_key))


class SimpleHeaderV0(Serializable):
    FIELDS = [("version", uint32), ("value", uint32)]

    def __init__(self, version, value):
        check(version == 0)
        self.version = version
        self.value = value


class SimpleHeaderV1(Serializable):
    FIELDS = [("version", uint32), ("value", uint32), ("hash_trie", hash256)]

    def __init__(self, version, value, hash_trie):
        check(version == 1)
        self.version = version
        self.value = value
        self.hash_trie = hash_trie


class VersionedSimpleHeader(Serializable):
    FIELDS = [
        ("header", EnumSerializer("version", {0: SimpleHeaderV0, 1: SimpleHeaderV1}))
    ]

    def __init__(self, header):
        self.header = header


class VersionedSimpleHeaderOld(Serializable):
    FIELDS = [("header", EnumSerializer("version", {0: SimpleHeaderV0}))]

    def __init__(self, header):
        self.header = header


class TestEnumSerializer(unittest.TestCase):
    def test_simple(self):
        vh0 = VersionedSimpleHeader(SimpleHeaderV0(0, 123))
        barray = vh0.serialize(bytearray())

        bb = ByteBuffer(barray)
        vh0d = VersionedSimpleHeader.deserialize(bb)
        h0d = vh0d.header

        self.assertEqual(0, h0d.version)
        self.assertEqual(123, h0d.value)

        vh1 = VersionedSimpleHeader(SimpleHeaderV1(1, 456, bytes(32)))
        barray = vh1.serialize(bytearray())

        bb = ByteBuffer(barray)
        vh1d = VersionedSimpleHeader.deserialize(bb)
        h1d = vh1d.header

        self.assertEqual(1, h1d.version)
        self.assertEqual(456, h1d.value)
        self.assertEqual(bytes(32), h1d.hash_trie)

        # Old versioned serializer cannot serialize new data
        with self.assertRaises(ValueError):
            vh = VersionedSimpleHeaderOld(SimpleHeaderV1(1, 456, bytes(32)))
            vh.serialize(bytearray())

        # Old versioned serializer cannot deserialize new data
        with self.assertRaises(ValueError):
            bb = ByteBuffer(barray)
            vhd = VersionedSimpleHeaderOld.deserialize(bb)
