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
    ChainMask,
    Optional,
    Serializable,
    uint32,
    Transaction,
    ByteBuffer,
    hash256,
    EnumSerializer,
    PrependedSizeMapSerializer,
    boolean,
    TokenBalanceMap,
)
from quarkchain.tests.test_utils import create_random_test_transaction
from quarkchain.utils import check

SIZE_LIST = [(RootBlockHeader, 216), (MinorBlockHeader, 479), (MinorBlockMeta, 216)]


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
        b = Branch.create(3, 8, 6)
        self.assertEqual(b.get_shard_size(), 8)
        self.assertEqual(b.get_full_shard_id(), (3 << 16) + 14)
        self.assertEqual(b.get_chain_id(), 3)


class TestIdentity(unittest.TestCase):
    def test_identity(self):
        id1 = Identity.create_random_identity()
        id2 = id1.create_from_key(id1.get_key())
        self.assertEqual(id1.get_recipient(), id2.get_recipient())
        self.assertEqual(id1.get_key(), id2.get_key())


class TestChainMask(unittest.TestCase):
    def test_chain_mask(self):
        sm0 = ChainMask(0b1)
        self.assertTrue(sm0.contain_full_shard_id(0))
        self.assertTrue(sm0.contain_full_shard_id(1 << 16))
        self.assertTrue(sm0.contain_full_shard_id(0b1111111))

        sm1 = ChainMask(0b101)
        self.assertFalse(sm1.contain_full_shard_id(0))
        self.assertTrue(sm1.contain_full_shard_id(0b1 << 16))
        self.assertFalse(sm1.contain_full_shard_id(0b10 << 16))
        self.assertFalse(sm1.contain_full_shard_id(0b11 << 16))
        self.assertFalse(sm1.contain_full_shard_id(0b100 << 16))
        self.assertTrue(sm1.contain_full_shard_id(0b101 << 16))
        self.assertFalse(sm1.contain_full_shard_id(0b110 << 16))
        self.assertFalse(sm1.contain_full_shard_id(0b111 << 16))
        self.assertFalse(sm1.contain_full_shard_id(0b1000 << 16))
        self.assertTrue(sm1.contain_full_shard_id(0b1001 << 16))


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


class MapData(Serializable):
    FIELDS = [("m", PrependedSizeMapSerializer(4, uint32, boolean))]

    def __init__(self, m):
        self.m = m


class TestMapSerializer(unittest.TestCase):
    def test_simple(self):
        m = dict()
        m[0] = True
        m[1] = False
        m[2] = False

        md = MapData(m)

        bmd = md.serialize(bytearray())

        bb = ByteBuffer(bmd)
        md1 = MapData.deserialize(bb)
        self.assertEqual(md.m, md1.m)
        self.assertEqual(len(md1.m), 3)

    def test_order(self):
        m0 = {3: 0, 1: 2, 10: 9}
        m1 = {10: 9, 3: 0, 1: 2}

        md0 = MapData(m0)
        md1 = MapData(m1)

        self.assertEqual(md0.serialize(), md1.serialize())


class TestTokenBalanceMap(unittest.TestCase):
    def test_add(self):
        m0 = TokenBalanceMap({0: 10})
        m1 = TokenBalanceMap({1: 20})

        m0.add(m1.balance_map)
        self.assertEqual(m0.balance_map, {0: 10, 1: 20})

        m2 = TokenBalanceMap({0: 30, 2: 50})
        m0.add(m2.balance_map)
        self.assertEqual(m0.balance_map, {0: 40, 1: 20, 2: 50})
