import unittest

from quarkchain.core import Branch, ShardInfo
from quarkchain.core import Identity, Address
from quarkchain.core import RootBlockHeader, MinorBlockHeader, MinorBlockMeta
from quarkchain.core import ShardMask, Optional, Serializable, uint32
from quarkchain.core import Transaction, ByteBuffer
from quarkchain.tests.test_utils import create_random_test_transaction

SIZE_LIST = [(RootBlockHeader, 178), (MinorBlockHeader, 488), (MinorBlockMeta, 186)]


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
        self.assertEqual(b.get_shard_id(), 6)


class TestShardInfo(unittest.TestCase):
    def test_shard_info(self):
        info = ShardInfo.create(4, False)
        self.assertEqual(info.get_shard_size(), 4)
        self.assertEqual(info.get_reshard_vote(), False)

        info = ShardInfo.create(2147483648, True)
        self.assertEqual(info.get_shard_size(), 2147483648)
        self.assertEqual(info.get_reshard_vote(), True)


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
