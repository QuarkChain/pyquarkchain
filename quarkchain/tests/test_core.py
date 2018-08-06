#!/usr/bin/python3

import unittest

from quarkchain.core import Branch, ShardInfo
from quarkchain.core import Identity, Address
from quarkchain.core import ShardMask, Optional, Serializable, uint32
from quarkchain.core import Transaction, ByteBuffer
from quarkchain.tests.test_utils import create_random_test_transaction


class TestTransaction(unittest.TestCase):

    def testTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc2 = Address.createRandomAccount()

        tx = create_random_test_transaction(id1, acc2)

        barray = tx.serialize(bytearray())

        bb = ByteBuffer(barray)
        tx1 = Transaction.deserialize(bb)
        self.assertEqual(bb.remaining(), 0)
        self.assertEqual(tx, tx1)
        self.assertTrue(tx1.verifySignature([id1.getRecipient()]))


class TestBranch(unittest.TestCase):

    def testBranch(self):
        b = Branch.create(8, 6)
        self.assertEqual(b.getShardSize(), 8)
        self.assertEqual(b.getShardId(), 6)


class TestShardInfo(unittest.TestCase):

    def testShardInfo(self):
        info = ShardInfo.create(4, False)
        self.assertEqual(info.getShardSize(), 4)
        self.assertEqual(info.getReshardVote(), False)

        info = ShardInfo.create(2147483648, True)
        self.assertEqual(info.getShardSize(), 2147483648)
        self.assertEqual(info.getReshardVote(), True)


class TestIdentity(unittest.TestCase):

    def testIdentity(self):
        id1 = Identity.createRandomIdentity()
        id2 = id1.createFromKey(id1.getKey())
        self.assertEqual(id1.getRecipient(), id2.getRecipient())
        self.assertEqual(id1.getKey(), id2.getKey())


class TestShardMask(unittest.TestCase):

    def testShardMask(self):
        sm0 = ShardMask(0b1)
        self.assertTrue(sm0.containShardId(0))
        self.assertTrue(sm0.containShardId(1))
        self.assertTrue(sm0.containShardId(0b1111111))

        sm1 = ShardMask(0b101)
        self.assertFalse(sm1.containShardId(0))
        self.assertTrue(sm1.containShardId(0b1))
        self.assertFalse(sm1.containShardId(0b10))
        self.assertFalse(sm1.containShardId(0b11))
        self.assertFalse(sm1.containShardId(0b100))
        self.assertTrue(sm1.containShardId(0b101))
        self.assertFalse(sm1.containShardId(0b110))
        self.assertFalse(sm1.containShardId(0b111))
        self.assertFalse(sm1.containShardId(0b1000))
        self.assertTrue(sm1.containShardId(0b1001))

    def testShardMaskIterate(self):
        sm0 = ShardMask(0b11)
        self.assertEqual(
            sorted(l for l in sm0.iterate(4)),
            [1, 0b11])
        self.assertEqual(
            sorted(l for l in sm0.iterate(8)),
            [1, 0b11, 0b101, 0b111])

        sm1 = ShardMask(0b101)
        self.assertEqual(
            sorted(l for l in sm1.iterate(8)),
            [1, 0b101])
        self.assertEqual(
            sorted(l for l in sm1.iterate(16)),
            [1, 0b101, 0b1001, 0b1101])


class Uint32Optional(Serializable):
    FIELDS = [
        ("value", Optional(uint32)),
    ]

    def __init__(self, value):
        self.value = value


class TestOptional(unittest.TestCase):

    def testOptional(self):
        v = Uint32Optional(123)
        b = v.serialize()
        v1 = Uint32Optional.deserialize(b)
        self.assertEqual(v.value, v1.value)

        v = Uint32Optional(None)
        b = v.serialize()
        v1 = Uint32Optional.deserialize(b)
        self.assertEqual(v.value, v1.value)
