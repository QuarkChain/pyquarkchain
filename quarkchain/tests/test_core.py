#!/usr/bin/python3

from quarkchain.core import Identity, Address
from quarkchain.core import Transaction, ByteBuffer, random_bytes
from quarkchain.core import MinorBlock, MinorBlockHeader, calculate_merkle_root, Branch, ShardInfo
from quarkchain.core import RootBlockHeader
import time
import unittest
from quarkchain.tests.test_utils import create_random_test_transaction
from quarkchain.core import ShardMask


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


class TestMinorBlock(unittest.TestCase):

    def testMinorBlock(self):
        id1 = Identity.createRandomIdentity()
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount()
        acc4 = Address.createRandomAccount()

        txList = [
            create_random_test_transaction(id1, acc2),
            create_random_test_transaction(id1, acc3),
            create_random_test_transaction(id1, acc4)]

        mRoot = calculate_merkle_root(txList)
        header = MinorBlockHeader(
            version=1,
            height=123,
            branch=Branch.create(2, 1),
            hashPrevRootBlock=random_bytes(32),
            hashPrevMinorBlock=random_bytes(32),
            hashMerkleRoot=mRoot,
            createTime=int(time.time()),
            difficulty=1234,
            nonce=0)
        block = MinorBlock(header, txList)
        bb = ByteBuffer(block.serialize())
        block1 = MinorBlock.deserialize(bb)
        self.assertEqual(bb.remaining(), 0)
        self.assertEqual(block, block1)


class TestRootBlock(unittest.TestCase):

    def testRootBlock(self):
        header = RootBlockHeader(
            version=0,
            height=1,
            shardInfo=ShardInfo.create(4, False),
            hashPrevBlock=random_bytes(32),
            hashMerkleRoot=random_bytes(32),
            createTime=1234,
            difficulty=4,
            nonce=5)
        barray = header.serialize()
        bb = ByteBuffer(barray)
        header1 = RootBlockHeader.deserialize(bb)
        self.assertEqual(bb.remaining(), 0)
        self.assertEqual(header, header1)


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
