#!/usr/bin/python3

from quarkchain.core import Identity, Address
from quarkchain.core import Transaction, TransactionInput, TransactionOutput, Code, ByteBuffer, random_bytes
from quarkchain.core import MinorBlock, MinorBlockHeader, calculate_merkle_root, Branch, ShardInfo
from quarkchain.core import RootBlockHeader
import random
import time
import unittest


def create_test_transaction(fromId, toAddress):
    acc1 = Address.createFromIdentity(fromId)
    tx = Transaction(
        [TransactionInput(random_bytes(32), 0)],
        Code(),
        [TransactionOutput(acc1, random.randint(0, 100)), TransactionOutput(toAddress, random.randint(0, 100))])
    tx.sign([fromId.getKey()])
    return tx


class TestTransaction(unittest.TestCase):

    def testTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc2 = Address.createRandomAccount()

        tx = create_test_transaction(id1, acc2)

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
            create_test_transaction(id1, acc2), create_test_transaction(id1, acc3), create_test_transaction(id1, acc4)]

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


class TestCode(unittest.TestCase):

    def testOversizedCode(self):
        code = Code(random_bytes(255))
        s = code.serialize()
        code1 = Code.deserialize(s)
        self.assertEqual(code, code1)

        code = Code(random_bytes(256))
        try:
            s = code.serialize()
        except RuntimeError as e:
            pass
        else:
            self.fail()
