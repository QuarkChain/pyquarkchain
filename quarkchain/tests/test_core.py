#!/usr/bin/python3

from quarkchain.core import Identity, Address
from quarkchain.core import Transaction, TransactionInput, TransactionOutput, Code, ByteBuffer, random_bytes
from quarkchain.core import MinorBlock, MinorBlockHeader, calculate_merkle_root
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
            branch=0b11,
            hashPrevMajorBlock=random_bytes(32),
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
