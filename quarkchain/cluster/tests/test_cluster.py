import unittest

from quarkchain.cluster.tests.test_utils import create_transfer_transaction, ClusterContext
from quarkchain.evm import opcodes
from quarkchain.core import Address, Branch, Identity
from quarkchain.utils import call_async


class TestCluster(unittest.TestCase):

    def testSingleCluster(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        with ClusterContext(1, acc1) as clusters:
            self.assertEqual(len(clusters), 1)

    def testThreeClusters(self):
        with ClusterContext(3) as clusters:
            self.assertEqual(len(clusters), 3)

    def testGetNextBlockToMine(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=0)
        acc3 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[2 | 0],
                fromId=id1,
                toAddress=acc2,
                amount=12345,
                withdraw=54321,
                withdrawTo=bytes(acc3.serialize()),
                startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].addTx(tx))

            # Expect to mine shard 0 since it has one tx
            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertFalse(isRoot)
            self.assertEqual(block1.header.height, 1)
            self.assertEqual(block1.header.branch.value, 2 | 0)
            self.assertEqual(len(block1.txList), 1)

            self.assertTrue(call_async(slaves[0].addBlock(block1)))
            self.assertEqual(slaves[0].shardStateMap[2].getBalance(acc2.recipient), 12345)
            self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

            # Expect to mine shard 1 due to proof-of-progress
            isRoot, block2 = call_async(master.getNextBlockToMine(address=acc1.addressInShard(1)))
            self.assertFalse(isRoot)
            self.assertEqual(block2.header.height, 1)
            self.assertEqual(block2.header.branch.value, 2 | 1)
            self.assertEqual(len(block2.txList), 0)

            self.assertTrue(call_async(slaves[1].addBlock(block2)))

            # Expect to mine root
            isRoot, block = call_async(master.getNextBlockToMine(address=acc1.addressInShard(1)))
            self.assertTrue(isRoot)
            self.assertEqual(block.header.height, 1)
            self.assertEqual(len(block.minorBlockHeaderList), 2)
            self.assertEqual(block.minorBlockHeaderList[0], block1.header)
            self.assertEqual(block.minorBlockHeaderList[1], block2.header)

            self.assertTrue(master.rootState.addBlock(block))
            slaves[1].shardStateMap[3].addRootBlock(block)
            self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

            # Expect to mine shard 1 for the gas on xshard tx to acc3
            isRoot, block3 = call_async(master.getNextBlockToMine(address=acc1.addressInShard(1)))
            self.assertFalse(isRoot)
            self.assertEqual(block3.header.height, 2)
            self.assertEqual(block3.header.branch.value, 2 | 1)
            self.assertEqual(len(block3.txList), 0)

            self.assertTrue(call_async(slaves[1].addBlock(block3)))
            # Expect withdrawTo is included in acc3's balance
            self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 54321)

    def testGetTransactionCount(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.getTransactionCount(acc1)), (branch, 0))
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            self.assertEqual(call_async(master.getTransactionCount(acc1)), (branch, 1))
            branch1 = Branch.create(2, 1)
            self.assertEqual(call_async(master.getTransactionCount(acc2)), (branch1, 0))

    def testAddTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
            )
            self.assertTrue(call_async(master.addTransaction(tx)))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 1)

            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
                withdraw=100,
            )
            self.assertFalse(call_async(master.addTransaction(tx)))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 1)

    def testAddMinorBlockRequestList(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(2, acc1) as clusters:
            shardState = clusters[0].slaveList[0].shardStateMap[0b10]
            b1 = shardState.getTip().createBlockToAppend()
            b1.finalize(evmState=shardState.runBlock(b1))
            addResult = call_async(clusters[0].slaveList[0].addBlock(b1))
            self.assertTrue(addResult)

            # Make sure the xshard list is added to another slave
            self.assertTrue(
                clusters[0].slaveList[1].shardStateMap[0b11].containRemoteMinorBlockHash(b1.header.getHash()))
            self.assertTrue(clusters[0].master.rootState.isMinorBlockValidated(b1.header.getHash()))
