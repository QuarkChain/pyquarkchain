import unittest


from quarkchain.cluster.tests.test_utils import create_transfer_transaction, ClusterContext
from quarkchain.evm import opcodes
from quarkchain.core import Address, Branch, Identity
from quarkchain.utils import call_async, assert_true_with_timeout


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
            self.assertEqual(block1.header.height, 2)
            self.assertEqual(block1.header.branch.value, 0b10)
            self.assertEqual(len(block1.txList), 1)

            self.assertTrue(call_async(slaves[0].addBlock(block1)))
            self.assertEqual(call_async(master.getPrimaryAccountData(acc2)).balance, 12345)
            self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

            # Expect to mine shard 1 due to proof-of-progress
            isRoot, block2 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertFalse(isRoot)
            self.assertEqual(block2.header.height, 2)
            self.assertEqual(block2.header.branch.value, 0b11)
            self.assertEqual(len(block2.txList), 0)

            self.assertTrue(call_async(slaves[1].addBlock(block2)))

            # Expect to mine root
            isRoot, block = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(isRoot)
            self.assertEqual(block.header.height, 2)
            self.assertEqual(len(block.minorBlockHeaderList), 2)
            self.assertEqual(block.minorBlockHeaderList[0], block1.header)
            self.assertEqual(block.minorBlockHeaderList[1], block2.header)

            self.assertTrue(master.rootState.addBlock(block))
            slaves[1].shardStateMap[3].addRootBlock(block)
            self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

            # Expect to mine shard 1 for the gas on xshard tx to acc3
            isRoot, block3 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertFalse(isRoot)
            self.assertEqual(block3.header.height, 3)
            self.assertEqual(block3.header.branch.value, 0b11)
            self.assertEqual(len(block3.txList), 0)

            self.assertTrue(call_async(slaves[1].addBlock(block3)))
            # Expect withdrawTo is included in acc3's balance
            self.assertEqual(call_async(master.getPrimaryAccountData(acc3)).balance, 54321)

    def testGetPrimaryAccountData(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.getPrimaryAccountData(acc1)).transactionCount, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            self.assertEqual(call_async(master.getPrimaryAccountData(acc1)).transactionCount, 1)
            self.assertEqual(call_async(master.getPrimaryAccountData(acc2)).transactionCount, 0)

    def testAddTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(2, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            tx1 = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
            )
            self.assertTrue(call_async(master.addTransaction(tx1)))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 1)

            # missing withdrawTo
            tx2 = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
                withdraw=100,
            )
            self.assertFalse(call_async(master.addTransaction(tx2)))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 1)

            # check the tx is received by the other cluster
            txQueue = clusters[1].slaveList[0].shardStateMap[branch.value].txQueue
            assert_true_with_timeout(lambda: len(txQueue) == 1)
            self.assertEqual(txQueue.pop_transaction(), tx1.code.getEvmTransaction())

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

            # Make sure another cluster received the new block
            assert_true_with_timeout(
                lambda: clusters[1].slaveList[0].shardStateMap[0b10].containBlockByHash(b1.header.getHash()))
            assert_true_with_timeout(
                lambda: clusters[1].slaveList[1].shardStateMap[0b11].containRemoteMinorBlockHash(b1.header.getHash()))
            assert_true_with_timeout(
                lambda: clusters[1].master.rootState.isMinorBlockValidated(b1.header.getHash()))

    def testAddRootBlockRequestList(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            # add blocks in cluster 0
            blockHeaderList = []
            for i in range(13):
                shardState0 = clusters[0].slaveList[0].shardStateMap[0b10]
                b1 = shardState0.getTip().createBlockToAppend()
                b1.finalize(evmState=shardState0.runBlock(b1))
                addResult = call_async(clusters[0].slaveList[0].addBlock(b1))
                self.assertTrue(addResult)
                blockHeaderList.append(b1.header)

            shardState0 = clusters[0].slaveList[1].shardStateMap[0b11]
            b2 = shardState0.getTip().createBlockToAppend()
            b2.finalize(evmState=shardState0.runBlock(b2))
            addResult = call_async(clusters[0].slaveList[1].addBlock(b2))
            self.assertTrue(addResult)
            blockHeaderList.append(b2.header)

            # add 1 block in cluster 1
            shardState1 = clusters[1].slaveList[1].shardStateMap[0b11]
            b3 = shardState1.getTip().createBlockToAppend()
            b3.finalize(evmState=shardState1.runBlock(b3))
            addResult = call_async(clusters[1].slaveList[1].addBlock(b3))
            self.assertTrue(addResult)

            self.assertEqual(clusters[1].slaveList[1].shardStateMap[0b11].headerTip, b3.header)

            # reestablish cluster connection
            call_async(clusters[1].network.connect("127.0.0.1", 38000))

            rB1 = clusters[0].master.rootState.createBlockToMine(blockHeaderList, acc1)
            call_async(clusters[0].master.addRootBlock(rB1))

            # Make sure the root block tip of local cluster is changed
            self.assertEqual(clusters[0].master.rootState.tip, rB1.header)

            # Make sure the root block tip of cluster 1 is changed
            assert_true_with_timeout(lambda: clusters[1].master.rootState.tip == rB1.header, 2)

            # Minor block is downloaded
            self.assertEqual(b1.header.height, 14)
            assert_true_with_timeout(
                lambda: clusters[1].slaveList[0].shardStateMap[0b10].headerTip == b1.header)

            # The tip is overwritten due to root chain first consensus
            assert_true_with_timeout(
                lambda: clusters[1].slaveList[1].shardStateMap[0b11].headerTip == b2.header)

    def testShardSynchronizerWithFork(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            blockList = []
            # cluster 0 has 13 blocks added
            for i in range(13):
                shardState0 = clusters[0].slaveList[0].shardStateMap[0b10]
                block = shardState0.getTip().createBlockToAppend()
                block.finalize(evmState=shardState0.runBlock(block))
                addResult = call_async(clusters[0].slaveList[0].addBlock(block))
                self.assertTrue(addResult)
                blockList.append(block)
            self.assertEqual(clusters[0].slaveList[0].shardStateMap[0b10].headerTip.height, 14)

            # cluster 1 has 12 blocks added
            for i in range(12):
                shardState0 = clusters[1].slaveList[0].shardStateMap[0b10]
                block = shardState0.getTip().createBlockToAppend()
                block.finalize(evmState=shardState0.runBlock(block))
                addResult = call_async(clusters[1].slaveList[0].addBlock(block))
                self.assertTrue(addResult)
            self.assertEqual(clusters[1].slaveList[0].shardStateMap[0b10].headerTip.height, 13)

            # reestablish cluster connection
            call_async(clusters[1].network.connect("127.0.0.1", 38000))

            # a new block from cluster 0 will trigger sync in cluster 1
            shardState0 = clusters[0].slaveList[0].shardStateMap[0b10]
            block = shardState0.getTip().createBlockToAppend()
            block.finalize(evmState=shardState0.runBlock(block))
            addResult = call_async(clusters[0].slaveList[0].addBlock(block))
            self.assertTrue(addResult)
            blockList.append(block)

            # expect cluster 1 has all the blocks from cluter 0 and
            # has the same tip as cluster 0
            for block in blockList:
                assert_true_with_timeout(
                    lambda: clusters[1].slaveList[0].shardStateMap[0b10].containBlockByHash(
                        block.header.getHash()))
                assert_true_with_timeout(
                    lambda: clusters[1].master.rootState.isMinorBlockValidated(
                        block.header.getHash()))

            self.assertEqual(clusters[1].slaveList[0].shardStateMap[0b10].headerTip,
                             clusters[0].slaveList[0].shardStateMap[0b10].headerTip)

    def testBroadcastCrossShardTransactions(self):
        ''' Test the cross shard transactions are broadcasted to the destination shards '''
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=0)
        acc3 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            tx1 = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[2 | 0],
                fromId=id1,
                toAddress=acc2,
                amount=12345,
                withdraw=54321,
                withdrawTo=bytes(acc3.serialize()),
                startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].addTx(tx1))

            b1 = slaves[0].shardStateMap[2 | 0].createBlockToMine(address=acc1)
            b2 = slaves[0].shardStateMap[2 | 0].createBlockToMine(address=acc1)
            b2.header.createTime += 1
            self.assertNotEqual(b1.header.getHash(), b2.header.getHash())

            self.assertTrue(call_async(slaves[0].addBlock(b1)))

            # expect shard 1 got the CrossShardTransactionList of b1
            xshardTxList = slaves[1].shardStateMap[2 | 1].db.getMinorBlockXshardTxList(b1.header.getHash())
            self.assertEqual(len(xshardTxList.txList), 1)
            self.assertEqual(xshardTxList.txList[0].amount, 54321)
            self.assertEqual(xshardTxList.txList[0].address, acc3)

            self.assertTrue(call_async(slaves[0].addBlock(b2)))
            # b2 doesn't update tip
            self.assertEqual(slaves[0].shardStateMap[2 | 0].headerTip, b1.header)

            # expect shard 1 got the CrossShardTransactionList of b2
            xshardTxList = slaves[1].shardStateMap[2 | 1].db.getMinorBlockXshardTxList(b2.header.getHash())
            self.assertEqual(len(xshardTxList.txList), 1)
            self.assertEqual(xshardTxList.txList[0].amount, 54321)
            self.assertEqual(xshardTxList.txList[0].address, acc3)

            b3 = slaves[1].shardStateMap[2 | 1].createBlockToMine(address=acc1.addressInShard(1))
            self.assertTrue(call_async(slaves[1].addBlock(b3)))

            isRoot, rB = call_async(master.getNextBlockToMine(address=acc1))
            call_async(master.addRootBlock(rB))

            # b4 should include the withdraw of tx1
            b4 = slaves[1].shardStateMap[2 | 1].createBlockToMine(address=acc1.addressInShard(1))

            # adding b1, b2, b3 again shouldn't affect b4 to be added later
            self.assertTrue(call_async(slaves[0].addBlock(b1)))
            self.assertTrue(call_async(slaves[0].addBlock(b2)))
            self.assertTrue(call_async(slaves[1].addBlock(b3)))

            self.assertTrue(call_async(slaves[1].addBlock(b4)))
            self.assertEqual(call_async(master.getPrimaryAccountData(acc3)).balance, 54321)

