import unittest

from quarkchain.cluster.tests.test_utils import create_transfer_transaction, ClusterContext
from quarkchain.core import Address, Branch, Identity
from quarkchain.evm import opcodes
from quarkchain.utils import call_async, assert_true_with_timeout


class TestCluster(unittest.TestCase):

    def test_single_cluster(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        with ClusterContext(1, acc1) as clusters:
            self.assertEqual(len(clusters), 1)

    def test_three_clusters(self):
        with ClusterContext(3) as clusters:
            self.assertEqual(len(clusters), 3)

    def test_get_next_block_to_mine(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[2 | 0],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc3,
                value=54321,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
                gasPrice=3,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            # Expect to mine shard 0 since it has one tx
            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertFalse(isRoot)
            self.assertEqual(block1.header.height, 2)
            self.assertEqual(block1.header.branch.value, 0b10)
            self.assertEqual(len(block1.txList), 1)

            originalBalanceAcc1 = call_async(master.get_primary_account_data(acc1)).balance
            gasPaid = (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 3
            self.assertTrue(call_async(slaves[0].add_block(block1)))
            self.assertEqual(call_async(master.get_primary_account_data(acc1)).balance,
                             originalBalanceAcc1 - 54321 - gasPaid)
            self.assertEqual(slaves[1].shardStateMap[3].get_balance(acc3.recipient), 0)

            # Expect to mine shard 1 due to proof-of-progress
            isRoot, block2 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertFalse(isRoot)
            self.assertEqual(block2.header.height, 2)
            self.assertEqual(block2.header.branch.value, 0b11)
            self.assertEqual(len(block2.txList), 0)

            self.assertTrue(call_async(slaves[1].add_block(block2)))

            # Expect to mine root
            isRoot, block = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertTrue(isRoot)
            self.assertEqual(block.header.height, 2)
            self.assertEqual(len(block.minorBlockHeaderList), 2)
            self.assertEqual(block.minorBlockHeaderList[0], block1.header)
            self.assertEqual(block.minorBlockHeaderList[1], block2.header)

            self.assertTrue(master.rootState.add_block(block))
            slaves[1].shardStateMap[3].add_root_block(block)
            self.assertEqual(slaves[1].shardStateMap[3].get_balance(acc3.recipient), 0)

            # Expect to mine shard 1 for the gas on xshard tx to acc3
            isRoot, block3 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertFalse(isRoot)
            self.assertEqual(block3.header.height, 3)
            self.assertEqual(block3.header.branch.value, 0b11)
            self.assertEqual(len(block3.txList), 0)

            self.assertTrue(call_async(slaves[1].add_block(block3)))
            # Expect withdrawTo is included in acc3's balance
            self.assertEqual(call_async(master.get_primary_account_data(acc3)).balance, 54321)

    def test_get_primary_account_data(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.get_primary_account_data(acc1)).transactionCount, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(block1)))

            self.assertEqual(call_async(master.get_primary_account_data(acc1)).transactionCount, 1)
            self.assertEqual(call_async(master.get_primary_account_data(acc2)).transactionCount, 0)

    def test_add_transaction(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id1, fullShardId=1)

        with ClusterContext(2, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch0 = Branch.create(2, 0)
            tx1 = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch0.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(call_async(master.add_transaction(tx1)))
            self.assertEqual(len(slaves[0].shardStateMap[branch0.value].txQueue), 1)

            branch1 = Branch.create(2, 1)
            tx2 = create_transfer_transaction(
                shardState=slaves[1].shardStateMap[branch1.value],
                key=id1.get_key(),
                fromAddress=acc2,
                toAddress=acc1,
                value=12345,
                gas=30000,
            )
            self.assertTrue(call_async(master.add_transaction(tx2)))
            self.assertEqual(len(slaves[1].shardStateMap[branch1.value].txQueue), 1)

            # check the tx is received by the other cluster
            txQueue = clusters[1].slaveList[0].shardStateMap[branch0.value].txQueue
            assert_true_with_timeout(lambda: len(txQueue) == 1)
            self.assertEqual(txQueue.pop_transaction(), tx1.code.get_evm_transaction())

            txQueue = clusters[1].slaveList[1].shardStateMap[branch1.value].txQueue
            assert_true_with_timeout(lambda: len(txQueue) == 1)
            self.assertEqual(txQueue.pop_transaction(), tx2.code.get_evm_transaction())

    def test_add_minor_block_request_list(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(2, acc1) as clusters:
            shardState = clusters[0].slaveList[0].shardStateMap[0b10]
            b1 = shardState.get_tip().create_block_to_append()
            b1.finalize(evmState=shardState.run_block(b1))
            addResult = call_async(clusters[0].slaveList[0].add_block(b1))
            self.assertTrue(addResult)

            # Make sure the xshard list is added to another slave
            self.assertTrue(
                clusters[0].slaveList[1].shardStateMap[0b11].contain_remote_minor_block_hash(b1.header.get_hash()))
            self.assertTrue(clusters[0].master.rootState.is_minor_block_validated(b1.header.get_hash()))

            # Make sure another cluster received the new block
            assert_true_with_timeout(
                lambda: clusters[1].slaveList[0].shardStateMap[0b10].contain_block_by_hash(b1.header.get_hash()))
            assert_true_with_timeout(
                lambda: clusters[1].slaveList[1].shardStateMap[0b11].contain_remote_minor_block_hash(b1.header.get_hash()))
            assert_true_with_timeout(
                lambda: clusters[1].master.rootState.is_minor_block_validated(b1.header.get_hash()))

    def test_add_root_block_request_list(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            # add blocks in cluster 0
            blockHeaderList = []
            for i in range(13):
                shardState0 = clusters[0].slaveList[0].shardStateMap[0b10]
                b1 = shardState0.get_tip().create_block_to_append()
                b1.finalize(evmState=shardState0.run_block(b1))
                addResult = call_async(clusters[0].slaveList[0].add_block(b1))
                self.assertTrue(addResult)
                blockHeaderList.append(b1.header)

            shardState0 = clusters[0].slaveList[1].shardStateMap[0b11]
            b2 = shardState0.get_tip().create_block_to_append()
            b2.finalize(evmState=shardState0.run_block(b2))
            addResult = call_async(clusters[0].slaveList[1].add_block(b2))
            self.assertTrue(addResult)
            blockHeaderList.append(b2.header)

            # add 1 block in cluster 1
            shardState1 = clusters[1].slaveList[1].shardStateMap[0b11]
            b3 = shardState1.get_tip().create_block_to_append()
            b3.finalize(evmState=shardState1.run_block(b3))
            addResult = call_async(clusters[1].slaveList[1].add_block(b3))
            self.assertTrue(addResult)

            self.assertEqual(clusters[1].slaveList[1].shardStateMap[0b11].headerTip, b3.header)

            # reestablish cluster connection
            call_async(clusters[1].network.connect("127.0.0.1", clusters[0].master.env.config.P2P_SEED_PORT))

            rB1 = clusters[0].master.rootState.create_block_to_mine(blockHeaderList, acc1)
            call_async(clusters[0].master.add_root_block(rB1))

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

    def test_shard_synchronizer_with_fork(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            blockList = []
            # cluster 0 has 13 blocks added
            for i in range(13):
                shardState0 = clusters[0].slaveList[0].shardStateMap[0b10]
                block = shardState0.get_tip().create_block_to_append()
                block.finalize(evmState=shardState0.run_block(block))
                addResult = call_async(clusters[0].slaveList[0].add_block(block))
                self.assertTrue(addResult)
                blockList.append(block)
            self.assertEqual(clusters[0].slaveList[0].shardStateMap[0b10].headerTip.height, 14)

            # cluster 1 has 12 blocks added
            for i in range(12):
                shardState0 = clusters[1].slaveList[0].shardStateMap[0b10]
                block = shardState0.get_tip().create_block_to_append()
                block.finalize(evmState=shardState0.run_block(block))
                addResult = call_async(clusters[1].slaveList[0].add_block(block))
                self.assertTrue(addResult)
            self.assertEqual(clusters[1].slaveList[0].shardStateMap[0b10].headerTip.height, 13)

            # reestablish cluster connection
            call_async(clusters[1].network.connect("127.0.0.1", clusters[0].master.env.config.P2P_SEED_PORT))

            # a new block from cluster 0 will trigger sync in cluster 1
            shardState0 = clusters[0].slaveList[0].shardStateMap[0b10]
            block = shardState0.get_tip().create_block_to_append()
            block.finalize(evmState=shardState0.run_block(block))
            addResult = call_async(clusters[0].slaveList[0].add_block(block))
            self.assertTrue(addResult)
            blockList.append(block)

            # expect cluster 1 has all the blocks from cluter 0 and
            # has the same tip as cluster 0
            for block in blockList:
                assert_true_with_timeout(
                    lambda: clusters[1].slaveList[0].shardStateMap[0b10].contain_block_by_hash(
                        block.header.get_hash()))
                assert_true_with_timeout(
                    lambda: clusters[1].master.rootState.is_minor_block_validated(
                        block.header.get_hash()))

            self.assertEqual(clusters[1].slaveList[0].shardStateMap[0b10].headerTip,
                             clusters[0].slaveList[0].shardStateMap[0b10].headerTip)

    def test_broadcast_cross_shard_transactions(self):
        ''' Test the cross shard transactions are broadcasted to the destination shards '''
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slaveList

            tx1 = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[2 | 0],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc3,
                value=54321,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].add_tx(tx1))

            b1 = slaves[0].shardStateMap[2 | 0].create_block_to_mine(address=acc1)
            b2 = slaves[0].shardStateMap[2 | 0].create_block_to_mine(address=acc1)
            b2.header.createTime += 1
            self.assertNotEqual(b1.header.get_hash(), b2.header.get_hash())

            self.assertTrue(call_async(slaves[0].add_block(b1)))

            # expect shard 1 got the CrossShardTransactionList of b1
            xshardTxList = slaves[1].shardStateMap[2 | 1].db.get_minor_block_xshard_tx_list(b1.header.get_hash())
            self.assertEqual(len(xshardTxList.txList), 1)
            self.assertEqual(xshardTxList.txList[0].txHash, tx1.get_hash())
            self.assertEqual(xshardTxList.txList[0].fromAddress, acc1)
            self.assertEqual(xshardTxList.txList[0].toAddress, acc3)
            self.assertEqual(xshardTxList.txList[0].value, 54321)

            self.assertTrue(call_async(slaves[0].add_block(b2)))
            # b2 doesn't update tip
            self.assertEqual(slaves[0].shardStateMap[2 | 0].headerTip, b1.header)

            # expect shard 1 got the CrossShardTransactionList of b2
            xshardTxList = slaves[1].shardStateMap[2 | 1].db.get_minor_block_xshard_tx_list(b2.header.get_hash())
            self.assertEqual(len(xshardTxList.txList), 1)
            self.assertEqual(xshardTxList.txList[0].txHash, tx1.get_hash())
            self.assertEqual(xshardTxList.txList[0].fromAddress, acc1)
            self.assertEqual(xshardTxList.txList[0].toAddress, acc3)
            self.assertEqual(xshardTxList.txList[0].value, 54321)

            b3 = slaves[1].shardStateMap[2 | 1].create_block_to_mine(address=acc1.address_in_shard(1))
            self.assertTrue(call_async(slaves[1].add_block(b3)))

            isRoot, rB = call_async(master.get_next_block_to_mine(address=acc1))
            call_async(master.add_root_block(rB))

            # b4 should include the withdraw of tx1
            b4 = slaves[1].shardStateMap[2 | 1].create_block_to_mine(address=acc1.address_in_shard(1))

            # adding b1, b2, b3 again shouldn't affect b4 to be added later
            self.assertTrue(call_async(slaves[0].add_block(b1)))
            self.assertTrue(call_async(slaves[0].add_block(b2)))
            self.assertTrue(call_async(slaves[1].add_block(b3)))

            self.assertTrue(call_async(slaves[1].add_block(b4)))
            self.assertEqual(call_async(master.get_primary_account_data(acc3)).balance, 54321)

