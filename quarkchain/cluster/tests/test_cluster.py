import unittest
from unittest.mock import MagicMock
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    ClusterContext,
)
from quarkchain.core import Address, Branch, Identity
from quarkchain.evm import opcodes
from quarkchain.utils import call_async, assert_true_with_timeout


class TestCluster(unittest.TestCase):
    def test_single_cluster(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        with ClusterContext(1, acc1) as clusters:
            self.assertEqual(len(clusters), 1)

    def test_three_clusters(self):
        with ClusterContext(3) as clusters:
            self.assertEqual(len(clusters), 3)

    def test_get_next_block_to_mine(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Expect to mine root that includes the genesis minor blocks
            is_root, block = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertTrue(is_root)
            self.assertEqual(block.header.height, 1)
            self.assertEqual(len(block.minor_block_header_list), 2)
            self.assertEqual(block.minor_block_header_list[0].height, 0)
            self.assertEqual(block.minor_block_header_list[1].height, 0)
            call_async(master.add_root_block(block))

            tx = create_transfer_transaction(
                shard_state=slaves[0].shards[Branch(2 | 0)].state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc3,
                value=54321,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
                gas_price=3,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            # Expect to mine shard 0 since it has one tx
            is_root, block1 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertFalse(is_root)
            self.assertEqual(block1.header.height, 1)
            self.assertEqual(block1.header.branch.value, 0b10)
            self.assertEqual(len(block1.tx_list), 1)

            originalBalanceAcc1 = call_async(
                master.get_primary_account_data(acc1)
            ).balance
            gasPaid = (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 3
            self.assertTrue(
                call_async(
                    master.add_raw_minor_block(block1.header.branch, block1.serialize())
                )
            )
            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).balance,
                originalBalanceAcc1 - 54321 - gasPaid,
            )
            self.assertEqual(
                slaves[1].shards[Branch(3)].state.get_balance(acc3.recipient), 0
            )

            # Expect to mine shard 1 due to proof-of-progress
            is_root, block2 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertFalse(is_root)
            self.assertEqual(block2.header.height, 1)
            self.assertEqual(block2.header.branch.value, 0b11)
            self.assertEqual(len(block2.tx_list), 0)

            self.assertTrue(
                call_async(
                    master.add_raw_minor_block(block2.header.branch, block2.serialize())
                )
            )

            # Expect to mine root
            is_root, block = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertTrue(is_root)
            self.assertEqual(block.header.height, 2)
            self.assertEqual(len(block.minor_block_header_list), 2)
            self.assertEqual(block.minor_block_header_list[0], block1.header)
            self.assertEqual(block.minor_block_header_list[1], block2.header)

            self.assertTrue(master.root_state.add_block(block))
            slaves[1].shards[Branch(3)].state.add_root_block(block)
            self.assertEqual(
                slaves[1].shards[Branch(3)].state.get_balance(acc3.recipient), 0
            )

            # Expect to mine shard 1 for the gas on xshard tx to acc3
            is_root, block3 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertFalse(is_root)
            self.assertEqual(block3.header.height, 2)
            self.assertEqual(block3.header.branch.value, 0b11)
            self.assertEqual(len(block3.tx_list), 0)

            self.assertTrue(
                call_async(
                    master.add_raw_minor_block(block3.header.branch, block3.serialize())
                )
            )
            # Expect withdrawTo is included in acc3's balance
            self.assertEqual(
                call_async(master.get_primary_account_data(acc3)).balance, 54321
            )

    def test_get_primary_account_data(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            branch = Branch.create(2, 0)
            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).transaction_count, 0
            )
            tx = create_transfer_transaction(
                shard_state=slaves[0].shards[branch].state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            is_root, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(
                call_async(
                    master.add_raw_minor_block(block1.header.branch, block1.serialize())
                )
            )

            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).transaction_count, 1
            )
            self.assertEqual(
                call_async(master.get_primary_account_data(acc2)).transaction_count, 0
            )

    def test_add_transaction(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id1, full_shard_id=1)

        with ClusterContext(2, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            branch0 = Branch.create(2, 0)
            tx1 = create_transfer_transaction(
                shard_state=slaves[0].shards[branch0].state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(call_async(master.add_transaction(tx1)))
            self.assertEqual(len(slaves[0].shards[branch0].state.tx_queue), 1)

            branch1 = Branch.create(2, 1)
            tx2 = create_transfer_transaction(
                shard_state=slaves[1].shards[branch1].state,
                key=id1.get_key(),
                from_address=acc2,
                to_address=acc1,
                value=12345,
                gas=30000,
            )
            self.assertTrue(call_async(master.add_transaction(tx2)))
            self.assertEqual(len(slaves[1].shards[branch1].state.tx_queue), 1)

            # check the tx is received by the other cluster
            tx_queue = clusters[1].slave_list[0].shards[branch0].state.tx_queue
            assert_true_with_timeout(lambda: len(tx_queue) == 1)
            self.assertEqual(tx_queue.pop_transaction(), tx1.code.get_evm_transaction())

            tx_queue = clusters[1].slave_list[1].shards[branch1].state.tx_queue
            assert_true_with_timeout(lambda: len(tx_queue) == 1)
            self.assertEqual(tx_queue.pop_transaction(), tx2.code.get_evm_transaction())

    def test_add_minor_block_request_list(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        with ClusterContext(2, acc1) as clusters:
            shard_state = clusters[0].slave_list[0].shards[Branch(0b10)].state
            b1 = shard_state.get_tip().create_block_to_append()
            b1.finalize(evm_state=shard_state.run_block(b1))
            addResult = call_async(
                clusters[0].master.add_raw_minor_block(b1.header.branch, b1.serialize())
            )
            self.assertTrue(addResult)

            # Make sure the xshard list is not broadcasted to the other shard
            self.assertFalse(
                clusters[0]
                .slave_list[1]
                .shards[Branch(0b11)]
                .state.contain_remote_minor_block_hash(b1.header.get_hash())
            )
            self.assertTrue(
                clusters[0].master.root_state.is_minor_block_validated(
                    b1.header.get_hash()
                )
            )

            # Make sure another cluster received the new block
            assert_true_with_timeout(
                lambda: clusters[1]
                .slave_list[0]
                .shards[Branch(0b10)]
                .state.contain_block_by_hash(b1.header.get_hash())
            )
            assert_true_with_timeout(
                lambda: clusters[1].master.root_state.is_minor_block_validated(
                    b1.header.get_hash()
                )
            )

    def test_add_root_block_request_list(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            # add blocks in cluster 0
            block_header_list = [clusters[0].get_shard_state(0).header_tip]
            for i in range(13):
                shardState0 = clusters[0].slave_list[0].shards[Branch(0b10)].state
                b1 = shardState0.get_tip().create_block_to_append()
                b1.finalize(evm_state=shardState0.run_block(b1))
                addResult = call_async(
                    clusters[0].master.add_raw_minor_block(
                        b1.header.branch, b1.serialize()
                    )
                )
                self.assertTrue(addResult)
                block_header_list.append(b1.header)

            block_header_list.append(clusters[0].get_shard_state(1).header_tip)
            shardState0 = clusters[0].slave_list[1].shards[Branch(0b11)].state
            b2 = shardState0.get_tip().create_block_to_append()
            b2.finalize(evm_state=shardState0.run_block(b2))
            addResult = call_async(
                clusters[0].master.add_raw_minor_block(b2.header.branch, b2.serialize())
            )
            self.assertTrue(addResult)
            block_header_list.append(b2.header)

            # add 1 block in cluster 1
            shardState1 = clusters[1].slave_list[1].shards[Branch(0b11)].state
            b3 = shardState1.get_tip().create_block_to_append()
            b3.finalize(evm_state=shardState1.run_block(b3))
            addResult = call_async(
                clusters[1].master.add_raw_minor_block(b3.header.branch, b3.serialize())
            )
            self.assertTrue(addResult)

            self.assertEqual(
                clusters[1].slave_list[1].shards[Branch(0b11)].state.header_tip,
                b3.header,
            )

            # reestablish cluster connection
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1",
                    clusters[0].master.env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_PORT,
                )
            )

            root_block1 = clusters[0].master.root_state.create_block_to_mine(
                block_header_list, acc1
            )
            call_async(clusters[0].master.add_root_block(root_block1))

            # Make sure the root block tip of local cluster is changed
            self.assertEqual(clusters[0].master.root_state.tip, root_block1.header)

            # Make sure the root block tip of cluster 1 is changed
            assert_true_with_timeout(
                lambda: clusters[1].master.root_state.tip == root_block1.header, 2
            )

            # Minor block is downloaded
            self.assertEqual(b1.header.height, 13)
            assert_true_with_timeout(
                lambda: clusters[1].slave_list[0].shards[Branch(0b10)].state.header_tip
                == b1.header
            )

            # The tip is overwritten due to root chain first consensus
            assert_true_with_timeout(
                lambda: clusters[1].slave_list[1].shards[Branch(0b11)].state.header_tip
                == b2.header
            )

    def test_shard_synchronizer_with_fork(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            blockList = []
            # cluster 0 has 13 blocks added
            for i in range(13):
                shardState0 = clusters[0].slave_list[0].shards[Branch(0b10)].state
                block = shardState0.get_tip().create_block_to_append()
                block.finalize(evm_state=shardState0.run_block(block))
                addResult = call_async(
                    clusters[0].master.add_raw_minor_block(
                        block.header.branch, block.serialize()
                    )
                )
                self.assertTrue(addResult)
                blockList.append(block)
            self.assertEqual(
                clusters[0].slave_list[0].shards[Branch(0b10)].state.header_tip.height,
                13,
            )

            # cluster 1 has 12 blocks added
            for i in range(12):
                shardState0 = clusters[1].slave_list[0].shards[Branch(0b10)].state
                block = shardState0.get_tip().create_block_to_append()
                block.finalize(evm_state=shardState0.run_block(block))
                addResult = call_async(
                    clusters[1].master.add_raw_minor_block(
                        block.header.branch, block.serialize()
                    )
                )
                self.assertTrue(addResult)
            self.assertEqual(
                clusters[1].slave_list[0].shards[Branch(0b10)].state.header_tip.height,
                12,
            )

            # reestablish cluster connection
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1",
                    clusters[0].master.env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_PORT,
                )
            )

            # a new block from cluster 0 will trigger sync in cluster 1
            shardState0 = clusters[0].slave_list[0].shards[Branch(0b10)].state
            block = shardState0.get_tip().create_block_to_append()
            block.finalize(evm_state=shardState0.run_block(block))
            addResult = call_async(
                clusters[0].master.add_raw_minor_block(
                    block.header.branch, block.serialize()
                )
            )
            self.assertTrue(addResult)
            blockList.append(block)

            # expect cluster 1 has all the blocks from cluter 0 and
            # has the same tip as cluster 0
            for block in blockList:
                assert_true_with_timeout(
                    lambda: clusters[1]
                    .slave_list[0]
                    .shards[Branch(0b10)]
                    .state.contain_block_by_hash(block.header.get_hash())
                )
                assert_true_with_timeout(
                    lambda: clusters[1].master.root_state.is_minor_block_validated(
                        block.header.get_hash()
                    )
                )

            self.assertEqual(
                clusters[1].slave_list[0].shards[Branch(0b10)].state.header_tip,
                clusters[0].slave_list[0].shards[Branch(0b10)].state.header_tip,
            )

    def test_broadcast_cross_shard_transactions(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            is_root, root_block = call_async(master.get_next_block_to_mine(Address.create_empty_account(), prefer_root=True))
            self.assertTrue(is_root)
            call_async(master.add_root_block(root_block))

            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc3,
                value=54321,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].add_tx(tx1))

            b1 = clusters[0].get_shard_state(0).create_block_to_mine(address=acc1)
            b2 = clusters[0].get_shard_state(0).create_block_to_mine(address=acc1)
            b2.header.create_time += 1
            self.assertNotEqual(b1.header.get_hash(), b2.header.get_hash())

            call_async(clusters[0].get_shard(0).add_block(b1))

            # expect shard 1 got the CrossShardTransactionList of b1
            xshardTxList = (
                clusters[0]
                .get_shard_state(1)
                .db.get_minor_block_xshard_tx_list(b1.header.get_hash())
            )
            self.assertEqual(len(xshardTxList.tx_list), 1)
            self.assertEqual(xshardTxList.tx_list[0].tx_hash, tx1.get_hash())
            self.assertEqual(xshardTxList.tx_list[0].from_address, acc1)
            self.assertEqual(xshardTxList.tx_list[0].to_address, acc3)
            self.assertEqual(xshardTxList.tx_list[0].value, 54321)

            call_async(clusters[0].get_shard(0).add_block(b2))
            # b2 doesn't update tip
            self.assertEqual(clusters[0].get_shard_state(0).header_tip, b1.header)

            # expect shard 1 got the CrossShardTransactionList of b2
            xshardTxList = (
                clusters[0]
                .get_shard_state(1)
                .db.get_minor_block_xshard_tx_list(b2.header.get_hash())
            )
            self.assertEqual(len(xshardTxList.tx_list), 1)
            self.assertEqual(xshardTxList.tx_list[0].tx_hash, tx1.get_hash())
            self.assertEqual(xshardTxList.tx_list[0].from_address, acc1)
            self.assertEqual(xshardTxList.tx_list[0].to_address, acc3)
            self.assertEqual(xshardTxList.tx_list[0].value, 54321)

            b3 = (
                slaves[1]
                .shards[Branch(2 | 1)]
                .state.create_block_to_mine(address=acc1.address_in_shard(1))
            )
            call_async(master.add_raw_minor_block(b3.header.branch, b3.serialize()))

            is_root, root_block = call_async(
                master.get_next_block_to_mine(address=acc1)
            )
            self.assertTrue(is_root)
            call_async(master.add_root_block(root_block))

            # b4 should include the withdraw of tx1
            b4 = (
                slaves[1]
                .shards[Branch(2 | 1)]
                .state.create_block_to_mine(address=acc1.address_in_shard(1))
            )

            # adding b1, b2, b3 again shouldn't affect b4 to be added later
            self.assertTrue(
                call_async(master.add_raw_minor_block(b1.header.branch, b1.serialize()))
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b2.header.branch, b2.serialize()))
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b3.header.branch, b3.serialize()))
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b4.header.branch, b4.serialize()))
            )
            self.assertEqual(
                call_async(master.get_primary_account_data(acc3)).balance, 54321
            )

    def test_broadcast_cross_shard_transactions_to_neighbor_only(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        # create 64 shards so that the neighbor rule can kick in
        # explicitly set num_slaves to 4 so that it does not spin up 64 slaves
        with ClusterContext(1, acc1, 64, num_slaves=4) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            is_root, root_block = call_async(
                master.get_next_block_to_mine(Address.create_empty_account(), prefer_root=True))
            self.assertTrue(is_root)
            call_async(master.add_root_block(root_block))

            b1 = (
                slaves[0]
                .shards[Branch(64 | 0)]
                .state.create_block_to_mine(address=acc1)
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b1.header.branch, b1.serialize()))
            )

            neighbor_shards = [2 ** i for i in range(6)]
            for shard_id in range(64):
                xshardTxList = (
                    clusters[0]
                    .get_shard_state(shard_id)
                    .db.get_minor_block_xshard_tx_list(b1.header.get_hash())
                )
                # Only neighbor should have it
                if shard_id in neighbor_shards:
                    self.assertIsNotNone(xshardTxList)
                else:
                    self.assertIsNone(xshardTxList)
