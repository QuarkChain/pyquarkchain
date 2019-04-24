import unittest
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    ClusterContext,
)
from quarkchain.core import (
    Address,
    Branch,
    Identity,
    TokenBalanceMap,
    XshardTxCursorInfo,
)
from quarkchain.evm import opcodes
from quarkchain.utils import call_async, assert_true_with_timeout
from quarkchain.cluster.p2p_commands import (
    CommandOp,
    GetRootBlockHeaderListWithSkipRequest,
    GetMinorBlockHeaderListWithSkipRequest,
    Direction,
)


def _tip_gen(shard_state):
    coinbase_amount = (
        shard_state.env.quark_chain_config.shards[
            shard_state.full_shard_id
        ].COINBASE_AMOUNT
        // 2
    )
    b = shard_state.get_tip().create_block_to_append()
    evm_state = shard_state.run_block(b)
    coinbase_amount_map = TokenBalanceMap(evm_state.block_fee_tokens)
    coinbase_amount_map.add(
        {shard_state.env.quark_chain_config.genesis_token: coinbase_amount}
    )
    b.finalize(evm_state=evm_state, coinbase_amount_map=coinbase_amount_map)
    return b


class TestCluster(unittest.TestCase):
    def test_single_cluster(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        with ClusterContext(1, acc1) as clusters:
            self.assertEqual(len(clusters), 1)

    def test_three_clusters(self):
        with ClusterContext(3) as clusters:
            self.assertEqual(len(clusters), 3)

    def test_create_shard_at_different_height(self):
        acc1 = Address.create_random_account(0)
        id1 = 0 << 16 | 1 | 0
        id2 = 1 << 16 | 1 | 0
        genesis_root_heights = {id1: 1, id2: 2}
        with ClusterContext(
            1,
            acc1,
            chain_size=2,
            shard_size=1,
            genesis_root_heights=genesis_root_heights,
        ) as clusters:
            master = clusters[0].master

            self.assertIsNone(clusters[0].get_shard(id1))
            self.assertIsNone(clusters[0].get_shard(id2))

            # Add root block with height 1, which will automatically create genesis block for shard 0
            root0 = call_async(master.get_next_block_to_mine(acc1, branch_value=None))
            self.assertEqual(root0.header.height, 1)
            self.assertEqual(len(root0.minor_block_header_list), 0)
            self.assertEqual(
                root0.header.coinbase_amount_map.balance_map[
                    master.env.quark_chain_config.genesis_token
                ],
                master.env.quark_chain_config.ROOT.COINBASE_AMOUNT,
            )
            call_async(master.add_root_block(root0))

            # shard 0 created at root height 1
            self.assertIsNotNone(clusters[0].get_shard(id1))
            self.assertIsNone(clusters[0].get_shard(id2))

            # shard 0 block should have correct root block and cursor info
            shard_state = clusters[0].get_shard(id1).state
            self.assertEqual(
                shard_state.header_tip.hash_prev_root_block, root0.header.get_hash()
            )
            self.assertEqual(
                shard_state.get_tip().meta.xshard_tx_cursor_info,
                XshardTxCursorInfo(1, 0, 0),
            )
            self.assertEqual(
                shard_state.get_token_balance(
                    acc1.recipient, shard_state.env.quark_chain_config.genesis_token
                ),
                1000000,  # from create_test_clusters in genesis alloc
            )

            # Add root block with height 2, which will automatically create genesis block for shard 1
            root1 = call_async(master.get_next_block_to_mine(acc1, branch_value=None))
            self.assertEqual(len(root1.minor_block_header_list), 1)
            self.assertEqual(
                root1.header.coinbase_amount_map.balance_map[
                    master.env.quark_chain_config.genesis_token
                ],
                master.env.quark_chain_config.ROOT.COINBASE_AMOUNT
                + root1.minor_block_header_list[0].coinbase_amount_map.balance_map[
                    master.env.quark_chain_config.genesis_token
                ],
            )
            self.assertEqual(root1.minor_block_header_list[0], shard_state.header_tip)
            call_async(master.add_root_block(root1))

            self.assertIsNotNone(clusters[0].get_shard(id1))
            # shard 1 created at root height 2
            self.assertIsNotNone(clusters[0].get_shard(id2))

            # X-shard from root should be deposited to the shard
            mblock = shard_state.create_block_to_mine()
            self.assertEqual(
                mblock.meta.xshard_tx_cursor_info,
                XshardTxCursorInfo(root1.header.height + 1, 0, 0),
            )
            call_async(clusters[0].get_shard(id1).add_block(mblock))
            self.assertEqual(
                shard_state.get_token_balance(
                    acc1.recipient, shard_state.env.quark_chain_config.genesis_token
                ),
                root1.header.coinbase_amount_map.balance_map[
                    shard_state.env.quark_chain_config.genesis_token
                ]
                + root0.header.coinbase_amount_map.balance_map[
                    shard_state.env.quark_chain_config.genesis_token
                ]
                + 1000000,  # from create_test_clusters in genesis alloc
            )
            self.assertEqual(
                mblock.header.coinbase_amount_map.balance_map[
                    shard_state.env.quark_chain_config.genesis_token
                ],
                shard_state.shard_config.COINBASE_AMOUNT // 2,
            )

            # Add root block with height 3, which will include
            # - the genesis block for shard 1; and
            # - the added block for shard 0.
            root2 = call_async(master.get_next_block_to_mine(acc1, branch_value=None))
            self.assertEqual(len(root2.minor_block_header_list), 2)

    def test_get_primary_account_data(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).transaction_count, 0
            )

            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            root = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(root))

            block1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)

        with ClusterContext(2, acc1) as clusters:
            master = clusters[0].master

            root = call_async(master.get_next_block_to_mine(acc1, branch_value=None))
            call_async(master.add_root_block(root))

            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(call_async(master.add_transaction(tx1)))
            self.assertEqual(len(clusters[0].get_shard_state(0b10).tx_queue), 1)

            tx2 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b11),
                key=id1.get_key(),
                from_address=acc2,
                to_address=acc1,
                value=12345,
                gas=30000,
            )
            self.assertTrue(call_async(master.add_transaction(tx2)))
            self.assertEqual(len(clusters[0].get_shard_state(0b11).tx_queue), 1)

            # check the tx is received by the other cluster
            tx_queue = clusters[1].get_shard_state(0b10).tx_queue
            assert_true_with_timeout(lambda: len(tx_queue) == 1)
            self.assertEqual(tx_queue.pop_transaction(), tx1.tx.to_evm_tx())

            tx_queue = clusters[1].get_shard_state(0b11).tx_queue
            assert_true_with_timeout(lambda: len(tx_queue) == 1)
            self.assertEqual(tx_queue.pop_transaction(), tx2.tx.to_evm_tx())

    def test_add_minor_block_request_list(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1) as clusters:
            shard_state = clusters[0].get_shard_state(0b10)
            b1 = _tip_gen(shard_state)
            add_result = call_async(
                clusters[0].master.add_raw_minor_block(b1.header.branch, b1.serialize())
            )
            self.assertTrue(add_result)

            # Make sure the xshard list is not broadcasted to the other shard
            self.assertFalse(
                clusters[0]
                .get_shard_state(0b11)
                .contain_remote_minor_block_hash(b1.header.get_hash())
            )
            self.assertTrue(
                clusters[0].master.root_state.db.contain_minor_block_by_hash(
                    b1.header.get_hash()
                )
            )

            # Make sure another cluster received the new block
            assert_true_with_timeout(
                lambda: clusters[0]
                .get_shard_state(0b10)
                .contain_block_by_hash(b1.header.get_hash())
            )
            assert_true_with_timeout(
                lambda: clusters[1].master.root_state.db.contain_minor_block_by_hash(
                    b1.header.get_hash()
                )
            )

    def test_add_root_block_request_list(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            # add blocks in cluster 0
            block_header_list = [clusters[0].get_shard_state(2 | 0).header_tip]
            shard_state0 = clusters[0].get_shard_state(0b10)
            for i in range(7):
                b1 = _tip_gen(shard_state0)
                add_result = call_async(
                    clusters[0].master.add_raw_minor_block(
                        b1.header.branch, b1.serialize()
                    )
                )
                self.assertTrue(add_result)
                block_header_list.append(b1.header)

            block_header_list.append(clusters[0].get_shard_state(2 | 1).header_tip)
            shard_state0 = clusters[0].get_shard_state(0b11)
            b2 = _tip_gen(shard_state0)
            add_result = call_async(
                clusters[0].master.add_raw_minor_block(b2.header.branch, b2.serialize())
            )
            self.assertTrue(add_result)
            block_header_list.append(b2.header)

            # add 1 block in cluster 1
            shard_state1 = clusters[1].get_shard_state(0b11)
            b3 = _tip_gen(shard_state1)
            add_result = call_async(
                clusters[1].master.add_raw_minor_block(b3.header.branch, b3.serialize())
            )
            self.assertTrue(add_result)

            self.assertEqual(clusters[1].get_shard_state(0b11).header_tip, b3.header)

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
            self.assertEqual(b1.header.height, 7)
            assert_true_with_timeout(
                lambda: clusters[1].get_shard_state(0b10).header_tip == b1.header
            )

            # The tip is overwritten due to root chain first consensus
            assert_true_with_timeout(
                lambda: clusters[1].get_shard_state(0b11).header_tip == b2.header
            )

    def test_shard_synchronizer_with_fork(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            block_list = []
            # cluster 0 has 13 blocks added
            shard_state0 = clusters[0].get_shard_state(0b10)
            for i in range(13):
                block = _tip_gen(shard_state0)
                add_result = call_async(
                    clusters[0].master.add_raw_minor_block(
                        block.header.branch, block.serialize()
                    )
                )
                self.assertTrue(add_result)
                block_list.append(block)
            self.assertEqual(clusters[0].get_shard_state(0b10).header_tip.height, 13)

            # cluster 1 has 12 blocks added
            shard_state0 = clusters[1].get_shard_state(0b10)
            for i in range(12):
                block = _tip_gen(shard_state0)
                add_result = call_async(
                    clusters[1].master.add_raw_minor_block(
                        block.header.branch, block.serialize()
                    )
                )
                self.assertTrue(add_result)
            self.assertEqual(clusters[1].get_shard_state(0b10).header_tip.height, 12)

            # reestablish cluster connection
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1",
                    clusters[0].master.env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_PORT,
                )
            )

            # a new block from cluster 0 will trigger sync in cluster 1
            shard_state0 = clusters[0].get_shard_state(0b10)
            block = _tip_gen(shard_state0)
            add_result = call_async(
                clusters[0].master.add_raw_minor_block(
                    block.header.branch, block.serialize()
                )
            )
            self.assertTrue(add_result)
            block_list.append(block)

            # expect cluster 1 has all the blocks from cluter 0 and
            # has the same tip as cluster 0
            for block in block_list:
                assert_true_with_timeout(
                    lambda: clusters[1]
                    .slave_list[0]
                    .shards[Branch(0b10)]
                    .state.contain_block_by_hash(block.header.get_hash())
                )
                assert_true_with_timeout(
                    lambda: clusters[
                        1
                    ].master.root_state.db.contain_minor_block_by_hash(
                        block.header.get_hash()
                    )
                )

            self.assertEqual(
                clusters[1].get_shard_state(0b10).header_tip,
                clusters[0].get_shard_state(0b10).header_tip,
            )

    def test_shard_genesis_fork_fork(self):
        """ Test shard forks at genesis blocks due to root chain fork at GENESIS.ROOT_HEIGHT"""
        acc1 = Address.create_random_account(0)
        acc2 = Address.create_random_account(1)

        genesis_root_heights = {2: 0, 3: 1}
        with ClusterContext(
            2,
            acc1,
            chain_size=1,
            shard_size=2,
            genesis_root_heights=genesis_root_heights,
        ) as clusters:
            # shutdown cluster connection
            clusters[1].peer.close()

            master0 = clusters[0].master
            root0 = call_async(master0.get_next_block_to_mine(acc1, branch_value=None))
            call_async(master0.add_root_block(root0))
            genesis0 = (
                clusters[0].get_shard_state(2 | 1).db.get_minor_block_by_height(0)
            )
            self.assertEqual(
                genesis0.header.hash_prev_root_block, root0.header.get_hash()
            )

            master1 = clusters[1].master
            root1 = call_async(master1.get_next_block_to_mine(acc2, branch_value=None))
            self.assertNotEqual(root0.header.get_hash(), root1.header.get_hash())
            call_async(master1.add_root_block(root1))
            genesis1 = (
                clusters[1].get_shard_state(2 | 1).db.get_minor_block_by_height(0)
            )
            self.assertEqual(
                genesis1.header.hash_prev_root_block, root1.header.get_hash()
            )

            self.assertNotEqual(genesis0.header.get_hash(), genesis1.header.get_hash())

            # let's make cluster1's root chain longer than cluster0's
            root2 = call_async(master1.get_next_block_to_mine(acc2, branch_value=None))
            call_async(master1.add_root_block(root2))
            self.assertEqual(master1.root_state.tip.height, 2)

            # reestablish cluster connection
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1",
                    clusters[0].master.env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_PORT,
                )
            )
            # Expect cluster0's genesis change to genesis1
            assert_true_with_timeout(
                lambda: clusters[0]
                .get_shard_state(2 | 1)
                .db.get_minor_block_by_height(0)
                .header.get_hash()
                == genesis1.header.get_hash()
            )
            self.assertTrue(clusters[0].get_shard_state(2 | 1).root_tip == root2.header)

    def test_broadcast_cross_shard_transactions(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list
            genesis_token = (
                clusters[0].get_shard_state(2 | 0).env.quark_chain_config.genesis_token
            )

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc3,
                value=54321,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].add_tx(tx1))

            b1 = clusters[0].get_shard_state(2 | 0).create_block_to_mine(address=acc1)
            b2 = clusters[0].get_shard_state(2 | 0).create_block_to_mine(address=acc1)
            b2.header.create_time += 1
            self.assertNotEqual(b1.header.get_hash(), b2.header.get_hash())

            call_async(clusters[0].get_shard(2 | 0).add_block(b1))

            # expect shard 1 got the CrossShardTransactionList of b1
            xshard_tx_list = (
                clusters[0]
                .get_shard_state(2 | 1)
                .db.get_minor_block_xshard_tx_list(b1.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 1)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx1.get_hash())
            self.assertEqual(xshard_tx_list.tx_list[0].from_address, acc1)
            self.assertEqual(xshard_tx_list.tx_list[0].to_address, acc3)
            self.assertEqual(xshard_tx_list.tx_list[0].value, 54321)

            call_async(clusters[0].get_shard(2 | 0).add_block(b2))
            # b2 doesn't update tip
            self.assertEqual(clusters[0].get_shard_state(2 | 0).header_tip, b1.header)

            # expect shard 1 got the CrossShardTransactionList of b2
            xshard_tx_list = (
                clusters[0]
                .get_shard_state(2 | 1)
                .db.get_minor_block_xshard_tx_list(b2.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 1)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx1.get_hash())
            self.assertEqual(xshard_tx_list.tx_list[0].from_address, acc1)
            self.assertEqual(xshard_tx_list.tx_list[0].to_address, acc3)
            self.assertEqual(xshard_tx_list.tx_list[0].value, 54321)

            b3 = (
                clusters[0]
                .get_shard_state(2 | 1)
                .create_block_to_mine(address=acc1.address_in_shard(1))
            )
            call_async(master.add_raw_minor_block(b3.header.branch, b3.serialize()))

            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            # b4 should include the withdraw of tx1
            b4 = (
                clusters[0]
                .get_shard_state(2 | 1)
                .create_block_to_mine(address=acc1.address_in_shard(1))
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
                call_async(
                    master.get_primary_account_data(acc3)
                ).token_balances.balance_map,
                {genesis_token: 54321},
            )

    def test_broadcast_cross_shard_transactions_to_neighbor_only(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        # create 64 shards so that the neighbor rule can kick in
        # explicitly set num_slaves to 4 so that it does not spin up 64 slaves
        with ClusterContext(1, acc1, shard_size=64, num_slaves=4) as clusters:
            master = clusters[0].master

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            b1 = clusters[0].get_shard_state(64).create_block_to_mine(address=acc1)
            self.assertTrue(
                call_async(master.add_raw_minor_block(b1.header.branch, b1.serialize()))
            )

            neighbor_shards = [2 ** i for i in range(6)]
            for shard_id in range(64):
                xshard_tx_list = (
                    clusters[0]
                    .get_shard_state(64 | shard_id)
                    .db.get_minor_block_xshard_tx_list(b1.header.get_hash())
                )
                # Only neighbor should have it
                if shard_id in neighbor_shards:
                    self.assertIsNotNone(xshard_tx_list)
                else:
                    self.assertIsNone(xshard_tx_list)

    def test_get_work_from_slave(self):
        genesis = Address.create_empty_account(full_shard_key=0)

        with ClusterContext(1, genesis, remote_mining=True) as clusters:
            slaves = clusters[0].slave_list

            # no posw
            state = clusters[0].get_shard_state(2 | 0)
            branch = state.create_block_to_mine().header.branch
            work = call_async(slaves[0].get_work(branch))
            self.assertEqual(work.difficulty, 10)

            # enable posw, with total stakes cover all the window
            state.shard_config.POSW_CONFIG.ENABLED = True
            state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 500000
            work = call_async(slaves[0].get_work(branch))
            self.assertEqual(work.difficulty, 0)

    def test_handle_get_minor_block_list_request_with_total_diff(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1) as clusters:
            cluster0_root_state = clusters[0].master.root_state
            cluster1_root_state = clusters[1].master.root_state
            coinbase = cluster1_root_state._calculate_root_block_coinbase([], 0)

            # Cluster 0 generates a root block of height 1 with 1e6 difficulty
            rb0 = cluster0_root_state.get_tip_block()
            rb1 = rb0.create_block_to_append(difficulty=int(1e6)).finalize(coinbase)

            # Establish cluster connection
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1",
                    clusters[0].master.env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_PORT,
                )
            )

            # Cluster 0 broadcasts the root block to cluster 1
            call_async(clusters[0].master.add_root_block(rb1))
            self.assertEqual(cluster0_root_state.tip.get_hash(), rb1.header.get_hash())

            # Make sure the root block tip of cluster 1 is changed
            assert_true_with_timeout(lambda: cluster1_root_state.tip == rb1.header, 2)

            # Cluster 1 generates a minor block and broadcasts to cluster 0
            shard_state = clusters[1].get_shard_state(0b10)
            b1 = _tip_gen(shard_state)
            add_result = call_async(
                clusters[1].master.add_raw_minor_block(b1.header.branch, b1.serialize())
            )
            self.assertTrue(add_result)

            # Make sure another cluster received the new minor block
            assert_true_with_timeout(
                lambda: clusters[1]
                .get_shard_state(0b10)
                .contain_block_by_hash(b1.header.get_hash())
            )
            assert_true_with_timeout(
                lambda: clusters[0].master.root_state.db.contain_minor_block_by_hash(
                    b1.header.get_hash()
                )
            )

            # Cluster 1 generates a new root block with higher total difficulty
            rb2 = rb0.create_block_to_append(difficulty=int(3e6)).finalize(coinbase)
            call_async(clusters[1].master.add_root_block(rb2))
            self.assertEqual(cluster1_root_state.tip.get_hash(), rb2.header.get_hash())

            # Generate a minor block b2
            b2 = _tip_gen(shard_state)
            add_result = call_async(
                clusters[1].master.add_raw_minor_block(b2.header.branch, b2.serialize())
            )
            self.assertTrue(add_result)

            # Make sure another cluster received the new minor block
            assert_true_with_timeout(
                lambda: clusters[1]
                .get_shard_state(0b10)
                .contain_block_by_hash(b2.header.get_hash())
            )
            assert_true_with_timeout(
                lambda: clusters[0].master.root_state.db.contain_minor_block_by_hash(
                    b2.header.get_hash()
                )
            )

    def test_new_block_header_pool(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(1, acc1) as clusters:
            shard_state = clusters[0].get_shard_state(0b10)
            b1 = _tip_gen(shard_state)
            add_result = call_async(
                clusters[0].master.add_raw_minor_block(b1.header.branch, b1.serialize())
            )
            self.assertTrue(add_result)

            # Update config to force checking diff
            clusters[
                0
            ].master.env.quark_chain_config.SKIP_MINOR_DIFFICULTY_CHECK = False
            b2 = b1.create_block_to_append(difficulty=12345)
            shard = clusters[0].slave_list[0].shards[b2.header.branch]
            with self.assertRaises(ValueError):
                call_async(shard.handle_new_block(b2))
            # Also the block should not exist in new block pool
            self.assertTrue(
                b2.header.get_hash() not in shard.state.new_block_header_pool
            )

    def test_get_root_block_headers_with_skip(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1) as clusters:
            master = clusters[0].master

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            root_block_header_list = [master.root_state.tip]
            for i in range(10):
                root_block = call_async(
                    master.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master.add_root_block(root_block))
                root_block_header_list.append(root_block.header)

            self.assertEqual(root_block_header_list[-1].height, 10)
            assert_true_with_timeout(
                lambda: clusters[1].master.root_state.tip.height == 10
            )

            peer = clusters[1].peer

            # Test Case 1 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_height(
                        height=1, skip=1, limit=3, direction=Direction.TIP
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[1])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[3])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[5])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=root_block_header_list[1].get_hash(),
                        skip=1,
                        limit=3,
                        direction=Direction.TIP,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[1])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[3])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[5])

            # Test Case 2 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_height(
                        height=2, skip=2, limit=4, direction=Direction.TIP
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[2])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[5])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[8])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=root_block_header_list[2].get_hash(),
                        skip=2,
                        limit=4,
                        direction=Direction.TIP,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[2])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[5])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[8])

            # Test Case 3 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_height(
                        height=6, skip=0, limit=100, direction=Direction.TIP
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[6])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[7])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[8])
            self.assertEqual(resp.block_header_list[3], root_block_header_list[9])
            self.assertEqual(resp.block_header_list[4], root_block_header_list[10])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=root_block_header_list[6].get_hash(),
                        skip=0,
                        limit=100,
                        direction=Direction.TIP,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[6])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[7])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[8])
            self.assertEqual(resp.block_header_list[3], root_block_header_list[9])
            self.assertEqual(resp.block_header_list[4], root_block_header_list[10])

            # Test Case 4 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_height(
                        height=2, skip=2, limit=4, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 1)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[2])
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=root_block_header_list[2].get_hash(),
                        skip=2,
                        limit=4,
                        direction=Direction.GENESIS,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 1)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[2])

            # Test Case 5 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_height(
                        height=11, skip=2, limit=4, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 0)

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=bytes(32), skip=2, limit=4, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 0)

            # Test Case 6 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_height(
                        height=8, skip=1, limit=5, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[8])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[6])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[4])
            self.assertEqual(resp.block_header_list[3], root_block_header_list[2])
            self.assertEqual(resp.block_header_list[4], root_block_header_list[0])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetRootBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=root_block_header_list[8].get_hash(),
                        skip=1,
                        limit=5,
                        direction=Direction.GENESIS,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], root_block_header_list[8])
            self.assertEqual(resp.block_header_list[1], root_block_header_list[6])
            self.assertEqual(resp.block_header_list[2], root_block_header_list[4])
            self.assertEqual(resp.block_header_list[3], root_block_header_list[2])
            self.assertEqual(resp.block_header_list[4], root_block_header_list[0])

    def test_get_root_block_header_sync_from_genesis(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, connect=False) as clusters:
            master = clusters[0].master
            root_block_header_list = [master.root_state.tip]
            for i in range(10):
                root_block = call_async(
                    master.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master.add_root_block(root_block))
                root_block_header_list.append(root_block.header)

            # Connect and the synchronizer should automically download
            call_async(clusters[1].network.connect(
                "127.0.0.1",
                clusters[0].network.env.cluster_config.P2P_PORT)
            )
            assert_true_with_timeout(lambda: clusters[1].master.root_state.tip == root_block_header_list[-1])
            self.assertEqual(clusters[1].master.synchronizer.stats.blocks_downloaded, len(root_block_header_list) - 1)

    def test_get_root_block_header_sync_from_height_3(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, connect=False) as clusters:
            master0 = clusters[0].master
            root_block_list = []
            for i in range(10):
                root_block = call_async(
                    master0.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master0.add_root_block(root_block))
                root_block_list.append(root_block)

            # Add 3 blocks to another cluster
            master1 = clusters[1].master
            for i in range(3):
                call_async(master1.add_root_block(root_block_list[i]))
            assert_true_with_timeout(lambda: master1.root_state.tip == root_block_list[2].header)

            # Connect and the synchronizer should automically download
            call_async(clusters[1].network.connect(
                "127.0.0.1",
                clusters[0].network.env.cluster_config.P2P_PORT)
            )
            assert_true_with_timeout(lambda: master1.root_state.tip == root_block_list[-1].header)
            self.assertEqual(master1.synchronizer.stats.blocks_downloaded, len(root_block_list) - 3)
            self.assertEqual(master1.synchronizer.stats.ancestor_lookup_requests, 1)

    def test_get_root_block_header_sync_with_fork(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, connect=False) as clusters:
            master0 = clusters[0].master
            root_block_list = []
            for i in range(10):
                root_block = call_async(
                    master0.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master0.add_root_block(root_block))
                root_block_list.append(root_block)

            # Add 2+3 blocks to another cluster: 2 are the same as cluster 0, and 3 are the fork
            master1 = clusters[1].master
            for i in range(2):
                call_async(master1.add_root_block(root_block_list[i]))
            for i in range(3):
                root_block = call_async(
                    master1.get_next_block_to_mine(
                        acc1, branch_value=None
                    )
                )
                call_async(master1.add_root_block(root_block))

            # Connect and the synchronizer should automically download
            call_async(clusters[1].network.connect(
                "127.0.0.1",
                clusters[0].network.env.cluster_config.P2P_PORT)
            )
            assert_true_with_timeout(lambda: master1.root_state.tip == root_block_list[-1].header)
            self.assertEqual(master1.synchronizer.stats.blocks_downloaded, len(root_block_list) - 2)
            self.assertEqual(master1.synchronizer.stats.ancestor_lookup_requests, 1)

    def test_get_root_block_header_sync_with_staleness(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, connect=False) as clusters:
            master0 = clusters[0].master
            root_block_list = []
            for i in range(10):
                root_block = call_async(
                    master0.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master0.add_root_block(root_block))
                root_block_list.append(root_block)
            assert_true_with_timeout(lambda: master0.root_state.tip == root_block_list[-1].header)

            # Add 3 blocks to another cluster
            master1 = clusters[1].master
            for i in range(8):
                root_block = call_async(
                    master1.get_next_block_to_mine(
                        acc1, branch_value=None
                    )
                )
                call_async(master1.add_root_block(root_block))
            master1.env.quark_chain_config.ROOT.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF = 5
            assert_true_with_timeout(lambda: master1.root_state.tip == root_block.header)

            # Connect and the synchronizer should automically download
            call_async(clusters[1].network.connect(
                "127.0.0.1",
                clusters[0].network.env.cluster_config.P2P_PORT)
            )
            assert_true_with_timeout(lambda: master1.synchronizer.stats.ancestor_not_found_count == 1)
            self.assertEqual(master1.synchronizer.stats.blocks_downloaded, 0)
            self.assertEqual(master1.synchronizer.stats.ancestor_lookup_requests, 1)

    def test_get_root_block_header_sync_with_multiple_lookup(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, connect=False) as clusters:
            master0 = clusters[0].master
            root_block_list = []
            for i in range(12):
                root_block = call_async(
                    master0.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master0.add_root_block(root_block))
                root_block_list.append(root_block)
            assert_true_with_timeout(lambda: master0.root_state.tip == root_block_list[-1].header)

            # Add 4+4 blocks to another cluster
            master1 = clusters[1].master
            for i in range(4):
                call_async(master1.add_root_block(root_block_list[i]))
            for i in range(4):
                root_block = call_async(
                    master1.get_next_block_to_mine(
                        acc1, branch_value=None
                    )
                )
                call_async(master1.add_root_block(root_block))
            master1.synchronizer.root_block_header_list_limit = 4

            # Connect and the synchronizer should automically download
            call_async(clusters[1].network.connect(
                "127.0.0.1",
                clusters[0].network.env.cluster_config.P2P_PORT)
            )
            assert_true_with_timeout(lambda: master1.root_state.tip == root_block_list[-1].header)
            self.assertEqual(master1.synchronizer.stats.blocks_downloaded, 8)
            self.assertEqual(master1.synchronizer.stats.headers_downloaded, 5 + 8)
            self.assertEqual(master1.synchronizer.stats.ancestor_lookup_requests, 2)

    def test_get_root_block_header_sync_with_start_equal_end(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, connect=False) as clusters:
            master0 = clusters[0].master
            root_block_list = []
            for i in range(5):
                root_block = call_async(
                    master0.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master0.add_root_block(root_block))
                root_block_list.append(root_block)
            assert_true_with_timeout(lambda: master0.root_state.tip == root_block_list[-1].header)

            # Add 3+1 blocks to another cluster
            master1 = clusters[1].master
            for i in range(3):
                call_async(master1.add_root_block(root_block_list[i]))
            for i in range(1):
                root_block = call_async(
                    master1.get_next_block_to_mine(
                        acc1, branch_value=None
                    )
                )
                call_async(master1.add_root_block(root_block))
            master1.synchronizer.root_block_header_list_limit = 3

            # Connect and the synchronizer should automically download
            call_async(clusters[1].network.connect(
                "127.0.0.1",
                clusters[0].network.env.cluster_config.P2P_PORT)
            )
            assert_true_with_timeout(lambda: master1.root_state.tip == root_block_list[-1].header)
            self.assertEqual(master1.synchronizer.stats.blocks_downloaded, 2)
            self.assertEqual(master1.synchronizer.stats.headers_downloaded, 6)
            self.assertEqual(master1.synchronizer.stats.ancestor_lookup_requests, 2)

    def test_get_root_block_header_sync_with_best_ancestor(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, connect=False) as clusters:
            master0 = clusters[0].master
            root_block_list = []
            for i in range(5):
                root_block = call_async(
                    master0.get_next_block_to_mine(
                        Address.create_empty_account(), branch_value=None
                    )
                )
                call_async(master0.add_root_block(root_block))
                root_block_list.append(root_block)
            assert_true_with_timeout(lambda: master0.root_state.tip == root_block_list[-1].header)

            # Add 2+2 blocks to another cluster
            master1 = clusters[1].master
            for i in range(2):
                call_async(master1.add_root_block(root_block_list[i]))
            for i in range(2):
                root_block = call_async(
                    master1.get_next_block_to_mine(
                        acc1, branch_value=None
                    )
                )
                call_async(master1.add_root_block(root_block))
            master1.synchronizer.root_block_header_list_limit = 3

            # Lookup will be [0, 2, 4], and then [3], where 3 cannot be found and thus 2 is the best.

            # Connect and the synchronizer should automically download
            call_async(clusters[1].network.connect(
                "127.0.0.1",
                clusters[0].network.env.cluster_config.P2P_PORT)
            )
            assert_true_with_timeout(lambda: master1.root_state.tip == root_block_list[-1].header)
            self.assertEqual(master1.synchronizer.stats.blocks_downloaded, 3)
            self.assertEqual(master1.synchronizer.stats.headers_downloaded, 4 + 3)
            self.assertEqual(master1.synchronizer.stats.ancestor_lookup_requests, 2)

    def test_get_minor_block_headers_with_skip(self):
        """ Test the broadcast is only done to the neighbors """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1) as clusters:
            master = clusters[0].master
            shard = next(iter(clusters[0].slave_list[0].shards.values()))

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            minor_block_header_list = [shard.state.header_tip]
            branch = shard.state.header_tip.branch
            for i in range(10):
                b = shard.state.create_block_to_mine()
                call_async(master.add_raw_minor_block(b.header.branch, b.serialize()))
                minor_block_header_list.append(b.header)

            self.assertEqual(minor_block_header_list[-1].height, 10)

            peer = next(iter(clusters[1].slave_list[0].shards[branch].peers.values()))

            # Test Case 1 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_height(
                        height=1, branch=branch, skip=1, limit=3, direction=Direction.TIP
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[1])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[3])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[5])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=minor_block_header_list[1].get_hash(),
                        branch=branch,
                        skip=1,
                        limit=3,
                        direction=Direction.TIP,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[1])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[3])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[5])

            # Test Case 2 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_height(
                        height=2, branch=branch, skip=2, limit=4, direction=Direction.TIP
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[2])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[5])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[8])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=minor_block_header_list[2].get_hash(),
                        branch=branch,
                        skip=2,
                        limit=4,
                        direction=Direction.TIP,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 3)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[2])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[5])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[8])

            # Test Case 3 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_height(
                        height=6, branch=branch, skip=0, limit=100, direction=Direction.TIP
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[6])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[7])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[8])
            self.assertEqual(resp.block_header_list[3], minor_block_header_list[9])
            self.assertEqual(resp.block_header_list[4], minor_block_header_list[10])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=minor_block_header_list[6].get_hash(),
                        branch=branch,
                        skip=0,
                        limit=100,
                        direction=Direction.TIP,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[6])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[7])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[8])
            self.assertEqual(resp.block_header_list[3], minor_block_header_list[9])
            self.assertEqual(resp.block_header_list[4], minor_block_header_list[10])

            # Test Case 4 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_height(
                        height=2, branch=branch, skip=2, limit=4, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 1)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[2])
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=minor_block_header_list[2].get_hash(),
                        branch=branch,
                        skip=2,
                        limit=4,
                        direction=Direction.GENESIS,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 1)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[2])

            # Test Case 5 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_height(
                        height=11, branch=branch, skip=2, limit=4, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 0)

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=bytes(32), branch=branch, skip=2, limit=4, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 0)

            # Test Case 6 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_height(
                        height=8, branch=branch, skip=1, limit=5, direction=Direction.GENESIS
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[8])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[6])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[4])
            self.assertEqual(resp.block_header_list[3], minor_block_header_list[2])
            self.assertEqual(resp.block_header_list[4], minor_block_header_list[0])

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=minor_block_header_list[8].get_hash(),
                        branch=branch,
                        skip=1,
                        limit=5,
                        direction=Direction.GENESIS,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 5)
            self.assertEqual(resp.block_header_list[0], minor_block_header_list[8])
            self.assertEqual(resp.block_header_list[1], minor_block_header_list[6])
            self.assertEqual(resp.block_header_list[2], minor_block_header_list[4])
            self.assertEqual(resp.block_header_list[3], minor_block_header_list[2])
            self.assertEqual(resp.block_header_list[4], minor_block_header_list[0])
