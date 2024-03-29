import unittest

from eth_keys.datatypes import PrivateKey

from quarkchain.cluster.p2p_commands import (
    CommandOp,
    GetRootBlockHeaderListWithSkipRequest,
    GetMinorBlockHeaderListWithSkipRequest,
    Direction,
)
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    create_contract_with_storage2_transaction,
    mock_pay_native_token_as_gas,
    ClusterContext,
)
from quarkchain.config import ConsensusType
from quarkchain.core import (
    Address,
    Branch,
    Identity,
    TokenBalanceMap,
    XshardTxCursorInfo,
    RootBlock,
)
from quarkchain.evm import opcodes
from quarkchain.utils import (
    call_async,
    assert_true_with_timeout,
    sha3_256,
    token_id_encode,
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

        with ClusterContext(2, acc1, should_set_gas_price_limit=True) as clusters:
            master = clusters[0].master

            root = call_async(master.get_next_block_to_mine(acc1, branch_value=None))
            call_async(master.add_root_block(root))

            # tx with gas price price lower than required (10 wei) should be rejected
            tx0 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=0,
                gas_price=9,
            )
            self.assertFalse(call_async(master.add_transaction(tx0)))

            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
                gas_price=10,
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
                gas_price=10,
            )
            self.assertTrue(call_async(master.add_transaction(tx2)))
            self.assertEqual(len(clusters[0].get_shard_state(0b11).tx_queue), 1)

            # check the tx is received by the other cluster
            state0 = clusters[1].get_shard_state(0b10)
            tx_queue, expect_evm_tx1 = state0.tx_queue, tx1.tx.to_evm_tx()
            assert_true_with_timeout(lambda: len(tx_queue) == 1)
            actual_evm_tx = tx_queue.pop_transaction(
                state0.get_transaction_count
            ).tx.to_evm_tx()
            self.assertEqual(actual_evm_tx, expect_evm_tx1)

            state1 = clusters[1].get_shard_state(0b11)
            tx_queue, expect_evm_tx2 = state1.tx_queue, tx2.tx.to_evm_tx()
            assert_true_with_timeout(lambda: len(tx_queue) == 1)
            actual_evm_tx = tx_queue.pop_transaction(
                state1.get_transaction_count
            ).tx.to_evm_tx()
            self.assertEqual(actual_evm_tx, expect_evm_tx2)

    def test_add_transaction_with_invalid_mnt(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)

        with ClusterContext(2, acc1, should_set_gas_price_limit=True) as clusters:
            master = clusters[0].master

            root = call_async(master.get_next_block_to_mine(acc1, branch_value=None))
            call_async(master.add_root_block(root))

            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
                gas_price=10,
                gas_token_id=1,
            )
            self.assertFalse(call_async(master.add_transaction(tx1)))

            tx2 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b11),
                key=id1.get_key(),
                from_address=acc2,
                to_address=acc1,
                value=12345,
                gas=30000,
                gas_price=10,
                gas_token_id=1,
            )
            self.assertFalse(call_async(master.add_transaction(tx2)))

    @mock_pay_native_token_as_gas(lambda *x: (50, x[-1] // 5))
    def test_add_transaction_with_valid_mnt(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(2, acc1, should_set_gas_price_limit=True) as clusters:
            master = clusters[0].master

            root = call_async(master.get_next_block_to_mine(acc1, branch_value=None))
            call_async(master.add_root_block(root))

            # gasprice will be 9, which is smaller than 10 as required.
            tx0 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
                gas_price=49,
                gas_token_id=1,
            )
            self.assertFalse(call_async(master.add_transaction(tx0)))

            # gasprice will be 10, but the balance will be insufficient.
            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
                gas_price=50,
                gas_token_id=1,
            )
            self.assertFalse(call_async(master.add_transaction(tx1)))

            tx2 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0b10),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
                gas_price=50,
                gas_token_id=1,
                nonce=5,
            )
            self.assertTrue(call_async(master.add_transaction(tx2)))

            # check the tx is received by the other cluster
            state1 = clusters[1].get_shard_state(0b10)
            tx_queue, expect_evm_tx2 = state1.tx_queue, tx2.tx.to_evm_tx()
            assert_true_with_timeout(lambda: len(tx_queue) == 1)
            actual_evm_tx = tx_queue.peek()[0].tx.tx.to_evm_tx()
            self.assertEqual(actual_evm_tx, expect_evm_tx2)

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

            # expect cluster 1 has all the blocks from cluster 0 and
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

    def test_broadcast_cross_shard_transactions_with_extra_gas(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=1)
        acc4 = Address.create_random_account(full_shard_key=1)

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
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST + 12345,
                gas_price=1,
            )
            self.assertTrue(slaves[0].add_tx(tx1))

            b1 = clusters[0].get_shard_state(2 | 0).create_block_to_mine(address=acc2)
            call_async(clusters[0].get_shard(2 | 0).add_block(b1))

            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc1)
                ).token_balances.balance_map,
                {
                    genesis_token: 1000000
                    - 54321
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST + 12345)
                },
            )

            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc1.address_in_shard(1))
                ).token_balances.balance_map,
                {genesis_token: 1000000},
            )

            # b2 should include the withdraw of tx1
            b2 = clusters[0].get_shard_state(2 | 1).create_block_to_mine(address=acc4)
            call_async(clusters[0].get_shard(2 | 1).add_block(b2))

            self.assert_balance(
                master, [acc3, acc1.address_in_shard(1)], [54321, 1012345]
            )

    def test_broadcast_cross_shard_transactions_with_extra_gas_old(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=1)
        acc4 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(1, acc1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list
            genesis_token = (
                clusters[0].get_shard_state(2 | 0).env.quark_chain_config.genesis_token
            )

            # Disable EVM (including remote call)
            clusters[0].get_shard(2 | 0).env.quark_chain_config.ENABLE_EVM_TIMESTAMP = (
                2 ** 64
            )
            clusters[0].get_shard(2 | 1).env.quark_chain_config.ENABLE_EVM_TIMESTAMP = (
                2 ** 64
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
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST + 12345,
                gas_price=1,
            )
            self.assertTrue(slaves[0].add_tx(tx1))

            b1 = clusters[0].get_shard_state(2 | 0).create_block_to_mine(address=acc2)
            call_async(clusters[0].get_shard(2 | 0).add_block(b1))

            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc1)
                ).token_balances.balance_map,
                {
                    genesis_token: 1000000
                    - 54321
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST)
                },
            )

            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc1.address_in_shard(1))
                ).token_balances.balance_map,
                {genesis_token: 1000000},
            )

            # b2 should include the withdraw of tx1
            b2 = clusters[0].get_shard_state(2 | 1).create_block_to_mine(address=acc4)
            call_async(clusters[0].get_shard(2 | 1).add_block(b2))

            self.assert_balance(
                master, [acc3, acc1.address_in_shard(1)], [54321, 1000000]
            )

    def test_broadcast_cross_shard_transactions_1x2(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=2 << 16)
        acc4 = Address.create_random_account(full_shard_key=3 << 16)

        with ClusterContext(1, acc1, chain_size=8, shard_size=1) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list
            genesis_token = (
                clusters[0].get_shard_state(1).env.quark_chain_config.genesis_token
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
                shard_state=clusters[0].get_shard_state(1),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc3,
                value=54321,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].add_tx(tx1))
            tx2 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc4,
                value=1234,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
                nonce=tx1.tx.to_evm_tx().nonce + 1,
            )
            self.assertTrue(slaves[0].add_tx(tx2))

            b1 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            b2 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            b2.header.create_time += 1

            call_async(clusters[0].get_shard(1).add_block(b1))

            # expect chain 2 got the CrossShardTransactionList of b1
            xshard_tx_list = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .db.get_minor_block_xshard_tx_list(b1.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 1)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx1.get_hash())
            self.assertEqual(xshard_tx_list.tx_list[0].from_address, acc1)
            self.assertEqual(xshard_tx_list.tx_list[0].to_address, acc3)
            self.assertEqual(xshard_tx_list.tx_list[0].value, 54321)

            # expect chain 3 got the CrossShardTransactionList of b1
            xshard_tx_list = (
                clusters[0]
                .get_shard_state((3 << 16) | 1)
                .db.get_minor_block_xshard_tx_list(b1.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 1)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx2.get_hash())
            self.assertEqual(xshard_tx_list.tx_list[0].from_address, acc1)
            self.assertEqual(xshard_tx_list.tx_list[0].to_address, acc4)
            self.assertEqual(xshard_tx_list.tx_list[0].value, 1234)

            call_async(clusters[0].get_shard(1 | 0).add_block(b2))
            # b2 doesn't update tip
            self.assertEqual(clusters[0].get_shard_state(1 | 0).header_tip, b1.header)

            # expect chain 2 got the CrossShardTransactionList of b1
            xshard_tx_list = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .db.get_minor_block_xshard_tx_list(b2.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 1)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx1.get_hash())
            self.assertEqual(xshard_tx_list.tx_list[0].from_address, acc1)
            self.assertEqual(xshard_tx_list.tx_list[0].to_address, acc3)
            self.assertEqual(xshard_tx_list.tx_list[0].value, 54321)

            # expect chain 3 got the CrossShardTransactionList of b1
            xshard_tx_list = (
                clusters[0]
                .get_shard_state((3 << 16) | 1)
                .db.get_minor_block_xshard_tx_list(b2.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 1)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx2.get_hash())
            self.assertEqual(xshard_tx_list.tx_list[0].from_address, acc1)
            self.assertEqual(xshard_tx_list.tx_list[0].to_address, acc4)
            self.assertEqual(xshard_tx_list.tx_list[0].value, 1234)

            b3 = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .create_block_to_mine(address=acc1.address_in_shard(2 << 16))
            )
            call_async(master.add_raw_minor_block(b3.header.branch, b3.serialize()))

            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            # b4 should include the withdraw of tx1
            b4 = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .create_block_to_mine(address=acc1.address_in_shard(2 << 16))
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

            # b5 should include the withdraw of tx2
            b5 = (
                clusters[0]
                .get_shard_state((3 << 16) | 1)
                .create_block_to_mine(address=acc1.address_in_shard(3 << 16))
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b5.header.branch, b5.serialize()))
            )
            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc4)
                ).token_balances.balance_map,
                {genesis_token: 1234},
            )

    def assert_balance(self, master, account_list, balance_list):
        genesis_token = master.env.quark_chain_config.genesis_token
        for idx, account in enumerate(account_list):
            self.assertEqual(
                call_async(
                    master.get_primary_account_data(account)
                ).token_balances.balance_map,
                {genesis_token: balance_list[idx]},
            )

    def test_broadcast_cross_shard_transactions_2x1(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=1 << 16)
        acc3 = Address.create_random_account(full_shard_key=2 << 16)
        acc4 = Address.create_random_account(full_shard_key=1 << 16)
        acc5 = Address.create_random_account(full_shard_key=0)

        with ClusterContext(
            1, acc1, chain_size=8, shard_size=1, mblock_coinbase_amount=1000000
        ) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            b0 = (
                clusters[0]
                .get_shard_state((1 << 16) + 1)
                .create_block_to_mine(address=acc2)
            )
            call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b0))

            self.assert_balance(master, [acc1, acc2], [1000000, 500000])

            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc3,
                value=54321,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
                gas_price=1,
            )
            self.assertTrue(slaves[0].add_tx(tx1))
            tx2 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc3,
                value=5555,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
                nonce=tx1.tx.to_evm_tx().nonce + 1,
                gas_price=3,
            )
            self.assertTrue(slaves[0].add_tx(tx2))

            tx3 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id2.get_key(),
                from_address=acc2,
                to_address=acc3,
                value=7787,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
                gas_price=2,
            )
            self.assertTrue(slaves[1].add_tx(tx3))

            b1 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc5)
            b2 = (
                clusters[0]
                .get_shard_state((1 << 16) + 1)
                .create_block_to_mine(address=acc4)
            )

            call_async(clusters[0].get_shard(1).add_block(b1))
            call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b2))

            self.assert_balance(
                master,
                [acc1, acc1.address_in_shard(2 << 16), acc2, acc4, acc5],
                [
                    1000000
                    - 54321
                    - 5555
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 4,
                    1000000,
                    500000 - 7787 - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 2,
                    500000 + opcodes.GTXCOST,
                    500000 + opcodes.GTXCOST * 2,
                ],
            )
            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc3)
                ).token_balances.balance_map,
                {},
            )

            # expect chain 2 got the CrossShardTransactionList of b1
            xshard_tx_list = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .db.get_minor_block_xshard_tx_list(b1.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 2)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx1.get_hash())
            self.assertEqual(xshard_tx_list.tx_list[1].tx_hash, tx2.get_hash())

            xshard_tx_list = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .db.get_minor_block_xshard_tx_list(b2.header.get_hash())
            )
            self.assertEqual(len(xshard_tx_list.tx_list), 1)
            self.assertEqual(xshard_tx_list.tx_list[0].tx_hash, tx3.get_hash())

            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            # b3 should include the deposits of tx1, t2, t3
            b3 = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .create_block_to_mine(address=acc1.address_in_shard(2 << 16))
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b3.header.branch, b3.serialize()))
            )
            self.assert_balance(
                master,
                [acc1, acc1.address_in_shard(2 << 16), acc2, acc3, acc4, acc5],
                [
                    1000000  # minior block reward
                    - 54321
                    - 5555
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 4,
                    1500000 + opcodes.GTXXSHARDCOST * 3,
                    500000 - 7787 - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 2,
                    54321 + 5555 + 7787,
                    500000 + opcodes.GTXCOST,
                    500000 + opcodes.GTXCOST * 2,
                ],
            )

            b4 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            self.assertTrue(
                call_async(master.add_raw_minor_block(b4.header.branch, b4.serialize()))
            )
            self.assert_balance(
                master,
                [acc1, acc1.address_in_shard(2 << 16), acc2, acc3, acc4, acc5],
                [
                    120 * 10 ** 18  # root block coinbase reward
                    + 1500000  # root block tax reward (3 blocks) from minor block tax
                    + 1500000  # minior block reward
                    - 54321
                    - 5555
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 4
                    + opcodes.GTXCOST * 3,  # root block tax reward from tx fee,
                    1500000 + opcodes.GTXXSHARDCOST * 3,
                    500000 - 7787 - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 2,
                    54321 + 5555 + 7787,
                    500000 + opcodes.GTXCOST,
                    500000 + opcodes.GTXCOST * 2,
                ],
            )

            root_block = call_async(
                master.get_next_block_to_mine(address=acc3, branch_value=None)
            )
            call_async(master.add_root_block(root_block))
            self.assert_balance(
                master,
                [acc1, acc1.address_in_shard(2 << 16), acc2, acc3, acc4, acc5],
                [
                    120 * 10 ** 18  # root block coinbase reward
                    + 1500000  # root block tax reward (3 blocks) from minor blocks
                    + 1500000  # minior block reward
                    - 54321
                    - 5555
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 4
                    + opcodes.GTXCOST * 3,  # root block tax reward from tx fee,
                    1500000 + opcodes.GTXXSHARDCOST * 3,
                    500000 - 7787 - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 2,
                    54321 + 5555 + 7787,
                    500000 + opcodes.GTXCOST,
                    500000 + opcodes.GTXCOST * 2,
                ],
            )

            b5 = (
                clusters[0]
                .get_shard_state((2 << 16) | 1)
                .create_block_to_mine(address=acc3)
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b5.header.branch, b5.serialize()))
            )
            self.assert_balance(
                master,
                [acc1, acc1.address_in_shard(2 << 16), acc2, acc3, acc4, acc5],
                [
                    120 * 10 ** 18  # root block coinbase reward
                    + 1500000  # root block tax reward (3 blocks) from minor blocks
                    + 1500000  # minior block reward
                    - 54321
                    - 5555
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 4
                    + opcodes.GTXCOST * 3,  # root block tax reward from tx fee,
                    1500000 + opcodes.GTXXSHARDCOST * 3,
                    500000 - 7787 - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 2,
                    120 * 10 ** 18  # root block coinbase reward
                    + 1000000  # root block tax reward (1 block) from minor blocks
                    + 500000
                    + 54321
                    + 5555
                    + 7787
                    + opcodes.GTXXSHARDCOST * 3,  # root block tax reward from tx fee
                    500000 + opcodes.GTXCOST,
                    500000 + opcodes.GTXCOST * 2,
                ],
            )

            root_block = call_async(
                master.get_next_block_to_mine(address=acc4, branch_value=None)
            )
            call_async(master.add_root_block(root_block))
            self.assert_balance(
                master,
                [acc1, acc1.address_in_shard(2 << 16), acc2, acc3, acc4, acc5],
                [
                    120 * 10 ** 18  # root block coinbase reward
                    + 1500000  # root block tax reward (3 blocks) from minor blocks
                    + 1500000  # minior block reward
                    - 54321
                    - 5555
                    - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 4
                    + opcodes.GTXCOST * 3,  # root block tax reward from tx fee,
                    1500000 + opcodes.GTXXSHARDCOST * 3,
                    500000 - 7787 - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 2,
                    120 * 10 ** 18  # root block coinbase reward
                    + 1000000  # root block tax reward (1 block) from minor blocks
                    + 500000
                    + 54321
                    + 5555
                    + 7787
                    + opcodes.GTXXSHARDCOST * 3,  # root block tax reward from tx fee
                    500000 + opcodes.GTXCOST,
                    500000 + opcodes.GTXCOST * 2,
                ],
            )

            b6 = (
                clusters[0]
                .get_shard_state((1 << 16) | 1)
                .create_block_to_mine(address=acc4)
            )
            self.assertTrue(
                call_async(master.add_raw_minor_block(b6.header.branch, b6.serialize()))
            )
            balances = [
                120 * 10 ** 18  # root block coinbase reward
                + 1500000  # root block tax reward (3 blocks) from minor blocks
                + 1500000  # minior block reward
                - 54321
                - 5555
                - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 4
                + opcodes.GTXCOST * 3,  # root block tax reward from tx fee,
                1500000 + opcodes.GTXXSHARDCOST * 3,
                500000 - 7787 - (opcodes.GTXXSHARDCOST + opcodes.GTXCOST) * 2,
                120 * 10 ** 18  # root block coinbase reward
                + 1000000  # root block tax reward (1 block) from minor blocks
                + 500000
                + 54321
                + 5555
                + 7787
                + opcodes.GTXXSHARDCOST * 3,  # root block tax reward from tx fee
                120 * 10 ** 18 + 500000 + 1000000 + opcodes.GTXCOST,
                500000 + opcodes.GTXCOST * 2,
            ]
            self.assert_balance(
                master,
                [acc1, acc1.address_in_shard(2 << 16), acc2, acc3, acc4, acc5],
                balances,
            )
            self.assertEqual(
                sum(balances),
                3 * 120 * 10 ** 18  # root block coinbase
                + 6 * 1000000  # mblock block coinbase
                + 2 * 1000000  # genesis
                + 500000,  # post-tax mblock coinbase
            )

    def test_cross_shard_contract_call(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1 << 16)
        acc3 = Address.create_from_identity(id2, full_shard_key=0)
        acc4 = Address.create_from_identity(id2, full_shard_key=1 << 16)

        storage_key = int(
            sha3_256(
                bytes.fromhex(acc4.recipient.hex().zfill(64) + "1".zfill(64))
            ).hex(),
            16,
        )

        with ClusterContext(
            1, acc1, chain_size=8, shard_size=1, mblock_coinbase_amount=10000000
        ) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list
            genesis_token = (
                clusters[0].get_shard_state(1).env.quark_chain_config.genesis_token
            )

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            tx0 = create_contract_with_storage2_transaction(
                shard_state=clusters[0].get_shard_state((1 << 16) | 1),
                key=id1.get_key(),
                from_address=acc2,
                to_full_shard_key=acc2.full_shard_key,
            )
            self.assertTrue(slaves[1].add_tx(tx0))
            b0 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            call_async(clusters[0].get_shard(1).add_block(b0))
            b1 = (
                clusters[0]
                .get_shard_state((1 << 16) + 1)
                .create_block_to_mine(address=acc2)
            )
            call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b1))

            tx1 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc3,
                value=1500000,
                gas=opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].add_tx(tx1))

            b00 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            call_async(clusters[0].get_shard(1).add_block(b00))
            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc3)
                ).token_balances.balance_map,
                {genesis_token: 1500000},
            )

            _, _, receipt = call_async(
                master.get_transaction_receipt(tx0.get_hash(), b1.header.branch)
            )
            self.assertEqual(receipt.success, b"\x01")
            contract_address = receipt.contract_address
            result = call_async(
                master.get_storage_at(contract_address, storage_key, b1.header.height)
            )
            self.assertEqual(
                result,
                bytes.fromhex(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
            )

            # should include b1
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            # call the contract with insufficient gas
            tx2 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id2.get_key(),
                from_address=acc3,
                to_address=contract_address,
                value=0,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST + 500,
                gas_price=1,
                data=bytes.fromhex("c2e171d7"),
            )
            self.assertTrue(slaves[0].add_tx(tx2))
            b2 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            call_async(clusters[0].get_shard(1).add_block(b2))

            # should include b2
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))
            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc4)
                ).token_balances.balance_map,
                {},
            )

            # The contract should be called
            b3 = (
                clusters[0]
                .get_shard_state((1 << 16) + 1)
                .create_block_to_mine(address=acc2)
            )
            call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b3))
            result = call_async(
                master.get_storage_at(contract_address, storage_key, b3.header.height)
            )
            self.assertEqual(
                result,
                bytes.fromhex(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
            )
            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc4)
                ).token_balances.balance_map,
                {},
            )
            _, _, receipt = call_async(
                master.get_transaction_receipt(tx2.get_hash(), b3.header.branch)
            )
            self.assertEqual(receipt.success, b"")

            # call the contract with enough gas
            tx3 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id2.get_key(),
                from_address=acc3,
                to_address=contract_address,
                value=0,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST + 700000,
                gas_price=1,
                data=bytes.fromhex("c2e171d7"),
            )
            self.assertTrue(slaves[0].add_tx(tx3))

            b4 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            call_async(clusters[0].get_shard(1).add_block(b4))

            # should include b4
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            # The contract should be called
            b5 = (
                clusters[0]
                .get_shard_state((1 << 16) + 1)
                .create_block_to_mine(address=acc2)
            )
            call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b5))
            result = call_async(
                master.get_storage_at(contract_address, storage_key, b5.header.height)
            )
            self.assertEqual(
                result,
                bytes.fromhex(
                    "000000000000000000000000000000000000000000000000000000000000162e"
                ),
            )
            self.assertEqual(
                call_async(
                    master.get_primary_account_data(acc4)
                ).token_balances.balance_map,
                {genesis_token: 677758},
            )
            _, _, receipt = call_async(
                master.get_transaction_receipt(tx3.get_hash(), b3.header.branch)
            )
            self.assertEqual(receipt.success, b"\x01")

    def test_cross_shard_contract_create(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1 << 16)

        storage_key = int(
            sha3_256(
                bytes.fromhex(acc2.recipient.hex().zfill(64) + "1".zfill(64))
            ).hex(),
            16,
        )

        with ClusterContext(
            1, acc1, chain_size=8, shard_size=1, mblock_coinbase_amount=1000000
        ) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            tx1 = create_contract_with_storage2_transaction(
                shard_state=clusters[0].get_shard_state((1 << 16) | 1),
                key=id1.get_key(),
                from_address=acc2,
                to_full_shard_key=acc1.full_shard_key,
            )
            self.assertTrue(slaves[1].add_tx(tx1))

            b1 = (
                clusters[0]
                .get_shard_state((1 << 16) + 1)
                .create_block_to_mine(address=acc2)
            )
            call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b1))

            _, _, receipt = call_async(
                master.get_transaction_receipt(tx1.get_hash(), b1.header.branch)
            )
            self.assertEqual(receipt.success, b"\x01")

            # should include b1
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            b2 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            call_async(clusters[0].get_shard(1).add_block(b2))

            # contract should be created
            _, _, receipt = call_async(
                master.get_transaction_receipt(tx1.get_hash(), b2.header.branch)
            )
            self.assertEqual(receipt.success, b"\x01")
            contract_address = receipt.contract_address
            result = call_async(
                master.get_storage_at(contract_address, storage_key, b2.header.height)
            )
            self.assertEqual(
                result,
                bytes.fromhex(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
            )

            # call the contract with enough gas
            tx2 = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1),
                key=id1.get_key(),
                from_address=acc1,
                to_address=contract_address,
                value=0,
                gas=opcodes.GTXCOST + 700000,
                gas_price=1,
                data=bytes.fromhex("c2e171d7"),
            )
            self.assertTrue(slaves[0].add_tx(tx2))

            b3 = clusters[0].get_shard_state(1).create_block_to_mine(address=acc1)
            call_async(clusters[0].get_shard(1).add_block(b3))

            _, _, receipt = call_async(
                master.get_transaction_receipt(tx2.get_hash(), b3.header.branch)
            )
            self.assertEqual(receipt.success, b"\x01")
            result = call_async(
                master.get_storage_at(contract_address, storage_key, b3.header.height)
            )
            self.assertEqual(
                result,
                bytes.fromhex(
                    "000000000000000000000000000000000000000000000000000000000000162e"
                ),
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
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1", clusters[0].network.env.cluster_config.P2P_PORT
                )
            )
            assert_true_with_timeout(
                lambda: clusters[1].master.root_state.tip == root_block_header_list[-1]
            )
            self.assertEqual(
                clusters[1].master.synchronizer.stats.blocks_downloaded,
                len(root_block_header_list) - 1,
            )

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
            assert_true_with_timeout(
                lambda: master1.root_state.tip == root_block_list[2].header
            )

            # Connect and the synchronizer should automically download
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1", clusters[0].network.env.cluster_config.P2P_PORT
                )
            )
            assert_true_with_timeout(
                lambda: master1.root_state.tip == root_block_list[-1].header
            )
            self.assertEqual(
                master1.synchronizer.stats.blocks_downloaded, len(root_block_list) - 3
            )
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
                    master1.get_next_block_to_mine(acc1, branch_value=None)
                )
                call_async(master1.add_root_block(root_block))

            # Connect and the synchronizer should automically download
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1", clusters[0].network.env.cluster_config.P2P_PORT
                )
            )
            assert_true_with_timeout(
                lambda: master1.root_state.tip == root_block_list[-1].header
            )
            self.assertEqual(
                master1.synchronizer.stats.blocks_downloaded, len(root_block_list) - 2
            )
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
            assert_true_with_timeout(
                lambda: master0.root_state.tip == root_block_list[-1].header
            )

            # Add 3 blocks to another cluster
            master1 = clusters[1].master
            for i in range(8):
                root_block = call_async(
                    master1.get_next_block_to_mine(acc1, branch_value=None)
                )
                call_async(master1.add_root_block(root_block))
            master1.env.quark_chain_config.ROOT.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF = 5
            assert_true_with_timeout(
                lambda: master1.root_state.tip == root_block.header
            )

            # Connect and the synchronizer should automically download
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1", clusters[0].network.env.cluster_config.P2P_PORT
                )
            )
            assert_true_with_timeout(
                lambda: master1.synchronizer.stats.ancestor_not_found_count == 1
            )
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
            assert_true_with_timeout(
                lambda: master0.root_state.tip == root_block_list[-1].header
            )

            # Add 4+4 blocks to another cluster
            master1 = clusters[1].master
            for i in range(4):
                call_async(master1.add_root_block(root_block_list[i]))
            for i in range(4):
                root_block = call_async(
                    master1.get_next_block_to_mine(acc1, branch_value=None)
                )
                call_async(master1.add_root_block(root_block))
            master1.synchronizer.root_block_header_list_limit = 4

            # Connect and the synchronizer should automically download
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1", clusters[0].network.env.cluster_config.P2P_PORT
                )
            )
            assert_true_with_timeout(
                lambda: master1.root_state.tip == root_block_list[-1].header
            )
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
            assert_true_with_timeout(
                lambda: master0.root_state.tip == root_block_list[-1].header
            )

            # Add 3+1 blocks to another cluster
            master1 = clusters[1].master
            for i in range(3):
                call_async(master1.add_root_block(root_block_list[i]))
            for i in range(1):
                root_block = call_async(
                    master1.get_next_block_to_mine(acc1, branch_value=None)
                )
                call_async(master1.add_root_block(root_block))
            master1.synchronizer.root_block_header_list_limit = 3

            # Connect and the synchronizer should automically download
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1", clusters[0].network.env.cluster_config.P2P_PORT
                )
            )
            assert_true_with_timeout(
                lambda: master1.root_state.tip == root_block_list[-1].header
            )
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
            assert_true_with_timeout(
                lambda: master0.root_state.tip == root_block_list[-1].header
            )

            # Add 2+2 blocks to another cluster
            master1 = clusters[1].master
            for i in range(2):
                call_async(master1.add_root_block(root_block_list[i]))
            for i in range(2):
                root_block = call_async(
                    master1.get_next_block_to_mine(acc1, branch_value=None)
                )
                call_async(master1.add_root_block(root_block))
            master1.synchronizer.root_block_header_list_limit = 3

            # Lookup will be [0, 2, 4], and then [3], where 3 cannot be found and thus 2 is the best.

            # Connect and the synchronizer should automically download
            call_async(
                clusters[1].network.connect(
                    "127.0.0.1", clusters[0].network.env.cluster_config.P2P_PORT
                )
            )
            assert_true_with_timeout(
                lambda: master1.root_state.tip == root_block_list[-1].header
            )
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
                        height=1,
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
                        height=2,
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
                        height=6,
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
                        height=2,
                        branch=branch,
                        skip=2,
                        limit=4,
                        direction=Direction.GENESIS,
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
                        height=11,
                        branch=branch,
                        skip=2,
                        limit=4,
                        direction=Direction.GENESIS,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 0)

            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_hash(
                        hash=bytes(32),
                        branch=branch,
                        skip=2,
                        limit=4,
                        direction=Direction.GENESIS,
                    ),
                )
            )
            self.assertEqual(len(resp.block_header_list), 0)

            # Test Case 6 ###################################################
            op, resp, rpc_id = call_async(
                peer.write_rpc_request(
                    op=CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
                    cmd=GetMinorBlockHeaderListWithSkipRequest.create_for_height(
                        height=8,
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

    def test_posw_on_root_chain(self):
        """ Test the broadcast is only done to the neighbors """
        staker_id = Identity.create_random_identity()
        staker_addr = Address.create_from_identity(staker_id, full_shard_key=0)
        signer_id = Identity.create_random_identity()
        signer_addr = Address.create_from_identity(signer_id, full_shard_key=0)

        def add_root_block(addr, sign=False):
            root_block = call_async(
                master.get_next_block_to_mine(addr, branch_value=None)
            )  # type: RootBlock
            if sign:
                root_block.header.sign_with_private_key(PrivateKey(signer_id.get_key()))
            call_async(master.add_root_block(root_block))

        with ClusterContext(1, staker_addr, shard_size=1) as clusters:
            master = clusters[0].master

            # add a root block first to init shard chains
            add_root_block(Address.create_empty_account())

            qkc_config = master.env.quark_chain_config
            qkc_config.ROOT.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
            qkc_config.ROOT.POSW_CONFIG.ENABLED = True
            qkc_config.ROOT.POSW_CONFIG.ENABLE_TIMESTAMP = 0
            qkc_config.ROOT.POSW_CONFIG.WINDOW_SIZE = 2
            # should always pass pow check if posw is applied
            qkc_config.ROOT.POSW_CONFIG.DIFF_DIVIDER = 1000000
            qkc_config.ROOT.POSW_CONFIG.BOOST_TIMESTAMP = 0
            shard = next(iter(clusters[0].slave_list[0].shards.values()))

            # monkey patch staking results
            def mock_get_root_chain_stakes(recipient, _):
                if recipient == staker_addr.recipient:
                    # allow 1 block in the windows
                    return (
                        qkc_config.ROOT.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK,
                        signer_addr.recipient,
                    )
                return 0, bytes(20)

            shard.state.get_root_chain_stakes = mock_get_root_chain_stakes

            # fail, because signature mismatch
            with self.assertRaises(ValueError):
                add_root_block(staker_addr)
            # succeed
            add_root_block(staker_addr, sign=True)
            # fail again, because quota used up
            with self.assertRaises(ValueError):
                add_root_block(staker_addr, sign=True)

    def test_total_balance_handle_xshard_deposit(self):
        """ Test the cross shard transactions are broadcasted to the destination shards """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1 << 16)
        qkc_token = token_id_encode("QKC")
        init_coinbase = 1000000

        with ClusterContext(
            1,
            acc1,
            chain_size=2,
            shard_size=1,
            small_coinbase=True,
            mblock_coinbase_amount=0,
        ) as clusters:
            master = clusters[0].master
            slaves = clusters[0].slave_list
            state1 = clusters[0].get_shard_state(1)
            state2 = clusters[0].get_shard_state((1 << 16) + 1)
            state2.env.cluster_config.PROMETHEUS.MONITOR_XSHARD_DEPOSIT = True

            # add a root block first so that later minor blocks referring to this root
            # can be broadcasted to other shards
            root_block = call_async(
                master.get_next_block_to_mine(
                    Address.create_empty_account(), branch_value=None
                )
            )
            call_async(master.add_root_block(root_block))

            balance, _ = state2.get_total_balance(
                qkc_token,
                state2.header_tip.get_hash(),
                root_block.header.get_hash(),
                100,
                None,
            )
            self.assertEqual(balance, init_coinbase)  # no input

            tx = create_transfer_transaction(
                shard_state=state1,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=100,
                gas=30000,
                gas_price=0,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            b1 = state1.create_block_to_mine(address=acc1)
            call_async(clusters[0].get_shard(1).add_block(b1))
            # add two blocks to shard 1, while only make the first included by root block
            b2s = []
            for _ in range(2):
                b2 = state2.create_block_to_mine(address=acc2)
                call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b2))
                b2s.append(b2)

            # add a root block so the xshard tx can be recorded
            root_block = master.root_state.create_block_to_mine(
                [b1.header, b2s[0].header], acc1
            )
            call_async(master.add_root_block(root_block))

            # check source shard
            balance, _ = state1.get_total_balance(
                qkc_token, state1.header_tip.get_hash(), None, 100, None
            )
            # minus transfer value plus root block coinbase
            self.assertEqual(balance, init_coinbase - 100 + 5)

            # query with root block, should include xshard deposit
            balance, _ = state2.get_total_balance(
                qkc_token,
                state2.header_tip.hash_prev_minor_block,
                root_block.header.get_hash(),
                100,
                None,
            )
            self.assertEqual(balance, init_coinbase + 100)
            # query without root block hash, should exclude xshard deposit
            balance, _ = state2.get_total_balance(
                qkc_token, state2.header_tip.hash_prev_minor_block, None, 100, None
            )
            self.assertEqual(balance, init_coinbase)
            # query latest header, deposit should be executed, regardless of root block
            # once next block is available
            b2 = state2.create_block_to_mine(address=acc2)
            call_async(clusters[0].get_shard((1 << 16) + 1).add_block(b2))
            for rh in [None, root_block.header.get_hash()]:
                balance, _ = state2.get_total_balance(
                    qkc_token, state2.header_tip.get_hash(), rh, 100, None
                )
                self.assertEqual(balance, init_coinbase + 100)
