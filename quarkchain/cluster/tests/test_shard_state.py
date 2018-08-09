import unittest

from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import get_test_env, create_transfer_transaction
from quarkchain.core import CrossShardTransactionDeposit, CrossShardTransactionList
from quarkchain.core import Identity, Address
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.evm import opcodes


def create_default_shard_state(env, shardId=0):
    shardState = ShardState(
        env=env,
        shardId=shardId,
    )
    return shardState


class TestShardState(unittest.TestCase):

    def test_shard_state_simple(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        self.assertEqual(state.rootTip.height, 1)
        self.assertEqual(state.headerTip.height, 1)

    def test_execute_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=0)
        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)
        tx = create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=12345,
        )
        state.evmState.gas_used = state.evmState.gas_limit
        res = state.execute_tx(tx, acc1)
        self.assertEqual(res, b'')

    def test_add_tx_incorrect_from_shard_id(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=1)
        acc2 = Address.create_random_account(fullShardId=1)
        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)
        # state is shard 0 but tx from shard 1
        tx = create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=12345,
        )
        self.assertFalse(state.add_tx(tx))
        self.assertIsNone(state.execute_tx(tx, acc1))

    def test_one_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=12345,
            gas=50000,
        )
        state.evmState.gas_used = state.evmState.gas_limit
        self.assertTrue(state.add_tx(tx))

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block.txList[0], tx)
        self.assertEqual(block.header.createTime, 0)
        self.assertEqual(i, 0)

        # tx claims to use more gas than the limit and thus not included
        b1 = state.create_block_to_mine(address=acc3, gasLimit=49999)
        self.assertEqual(len(b1.txList), 0)

        b1 = state.create_block_to_mine(address=acc3, gasLimit=50000)
        self.assertEqual(len(b1.txList), 1)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.get_balance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345)
        self.assertEqual(state.get_balance(acc2.recipient), 12345)
        self.assertEqual(state.get_balance(acc3.recipient), opcodes.GTXCOST // 2)

        # Check receipts
        self.assertEqual(len(state.evmState.receipts), 1)
        self.assertEqual(state.evmState.receipts[0].state_root, b'\x01')
        self.assertEqual(state.evmState.receipts[0].gas_used, 21000)

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        # Check receipts in storage
        resp = state.get_transaction_receipt(tx.get_hash())
        self.assertIsNotNone(resp)
        block, i, r = resp
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)
        self.assertEqual(r.success, b'\x01')
        self.assertEqual(r.gasUsed, 21000)

        # Check Account has full_shard_id
        self.assertEqual(state.evmState.get_full_shard_id(acc2.recipient), acc2.fullShardId)

        txList, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(txList[0].value, 12345)
        txList, _ = state.db.get_transactions_by_address(acc2)
        self.assertEqual(txList[0].value, 12345)

    def test_duplicated_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=12345,
        )
        self.assertTrue(state.add_tx(tx))
        self.assertFalse(state.add_tx(tx))  # already in txQueue

        self.assertEqual(len(state.txQueue), 1)
        self.assertEqual(len(state.txDict), 1)

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(len(block.txList), 1)
        self.assertEqual(block.txList[0], tx)
        self.assertEqual(block.header.createTime, 0)
        self.assertEqual(i, 0)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.txList), 1)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.get_balance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345)
        self.assertEqual(state.get_balance(acc2.recipient), 12345)
        self.assertEqual(state.get_balance(acc3.recipient), opcodes.GTXCOST // 2)

        # Check receipts
        self.assertEqual(len(state.evmState.receipts), 1)
        self.assertEqual(state.evmState.receipts[0].state_root, b'\x01')
        self.assertEqual(state.evmState.receipts[0].gas_used, 21000)
        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        # tx already confirmed
        self.assertTrue(state.db.contain_transaction_hash(tx.get_hash()))
        self.assertFalse(state.add_tx(tx))

    def test_add_invalid_tx_fail(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=999999999999999999999,  # insane
        )
        self.assertFalse(state.add_tx(tx))
        self.assertEqual(len(state.txQueue), 0)

    def test_two_tx_in_one_block(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id2, fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=2000000 + opcodes.GTXCOST)
        state = create_default_shard_state(env=env)

        state.add_tx(create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=1000000,
        ))

        b0 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b0)
        self.assertEqual(state.get_balance(id1.recipient), 1000000)
        self.assertEqual(state.get_balance(acc2.recipient), 1000000)
        self.assertEqual(state.get_balance(acc3.recipient), opcodes.GTXCOST // 2)

        # Check Account has full_shard_id
        self.assertEqual(state.evmState.get_full_shard_id(acc2.recipient), acc2.fullShardId)

        state.add_tx(create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=Address(acc2.recipient, acc2.fullShardId + 2),  # set a different full shard id
            value=12345,
            gas=50000,
        ))
        state.add_tx(create_transfer_transaction(
            shardState=state,
            key=id2.get_key(),
            fromAddress=acc2,
            toAddress=acc1,
            value=54321,
            gas=40000,
        ))
        b1 = state.create_block_to_mine(address=acc3, gasLimit=40000)
        self.assertEqual(len(b1.txList), 1)
        b1 = state.create_block_to_mine(address=acc3, gasLimit=90000)
        self.assertEqual(len(b1.txList), 2)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.get_balance(id1.recipient), 1000000 - opcodes.GTXCOST - 12345 + 54321)
        self.assertEqual(state.get_balance(acc2.recipient), 1000000 - opcodes.GTXCOST + 12345 - 54321)
        self.assertEqual(state.get_balance(acc3.recipient), opcodes.GTXCOST * 1.5)

        # Check receipts
        self.assertEqual(len(state.evmState.receipts), 2)
        self.assertEqual(state.evmState.receipts[0].state_root, b'\x01')
        self.assertEqual(state.evmState.receipts[0].gas_used, 21000)
        self.assertEqual(state.evmState.receipts[1].state_root, b'\x01')
        self.assertEqual(state.evmState.receipts[1].gas_used, 42000)

        block, i = state.get_transaction_by_hash(b1.txList[0].get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        block, i = state.get_transaction_by_hash(b1.txList[1].get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 1)

        # Check acc2 fullShardId doesn't change
        self.assertEqual(state.evmState.get_full_shard_id(acc2.recipient), acc2.fullShardId)

    def test_fork_does_not_confirm_tx(self):
        """Tx should only be confirmed and removed from tx queue by the best chain"""
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id2, fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=2000000 + opcodes.GTXCOST)
        state = create_default_shard_state(env=env)

        state.add_tx(create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=1000000,
        ))

        b0 = state.create_block_to_mine(address=acc3)
        b1 = state.create_block_to_mine(address=acc3)
        b0.txList = []  # make b0 empty
        state.finalize_and_add_block(b0)

        self.assertEqual(len(state.txQueue), 1)

        self.assertEqual(len(b1.txList), 1)
        state.finalize_and_add_block(b1)
        # b1 is a fork and does not remove the tx from queue
        self.assertEqual(len(state.txQueue), 1)

        b2 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b2)
        self.assertEqual(len(state.txQueue), 0)

    def test_revert_fork_put_tx_back_to_queue(self):
        """Tx in the reverted chain should be put back to the queue"""
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id2, fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=2000000 + opcodes.GTXCOST)
        state = create_default_shard_state(env=env)

        state.add_tx(create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=1000000,
        ))

        b0 = state.create_block_to_mine(address=acc3)
        b1 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b0)

        self.assertEqual(len(state.txQueue), 0)

        b1.txList = []  # make b1 empty
        state.finalize_and_add_block(b1)
        self.assertEqual(len(state.txQueue), 0)

        b2 = b1.create_block_to_append()
        state.finalize_and_add_block(b2)

        # now b1-b2 becomes the best chain and we expect b0 to be reverted and put the tx back to queue
        self.assertEqual(len(state.txQueue), 1)

        b3 = b0.create_block_to_append()
        state.finalize_and_add_block(b3)
        self.assertEqual(len(state.txQueue), 1)

        b4 = b3.create_block_to_append()
        state.finalize_and_add_block(b4)

        # b0-b3-b4 becomes the best chain
        self.assertEqual(len(state.txQueue), 0)


    def test_stale_block_count(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.create_block_to_mine(address=acc3)
        b2 = state.create_block_to_mine(address=acc3)
        b2.header.createTime += 1

        state.finalize_and_add_block(b1)
        self.assertEqual(state.db.get_block_count_by_height(2), 1)

        state.finalize_and_add_block(b2)
        self.assertEqual(state.db.get_block_count_by_height(2), 2)

    def test_xshard_tx_sent(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id1, fullShardId=1)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        tx = create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        state.add_tx(tx)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.txList), 1)

        self.assertEqual(state.evmState.gas_used, 0)
        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(len(state.evmState.xshard_list), 1)
        self.assertEqual(
            state.evmState.xshard_list[0],
            CrossShardTransactionDeposit(
                txHash=tx.get_hash(),
                fromAddress=acc1,
                toAddress=acc2,
                value=888888,
                gasPrice=1))
        self.assertEqual(state.get_balance(id1.recipient), 10000000 - 888888 - opcodes.GTXCOST - opcodes.GTXXSHARDCOST)
        # Make sure the xshard gas is not used by local block
        self.assertEqual(state.evmState.gas_used, opcodes.GTXCOST + opcodes.GTXXSHARDCOST)
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(state.get_balance(acc3.recipient), opcodes.GTXCOST // 2)

    def test_xshard_tx_insufficient_gas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id1, fullShardId=1)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        state.add_tx(create_transfer_transaction(
            shardState=state,
            key=id1.get_key(),
            fromAddress=acc1,
            toAddress=acc2,
            value=888888,
            gas=opcodes.GTXCOST,
        ))

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.txList), 0)
        self.assertEqual(len(state.txQueue), 0)

    def test_xshard_tx_received(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id1, fullShardId=1)
        acc3 = Address.create_random_account(fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        tx = create_transfer_transaction(
            shardState=state1,
            key=id1.get_key(),
            fromAddress=acc2,
            toAddress=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gasPrice=2,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            txList=CrossShardTransactionList(txList=[
                CrossShardTransactionDeposit(
                    txHash=tx.get_hash(),
                    fromAddress=acc2,
                    toAddress=acc1,
                    value=888888,
                    gasPrice=2)
            ]))

        # Create a root block containing the block with the x-shard tx
        rB = state0.rootTip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()
        state0.add_root_block(rB)

        # Add b0 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(address=acc3)
        state0.finalize_and_add_block(b2)

        self.assertEqual(state0.get_balance(acc1.recipient), 10000000 + 888888)
        # Half collected by root
        self.assertEqual(state0.get_balance(acc3.recipient), opcodes.GTXXSHARDCOST * 2 // 2)

        # X-shard gas used
        evmState0 = state0.evmState
        self.assertEqual(evmState0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

    def test_xshard_for_two_root_blocks(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id1, fullShardId=1)
        acc3 = Address.create_random_account(fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        tx = create_transfer_transaction(
            shardState=state1,
            key=id1.get_key(),
            fromAddress=acc2,
            toAddress=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            txList=CrossShardTransactionList(txList=[
                CrossShardTransactionDeposit(
                    txHash=tx.get_hash(),
                    fromAddress=acc2,
                    toAddress=acc1,
                    value=888888,
                    gasPrice=2)
            ]))

        # Create a root block containing the block with the x-shard tx
        rB0 = state0.rootTip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()
        state0.add_root_block(rB0)

        b2 = state0.get_tip().create_block_to_append()
        state0.finalize_and_add_block(b2)

        b3 = b1.create_block_to_append()

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b3.header.get_hash(),
            txList=CrossShardTransactionList(txList=[
                CrossShardTransactionDeposit(
                    txHash=bytes(32),
                    fromAddress=acc2,
                    toAddress=acc1,
                    value=385723,
                    gasPrice=3)
            ]))

        rB1 = state0.rootTip.create_block_to_append() \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b3.header) \
            .finalize()
        state0.add_root_block(rB1)

        # Test x-shard gas limit when create_block_to_mine
        b5 = state0.create_block_to_mine(address=acc3, gasLimit=0)
        # Current algorithm allows at least one root block to be included
        self.assertEqual(b5.header.hashPrevRootBlock, rB0.header.get_hash())
        b6 = state0.create_block_to_mine(address=acc3, gasLimit=opcodes.GTXXSHARDCOST)
        self.assertEqual(b6.header.hashPrevRootBlock, rB0.header.get_hash())
        # There are two x-shard txs: one is root block coinbase with zero gas, and anonther is from shard 1
        b7 = state0.create_block_to_mine(address=acc3, gasLimit=2 * opcodes.GTXXSHARDCOST)
        self.assertEqual(b7.header.hashPrevRootBlock, rB1.header.get_hash())
        b8 = state0.create_block_to_mine(address=acc3, gasLimit=3 * opcodes.GTXXSHARDCOST)
        self.assertEqual(b8.header.hashPrevRootBlock, rB1.header.get_hash())

        # Add b0 and make sure all x-shard tx's are added
        b4 = state0.create_block_to_mine(address=acc3)
        self.assertEqual(b4.header.hashPrevRootBlock, rB1.header.get_hash())
        state0.finalize_and_add_block(b4)

        self.assertEqual(state0.get_balance(acc1.recipient), 10000000 + 888888 + 385723)
        # Half collected by root
        self.assertEqual(state0.get_balance(acc3.recipient), opcodes.GTXXSHARDCOST * (2 + 3) // 2)

        # Check gas used for receiving x-shard tx
        self.assertEqual(state0.evmState.gas_used, 18000)
        self.assertEqual(state0.evmState.xshard_receive_gas_used, 18000)

    def test_fork_resolve(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b0 = state.get_tip().create_block_to_append()
        b1 = state.get_tip().create_block_to_append()

        state.finalize_and_add_block(b0)
        self.assertEqual(state.headerTip, b0.header)

        # Fork happens, first come first serve
        state.finalize_and_add_block(b1)
        self.assertEqual(state.headerTip, b0.header)

        # Longer fork happens, override existing one
        b2 = b1.create_block_to_append()
        state.finalize_and_add_block(b2)
        self.assertEqual(state.headerTip, b2.header)

    def test_root_chain_first_consensus(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(address=Address.create_empty_account())

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        b1.finalize(evmState=state1.run_block(b1))

        # Create a root block containing the block with the x-shard tx
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            txList=CrossShardTransactionList(txList=[]))
        rB = state0.rootTip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()
        state0.add_root_block(rB)

        b00 = b0.create_block_to_append()
        state0.finalize_and_add_block(b00)
        self.assertEqual(state0.headerTip, b00.header)

        # Create another fork that is much longer (however not confirmed by rB)
        b3 = b2.create_block_to_append()
        state0.finalize_and_add_block(b3)
        b4 = b3.create_block_to_append()
        state0.finalize_and_add_block(b4)
        self.assertGreater(b4.header.height, b00.header.height)
        self.assertEqual(state0.headerTip, b00.header)

    def test_shard_state_add_root_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(address=Address.create_empty_account())

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        b1.finalize(evmState=state1.run_block(b1))

        # Create a root block containing the block with the x-shard tx
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            txList=CrossShardTransactionList(txList=[]))
        rB = state0.rootTip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()
        rB1 = state0.rootTip.create_block_to_append() \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        state0.add_root_block(rB)

        b00 = b0.create_block_to_append()
        state0.finalize_and_add_block(b00)
        self.assertEqual(state0.headerTip, b00.header)

        # Create another fork that is much longer (however not confirmed by rB)
        b3 = b2.create_block_to_append()
        state0.finalize_and_add_block(b3)
        b4 = b3.create_block_to_append()
        state0.finalize_and_add_block(b4)
        self.assertEqual(state0.headerTip, b00.header)
        self.assertEqual(state0.db.get_minor_block_by_height(3), b00)
        self.assertIsNone(state0.db.get_minor_block_by_height(4))

        b5 = b1.create_block_to_append()
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b5.header.get_hash(),
            txList=CrossShardTransactionList(txList=[]))
        rB2 = rB1.create_block_to_append() \
            .add_minor_block_header(b3.header) \
            .add_minor_block_header(b4.header) \
            .add_minor_block_header(b5.header) \
            .finalize()

        self.assertFalse(state0.add_root_block(rB1))
        self.assertTrue(state0.add_root_block(rB2))
        self.assertEqual(state0.headerTip, b4.header)
        self.assertEqual(state0.metaTip, b4.meta)
        self.assertEqual(state0.rootTip, rB2.header)

        self.assertEqual(state0.db.get_minor_block_by_height(3), b3)
        self.assertEqual(state0.db.get_minor_block_by_height(4), b4)

    def test_shard_state_fork_resolve_with_higher_root_chain(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b0 = state.get_tip().create_block_to_append()
        state.finalize_and_add_block(b0)
        rB = state.rootTip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .finalize()

        self.assertEqual(state.headerTip, b0.header)
        self.assertTrue(state.add_root_block(rB))

        b1 = state.get_tip().create_block_to_append()
        b2 = state.get_tip().create_block_to_append(nonce=1)
        b2.header.hashPrevRootBlock = rB.header.get_hash()
        b3 = state.get_tip().create_block_to_append(nonce=2)
        b3.header.hashPrevRootBlock = rB.header.get_hash()

        state.finalize_and_add_block(b1)
        self.assertEqual(state.headerTip, b1.header)

        # Fork happens, although they have the same height, b2 survives since it confirms root block
        state.finalize_and_add_block(b2)
        self.assertEqual(state.headerTip, b2.header)

        # b3 confirms the same root block as b2, so it will not override b2
        state.finalize_and_add_block(b3)
        self.assertEqual(state.headerTip, b2.header)

    def test_shard_state_difficulty(self):
        env = get_test_env()
        env.config.GENESIS_MINOR_DIFFICULTY = 10000
        env.config.SKIP_MINOR_DIFFICULTY_CHECK = False
        env.config.MINOR_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=9,
            diffFactor=2048,
            minimumDiff=1)
        env.config.NETWORK_ID = 1  # other network ids will skip difficulty check
        state = create_default_shard_state(env=env, shardId=0)

        # Check new difficulty
        b0 = state.create_block_to_mine(state.headerTip.createTime + 8)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty // 2048 + state.headerTip.difficulty)
        b0 = state.create_block_to_mine(state.headerTip.createTime + 9)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty)
        b0 = state.create_block_to_mine(state.headerTip.createTime + 17)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty)
        b0 = state.create_block_to_mine(state.headerTip.createTime + 24)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty - state.headerTip.difficulty // 2048)
        b0 = state.create_block_to_mine(state.headerTip.createTime + 35)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty - state.headerTip.difficulty // 2048 * 2)

        for i in range(0, 2 ** 32):
            b0.header.nonce = i
            if int.from_bytes(b0.header.get_hash(), byteorder="big") * env.config.GENESIS_MINOR_DIFFICULTY < 2 ** 256:
                self.assertEqual(state.add_block(b0), [])
                break
            else:
                with self.assertRaises(ValueError):
                    state.add_block(b0)

    def test_shard_state_recovery_from_root_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        blockHeaders = []
        blockMetas = []
        for i in range(12):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            blockHeaders.append(b.header)
            blockMetas.append(b.meta)

        # add a fork
        b1 = state.db.get_minor_block_by_height(3)
        b1.header.createTime += 1
        state.finalize_and_add_block(b1)
        self.assertEqual(state.db.get_minor_block_by_hash(b1.header.get_hash()), b1)

        rB = state.rootTip.create_block_to_append()
        rB.minorBlockHeaderList = blockHeaders[:5]
        rB.finalize()

        state.add_root_block(rB)

        recoveredState = ShardState(env=env, shardId=0)
        self.assertEqual(recoveredState.headerTip.height, 1)

        recoveredState.init_from_root_block(rB)
        # forks are pruned
        self.assertIsNone(recoveredState.db.get_minor_block_by_hash(b1.header.get_hash()))
        self.assertEqual(recoveredState.db.get_minor_block_by_hash(b1.header.get_hash(), consistencyCheck=False), b1)

        self.assertEqual(recoveredState.rootTip, rB.header)
        self.assertEqual(recoveredState.headerTip, blockHeaders[4])
        self.assertEqual(recoveredState.confirmedHeaderTip, blockHeaders[4])
        self.assertEqual(recoveredState.metaTip, blockMetas[4])
        self.assertEqual(recoveredState.confirmedMetaTip, blockMetas[4])
        self.assertEqual(recoveredState.evmState.trie.root_hash, blockMetas[4].hashEvmStateRoot)

    def test_add_block_receipt_root_not_match(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1)
        acc3 = Address.create_random_account(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.create_block_to_mine(address=acc3)

        # Should succeed
        state.finalize_and_add_block(b1)
        b1.finalize(evmState=state.run_block(b1))
        b1.meta.hashEvmReceiptRoot = b'00' * 32
        self.assertRaises(ValueError, state.add_block(b1))

    def test_not_update_tip_on_root_fork(self):
        ''' block's hashPrevRootBlock must be on the same chain with rootTip to update tip.

                 +--+
              a. |r1|
                /+--+
               /   |
        +--+  /  +--+    +--+
        |r0|<----|m1|<---|m2| c.
        +--+  \  +--+    +--+
               \   |      |
                \+--+     |
              b. |r2|<----+
                 +--+

        Initial state: r0 <- m1
        Then adding r1, r2, m2 should not make m2 the tip because r1 is the root tip and r2 and r1
        are not on the same root chain.
        '''
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        m1 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m1)

        r1 = state.rootTip.create_block_to_append()
        r2 = state.rootTip.create_block_to_append()
        r1.minorBlockHeaderList.append(m1.header)
        r1.finalize()

        state.add_root_block(r1)

        r2.minorBlockHeaderList.append(m1.header)
        r2.header.createTime = r1.header.createTime + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.get_hash(), r2.header.get_hash())

        state.add_root_block(r2)

        self.assertEqual(state.rootTip, r1.header)

        m2 = m1.create_block_to_append(address=acc1)
        m2.header.hashPrevRootBlock = r2.header.get_hash()

        state.finalize_and_add_block(m2)
        # m2 is added
        self.assertEqual(state.db.get_minor_block_by_hash(m2.header.get_hash()), m2)
        # but m1 should still be the tip
        self.assertEqual(state.headerTip, m1.header)

    def test_add_root_block_revert_header_tip(self):
        ''' block's hashPrevRootBlock must be on the same chain with rootTip to update tip.

                 +--+
                 |r1|<-------------+
                /+--+              |
               /   |               |
        +--+  /  +--+    +--+     +--+
        |r0|<----|m1|<---|m2| <---|m3|
        +--+  \  +--+    +--+     +--+
               \   |       \
                \+--+.     +--+
                 |r2|<-----|r3| (r3 includes m2)
                 +--+      +--+

        Initial state: r0 <- m1 <- m2
        Adding r1, r2, m3 makes r1 the rootTip, m3 the headerTip
        Adding r3 should change the rootTip to r3, headerTip to m2
        '''
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        m1 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m1)

        m2 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m2)

        r1 = state.rootTip.create_block_to_append()
        r2 = state.rootTip.create_block_to_append()
        r1.minorBlockHeaderList.append(m1.header)
        r1.finalize()

        state.add_root_block(r1)

        r2.minorBlockHeaderList.append(m1.header)
        r2.header.createTime = r1.header.createTime + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.get_hash(), r2.header.get_hash())

        state.add_root_block(r2)

        self.assertEqual(state.rootTip, r1.header)

        m3 = state.create_block_to_mine(address=acc1)
        self.assertEqual(m3.header.hashPrevRootBlock, r1.header.get_hash())
        state.finalize_and_add_block(m3)

        r3 = r2.create_block_to_append(address=acc1)
        r3.add_minor_block_header(m2.header)
        r3.finalize()
        state.add_root_block(r3)
        self.assertEqual(state.rootTip, r3.header)
        self.assertEqual(state.headerTip, m2.header)
