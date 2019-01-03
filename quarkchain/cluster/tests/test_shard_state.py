import random
import unittest

from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import (
    get_test_env,
    create_transfer_transaction,
)
from quarkchain.core import CrossShardTransactionDeposit, CrossShardTransactionList
from quarkchain.core import Identity, Address
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.evm import opcodes
from quarkchain.genesis import GenesisManager


def create_default_shard_state(env, shard_id=0, diff_calc=None):
    genesis_manager = GenesisManager(env.quark_chain_config)
    shard_state = ShardState(env=env, shard_id=shard_id, diff_calc=diff_calc)
    shard_state.init_genesis_state(genesis_manager.create_root_block())
    return shard_state


class TestShardState(unittest.TestCase):
    def setUp(self):
        super().setUp()
        config = get_test_env().quark_chain_config
        self.root_coinbase = config.ROOT.COINBASE_AMOUNT
        self.shard_coinbase = config.SHARD_LIST[0].COINBASE_AMOUNT
        # to make test verification easier, assume following tax rate
        assert config.REWARD_TAX_RATE == 0.5

    def test_shard_state_simple(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        self.assertEqual(state.root_tip.height, 0)
        self.assertEqual(state.header_tip.height, 0)
        # make sure genesis minor block has the right coinbase after-tax
        self.assertEqual(state.header_tip.coinbase_amount, 2500000000000000000)

    def test_gas_price(self):
        id_list = [Identity.create_random_identity() for _ in range(5)]
        acc_list = [Address.create_from_identity(i, full_shard_id=0) for i in id_list]
        env = get_test_env(genesis_account=acc_list[0], genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # 5 tx per block, make 3 blocks
        for _ in range(3):
            for j in range(5):
                state.add_tx(
                    create_transfer_transaction(
                        shard_state=state,
                        key=id_list[j].get_key(),
                        from_address=acc_list[j],
                        to_address=random.choice(acc_list),
                        value=0,
                        gas_price=42 if j == 0 else 0,
                    )
                )
            b = state.create_block_to_mine(address=acc_list[1])
            state.finalize_and_add_block(b)

        # for testing purposes, update percentile to take max gas price
        state.gas_price_suggestion_oracle.percentile = 100
        gas_price = state.gas_price()
        self.assertEqual(gas_price, 42)
        # results should be cached (same header). updating oracle shouldn't take effect
        state.gas_price_suggestion_oracle.percentile = 50
        gas_price = state.gas_price()
        self.assertEqual(gas_price, 42)

    def test_estimate_gas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        tx_gen = lambda data: create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            data=data,
        )
        tx = tx_gen(b"")
        estimate = state.estimate_gas(tx, acc1)
        self.assertEqual(estimate, 21000)
        tx = tx_gen(b"12123478123412348125936583475758")
        estimate = state.estimate_gas(tx, acc1)
        self.assertEqual(estimate, 23176)

    def test_execute_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
        )
        # adding this line to make sure `execute_tx` would reset `gas_used`
        state.evm_state.gas_used = state.evm_state.gas_limit
        res = state.execute_tx(tx, acc1)
        self.assertEqual(res, b"")

    def test_add_tx_incorrect_from_shard_id(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=1)
        acc2 = Address.create_random_account(full_shard_id=1)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        # state is shard 0 but tx from shard 1
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
        )
        self.assertFalse(state.add_tx(tx))
        self.assertIsNone(state.execute_tx(tx, acc1))

    def test_one_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
        )
        state.evm_state.gas_used = state.evm_state.gas_limit
        self.assertTrue(state.add_tx(tx))

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block.tx_list[0], tx)
        self.assertEqual(block.header.create_time, 0)
        self.assertEqual(i, 0)

        # tx claims to use more gas than the limit and thus not included
        b1 = state.create_block_to_mine(address=acc3, gas_limit=49999)
        self.assertEqual(len(b1.tx_list), 0)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_balance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345
        )
        self.assertEqual(state.get_balance(acc2.recipient), 12345)
        self.assertEqual(
            state.get_balance(acc3.recipient),
            opcodes.GTXCOST // 2 + self.shard_coinbase // 2,
        )

        # Check receipts
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, 21000)

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        # Check receipts in storage
        resp = state.get_transaction_receipt(tx.get_hash())
        self.assertIsNotNone(resp)
        block, i, r = resp
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)
        self.assertEqual(r.success, b"\x01")
        self.assertEqual(r.gas_used, 21000)

        # Check Account has full_shard_id
        self.assertEqual(
            state.evm_state.get_full_shard_id(acc2.recipient), acc2.full_shard_id
        )

        tx_list, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(tx_list[0].value, 12345)
        tx_list, _ = state.db.get_transactions_by_address(acc2)
        self.assertEqual(tx_list[0].value, 12345)

    def test_duplicated_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
        )
        self.assertTrue(state.add_tx(tx))
        self.assertFalse(state.add_tx(tx))  # already in tx_queue

        self.assertEqual(len(state.tx_queue), 1)
        self.assertEqual(len(state.tx_dict), 1)

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(len(block.tx_list), 1)
        self.assertEqual(block.tx_list[0], tx)
        self.assertEqual(block.header.create_time, 0)
        self.assertEqual(i, 0)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_balance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345
        )
        self.assertEqual(state.get_balance(acc2.recipient), 12345)
        self.assertEqual(
            state.get_balance(acc3.recipient),
            opcodes.GTXCOST // 2 + self.shard_coinbase // 2,
        )

        # Check receipts
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, 21000)
        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        # tx already confirmed
        self.assertTrue(state.db.contain_transaction_hash(tx.get_hash()))
        self.assertFalse(state.add_tx(tx))

    def test_add_invalid_tx_fail(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=999999999999999999999,  # insane
        )
        self.assertFalse(state.add_tx(tx))
        self.assertEqual(len(state.tx_queue), 0)

    def test_add_non_neighbor_tx_fail(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=3)  # not acc1's neighbor
        acc3 = Address.create_random_account(full_shard_id=8)  # acc1's neighbor

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=0,
            gas=1000000,
        )
        self.assertFalse(state.add_tx(tx))
        self.assertEqual(len(state.tx_queue), 0)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc3,
            value=0,
            gas=1000000,
        )
        self.assertTrue(state.add_tx(tx))
        self.assertEqual(len(state.tx_queue), 1)

    def test_exceeding_xshard_limit(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_random_account(full_shard_id=1)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        # a huge number to make xshard tx limit become 0 so that no xshard tx can be
        # included in the block
        env.quark_chain_config.MAX_NEIGHBORS = 10 ** 18
        state = create_default_shard_state(env=env)

        # xshard tx
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 0)

        # inshard tx
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc3,
            value=12345,
            gas=50000,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

    def test_two_tx_in_one_block(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id2, full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=1000000,
            )
        )

        b0 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b0)
        self.assertEqual(state.get_balance(id1.recipient), 1000000)
        self.assertEqual(state.get_balance(acc2.recipient), 1000000)
        self.assertEqual(
            state.get_balance(acc3.recipient),
            opcodes.GTXCOST // 2 + self.shard_coinbase // 2,
        )

        # Check Account has full_shard_id
        self.assertEqual(
            state.evm_state.get_full_shard_id(acc2.recipient), acc2.full_shard_id
        )

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address(
                    acc2.recipient, acc2.full_shard_id + 2
                ),  # set a different full shard id
                value=12345,
                gas=50000,
            )
        )
        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id2.get_key(),
                from_address=acc2,
                to_address=acc1,
                value=54321,
                gas=40000,
            )
        )
        b1 = state.create_block_to_mine(address=acc3, gas_limit=40000)
        self.assertEqual(len(b1.tx_list), 1)
        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 2)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_balance(id1.recipient), 1000000 - opcodes.GTXCOST - 12345 + 54321
        )
        self.assertEqual(
            state.get_balance(acc2.recipient), 1000000 - opcodes.GTXCOST + 12345 - 54321
        )
        # 2 block rewards
        self.assertEqual(
            state.get_balance(acc3.recipient),
            int(opcodes.GTXCOST * 1.5) + self.shard_coinbase,
        )

        # Check receipts
        self.assertEqual(len(state.evm_state.receipts), 2)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, 21000)
        self.assertEqual(state.evm_state.receipts[1].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[1].gas_used, 42000)

        block, i = state.get_transaction_by_hash(b1.tx_list[0].get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        block, i = state.get_transaction_by_hash(b1.tx_list[1].get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 1)

        # Check acc2 full_shard_id doesn't change
        self.assertEqual(
            state.evm_state.get_full_shard_id(acc2.recipient), acc2.full_shard_id
        )

    def test_fork_does_not_confirm_tx(self):
        """Tx should only be confirmed and removed from tx queue by the best chain"""
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id2, full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=1000000,
            )
        )

        b0 = state.create_block_to_mine(address=acc3)
        b1 = state.create_block_to_mine(address=acc3)
        b0.tx_list = []  # make b0 empty
        state.finalize_and_add_block(b0)

        self.assertEqual(len(state.tx_queue), 1)

        self.assertEqual(len(b1.tx_list), 1)
        state.finalize_and_add_block(b1)
        # b1 is a fork and does not remove the tx from queue
        self.assertEqual(len(state.tx_queue), 1)

        b2 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b2)
        self.assertEqual(len(state.tx_queue), 0)

    def test_revert_fork_put_tx_back_to_queue(self):
        """Tx in the reverted chain should be put back to the queue"""
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id2, full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=1000000,
            )
        )

        b0 = state.create_block_to_mine(address=acc3)
        b1 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b0)

        self.assertEqual(len(state.tx_queue), 0)

        b1.tx_list = []  # make b1 empty
        state.finalize_and_add_block(b1)
        self.assertEqual(len(state.tx_queue), 0)

        b2 = b1.create_block_to_append()
        state.finalize_and_add_block(b2)

        # now b1-b2 becomes the best chain and we expect b0 to be reverted and put the tx back to queue
        self.assertEqual(len(state.tx_queue), 1)

        b3 = b0.create_block_to_append()
        state.finalize_and_add_block(b3)
        self.assertEqual(len(state.tx_queue), 1)

        b4 = b3.create_block_to_append()
        state.finalize_and_add_block(b4)

        # b0-b3-b4 becomes the best chain
        self.assertEqual(len(state.tx_queue), 0)

    def test_stale_block_count(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.create_block_to_mine(address=acc3)
        b2 = state.create_block_to_mine(address=acc3)
        b2.header.create_time += 1

        state.finalize_and_add_block(b1)
        self.assertEqual(state.db.get_block_count_by_height(1), 1)

        state.finalize_and_add_block(b2)
        self.assertEqual(state.db.get_block_count_by_height(1), 2)

    def test_xshard_tx_sent(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id1, full_shard_id=1)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        # Add a root block to update block gas limit so that xshard tx can be included
        root_block = (
            state.root_tip.create_block_to_append()
            .add_minor_block_header(state.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        state.add_tx(tx)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        self.assertEqual(state.evm_state.gas_used, 0)
        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(len(state.evm_state.xshard_list), 1)
        self.assertEqual(
            state.evm_state.xshard_list[0],
            CrossShardTransactionDeposit(
                tx_hash=tx.get_hash(),
                from_address=acc1,
                to_address=acc2,
                value=888888,
                gas_price=1,
            ),
        )
        self.assertEqual(
            state.get_balance(id1.recipient),
            10000000 - 888888 - (opcodes.GTXCOST + opcodes.GTXXSHARDCOST),
        )
        # Make sure the xshard gas is not used by local block
        self.assertEqual(
            state.evm_state.gas_used, opcodes.GTXCOST + opcodes.GTXXSHARDCOST
        )
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(
            state.get_balance(acc3.recipient),
            opcodes.GTXCOST // 2 + self.shard_coinbase // 2,
        )

    def test_xshard_tx_insufficient_gas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id1, full_shard_id=1)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=888888,
                gas=opcodes.GTXCOST,
            )
        )

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 0)
        self.assertEqual(len(state.tx_queue), 0)

    def test_xshard_tx_received(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id1, full_shard_id=16)
        acc3 = Address.create_random_account(full_shard_id=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=16)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(address=acc3)
        state0.finalize_and_add_block(b2)

        self.assertEqual(state0.get_balance(acc1.recipient), 10000000 + 888888)
        # Half collected by root
        self.assertEqual(
            state0.get_balance(acc3.recipient),
            opcodes.GTXXSHARDCOST * 2 // 2 + self.shard_coinbase // 2,
        )

        # X-shard gas used
        evmState0 = state0.evm_state
        self.assertEqual(evmState0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

    def test_xshard_tx_received_exclude_non_neighbor(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id1, full_shard_id=3)
        acc3 = Address.create_random_account(full_shard_id=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=3)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        tx = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(address=acc3)
        state0.finalize_and_add_block(b2)

        self.assertEqual(state0.get_balance(acc1.recipient), 10000000)
        # Half collected by root
        self.assertEqual(state0.get_balance(acc3.recipient), self.shard_coinbase // 2)

        # X-shard gas used
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, 0)

    def test_xshard_for_two_root_blocks(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        acc2 = Address.create_from_identity(id1, full_shard_id=1)
        acc3 = Address.create_random_account(full_shard_id=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from state1
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block0 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block0)

        b2 = state0.get_tip().create_block_to_append()
        state0.finalize_and_add_block(b2)

        b3 = b1.create_block_to_append()
        b3.header.hash_prev_root_block = root_block.header.get_hash()

        # Add a x-shard tx from state1
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b3.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=bytes(32),
                        from_address=acc2,
                        to_address=acc1,
                        value=385723,
                        gas_price=3,
                    )
                ]
            ),
        )

        root_block1 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b3.header)
            .finalize()
        )
        state0.add_root_block(root_block1)

        # Test x-shard gas limit when create_block_to_mine
        b5 = state0.create_block_to_mine(address=acc3, gas_limit=0)
        # Current algorithm allows at least one root block to be included
        self.assertEqual(b5.header.hash_prev_root_block, root_block0.header.get_hash())
        b6 = state0.create_block_to_mine(address=acc3, gas_limit=opcodes.GTXXSHARDCOST)
        self.assertEqual(b6.header.hash_prev_root_block, root_block0.header.get_hash())
        # There are two x-shard txs: one is root block coinbase with zero gas, and another is from shard 1
        b7 = state0.create_block_to_mine(
            address=acc3, gas_limit=2 * opcodes.GTXXSHARDCOST
        )
        self.assertEqual(b7.header.hash_prev_root_block, root_block1.header.get_hash())
        b8 = state0.create_block_to_mine(
            address=acc3, gas_limit=3 * opcodes.GTXXSHARDCOST
        )
        self.assertEqual(b8.header.hash_prev_root_block, root_block1.header.get_hash())

        # Add b0 and make sure all x-shard tx's are added
        b4 = state0.create_block_to_mine(address=acc3)
        self.assertEqual(b4.header.hash_prev_root_block, root_block1.header.get_hash())
        state0.finalize_and_add_block(b4)

        self.assertEqual(state0.get_balance(acc1.recipient), 10000000 + 888888 + 385723)
        # Half collected by root
        self.assertEqual(
            state0.get_balance(acc3.recipient),
            opcodes.GTXXSHARDCOST * (2 + 3) // 2 + self.shard_coinbase // 2,
        )

        # Check gas used for receiving x-shard tx
        self.assertEqual(state0.evm_state.gas_used, 18000)
        self.assertEqual(state0.evm_state.xshard_receive_gas_used, 18000)

    def test_fork_resolve(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        b0 = state.get_tip().create_block_to_append()
        b1 = state.get_tip().create_block_to_append()

        state.finalize_and_add_block(b0)
        self.assertEqual(state.header_tip, b0.header)

        # Fork happens, first come first serve
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b0.header)

        # Longer fork happens, override existing one
        b2 = b1.create_block_to_append()
        state.finalize_and_add_block(b2)
        self.assertEqual(state.header_tip, b2.header)

    def test_root_chain_first_consensus(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(
            address=Address.create_empty_account()
        )

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        evm_state = state1.run_block(b1)
        b1.finalize(evm_state=evm_state, coinbase_amount=evm_state.block_fee)

        # Create a root block containing the block with the x-shard tx
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(), tx_list=CrossShardTransactionList(tx_list=[])
        )
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        b00 = b0.create_block_to_append()
        state0.finalize_and_add_block(b00)
        self.assertEqual(state0.header_tip, b00.header)

        # Create another fork that is much longer (however not confirmed by root_block)
        b3 = b2.create_block_to_append()
        state0.finalize_and_add_block(b3)
        b4 = b3.create_block_to_append()
        state0.finalize_and_add_block(b4)
        self.assertGreater(b4.header.height, b00.header.height)
        self.assertEqual(state0.header_tip, b00.header)

    def test_shard_state_add_root_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(
            address=Address.create_empty_account()
        )

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        evm_state = state1.run_block(b1)
        b1.finalize(evm_state=evm_state, coinbase_amount=evm_state.block_fee)

        # Create a root block containing the block with the x-shard tx
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(), tx_list=CrossShardTransactionList(tx_list=[])
        )

        # Add one empty root block
        empty_root = state0.root_tip.create_block_to_append().finalize()
        state0.add_root_block(empty_root)

        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        root_block1 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        state0.add_root_block(root_block)

        b00 = b0.create_block_to_append()
        state0.finalize_and_add_block(b00)
        self.assertEqual(state0.header_tip, b00.header)

        # Create another fork that is much longer (however not confirmed by root_block)
        b3 = b2.create_block_to_append()
        state0.finalize_and_add_block(b3)
        b4 = b3.create_block_to_append()
        state0.finalize_and_add_block(b4)
        self.assertEqual(state0.header_tip, b00.header)
        self.assertEqual(state0.db.get_minor_block_by_height(2), b00)
        self.assertIsNone(state0.db.get_minor_block_by_height(3))

        b5 = b1.create_block_to_append()
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b5.header.get_hash(), tx_list=CrossShardTransactionList(tx_list=[])
        )

        self.assertFalse(state0.add_root_block(root_block1))

        # Add one empty root block
        empty_root = root_block1.create_block_to_append().finalize()
        state0.add_root_block(empty_root)
        root_block2 = (
            empty_root.create_block_to_append()
            .add_minor_block_header(b3.header)
            .add_minor_block_header(b4.header)
            .add_minor_block_header(b5.header)
            .finalize()
        )

        self.assertTrue(state0.add_root_block(root_block2))
        self.assertEqual(state0.header_tip, b4.header)
        self.assertEqual(state0.meta_tip, b4.meta)
        self.assertEqual(state0.root_tip, root_block2.header)

        self.assertEqual(state0.db.get_minor_block_by_height(2), b3)
        self.assertEqual(state0.db.get_minor_block_by_height(3), b4)

    def test_shard_state_add_root_block_too_many_minor_blocks(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=1
        )
        state = create_default_shard_state(env=env, shard_id=0)

        headers = [state.header_tip]
        for i in range(13):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            headers.append(b.header)

        root_block = (
            state.root_tip.create_block_to_append()
            .extend_minor_block_header_list(headers)
            .finalize()
        )

        # Too many blocks
        self.assertRaises(ValueError, state.add_root_block, root_block)

        self.assertEqual(state.get_unconfirmed_header_list(), headers[:13])

        # 10 blocks is okay
        root_block.minor_block_header_list = headers[:13]
        root_block.finalize()
        state.add_root_block(root_block)

    def test_shard_state_fork_resolve_with_higher_root_chain(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        b0 = state.get_tip().create_block_to_append()
        state.finalize_and_add_block(b0)
        root_block = (
            state.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .finalize()
        )

        self.assertEqual(state.header_tip, b0.header)
        self.assertTrue(state.add_root_block(root_block))

        b1 = state.get_tip().create_block_to_append()
        b2 = state.get_tip().create_block_to_append(nonce=1)
        b2.header.hash_prev_root_block = root_block.header.get_hash()
        b3 = state.get_tip().create_block_to_append(nonce=2)
        b3.header.hash_prev_root_block = root_block.header.get_hash()

        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)

        # Fork happens, although they have the same height, b2 survives since it confirms root block
        state.finalize_and_add_block(b2)
        self.assertEqual(state.header_tip, b2.header)

        # b3 confirms the same root block as b2, so it will not override b2
        state.finalize_and_add_block(b3)
        self.assertEqual(state.header_tip, b2.header)

    def test_shard_state_difficulty(self):
        env = get_test_env()
        for shard in env.quark_chain_config.SHARD_LIST:
            shard.GENESIS.DIFFICULTY = 10000

        env.quark_chain_config.SKIP_MINOR_DIFFICULTY_CHECK = False
        diff_calc = EthDifficultyCalculator(cutoff=9, diff_factor=2048, minimum_diff=1)
        env.quark_chain_config.NETWORK_ID = (
            1
        )  # other network ids will skip difficulty check
        state = create_default_shard_state(env=env, shard_id=0, diff_calc=diff_calc)

        # Check new difficulty
        b0 = state.create_block_to_mine(state.header_tip.create_time + 8)
        self.assertEqual(
            b0.header.difficulty,
            state.header_tip.difficulty // 2048 + state.header_tip.difficulty,
        )
        b0 = state.create_block_to_mine(state.header_tip.create_time + 9)
        self.assertEqual(b0.header.difficulty, state.header_tip.difficulty)
        b0 = state.create_block_to_mine(state.header_tip.create_time + 17)
        self.assertEqual(b0.header.difficulty, state.header_tip.difficulty)
        b0 = state.create_block_to_mine(state.header_tip.create_time + 24)
        self.assertEqual(
            b0.header.difficulty,
            state.header_tip.difficulty - state.header_tip.difficulty // 2048,
        )
        b0 = state.create_block_to_mine(state.header_tip.create_time + 35)
        self.assertEqual(
            b0.header.difficulty,
            state.header_tip.difficulty - state.header_tip.difficulty // 2048 * 2,
        )

    def test_shard_state_recovery_from_root_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        blockHeaders = []
        blockMetas = []
        for i in range(12):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            blockHeaders.append(b.header)
            blockMetas.append(b.meta)

        # add a fork
        b1 = state.db.get_minor_block_by_height(3)
        b1.header.create_time += 1
        state.finalize_and_add_block(b1)
        self.assertEqual(state.db.get_minor_block_by_hash(b1.header.get_hash()), b1)

        root_block = state.root_tip.create_block_to_append()
        root_block.minor_block_header_list = blockHeaders[:5]
        root_block.finalize()

        state.add_root_block(root_block)

        recoveredState = ShardState(env=env, shard_id=0)

        recoveredState.init_from_root_block(root_block)
        # forks are pruned
        self.assertIsNone(
            recoveredState.db.get_minor_block_by_hash(b1.header.get_hash())
        )
        self.assertEqual(
            recoveredState.db.get_minor_block_by_hash(
                b1.header.get_hash(), consistency_check=False
            ),
            b1,
        )

        self.assertEqual(recoveredState.root_tip, root_block.header)
        self.assertEqual(recoveredState.header_tip, blockHeaders[4])
        self.assertEqual(recoveredState.confirmed_header_tip, blockHeaders[4])
        self.assertEqual(recoveredState.meta_tip, blockMetas[4])
        self.assertEqual(
            recoveredState.evm_state.trie.root_hash, blockMetas[4].hash_evm_state_root
        )

    def test_add_block_receipt_root_not_match(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1)
        acc3 = Address.create_random_account(full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.create_block_to_mine(address=acc3)

        # Should succeed
        state.finalize_and_add_block(b1)
        evm_state = state.run_block(b1)
        b1.finalize(evm_state=evm_state, coinbase_amount=b1.header.coinbase_amount)
        b1.meta.hash_evm_receipt_root = bytes(32)
        # low-level db operation to clear existing records
        del state.db.m_header_pool[b1.header.get_hash()]
        with self.assertRaises(ValueError):
            state.add_block(b1)

    def test_not_update_tip_on_root_fork(self):
        """ block's hash_prev_root_block must be on the same chain with root_tip to update tip.

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
        """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        m1 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m1)

        r1 = state.root_tip.create_block_to_append()
        r2 = state.root_tip.create_block_to_append()
        r1.minor_block_header_list.append(m1.header)
        r1.finalize()

        state.add_root_block(r1)

        r2.minor_block_header_list.append(m1.header)
        r2.header.create_time = r1.header.create_time + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.get_hash(), r2.header.get_hash())

        state.add_root_block(r2)

        self.assertEqual(state.root_tip, r1.header)

        m2 = m1.create_block_to_append(address=acc1)
        m2.header.hash_prev_root_block = r2.header.get_hash()

        state.finalize_and_add_block(m2)
        # m2 is added
        self.assertEqual(state.db.get_minor_block_by_hash(m2.header.get_hash()), m2)
        # but m1 should still be the tip
        self.assertEqual(state.header_tip, m1.header)

    def test_add_root_block_revert_header_tip(self):
        """ block's hash_prev_root_block must be on the same chain with root_tip to update tip.

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
        Adding r1, r2, m3 makes r1 the root_tip, m3 the header_tip
        Adding r3 should change the root_tip to r3, header_tip to m2
        """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        m1 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m1)

        m2 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m2)

        r1 = state.root_tip.create_block_to_append()
        r2 = state.root_tip.create_block_to_append()
        r1.minor_block_header_list.append(m1.header)
        r1.finalize()

        state.add_root_block(r1)

        r2.minor_block_header_list.append(m1.header)
        r2.header.create_time = r1.header.create_time + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.get_hash(), r2.header.get_hash())

        state.add_root_block(r2)

        self.assertEqual(state.root_tip, r1.header)

        m3 = state.create_block_to_mine(address=acc1)
        self.assertEqual(m3.header.hash_prev_root_block, r1.header.get_hash())
        state.finalize_and_add_block(m3)

        r3 = r2.create_block_to_append(address=acc1)
        r3.add_minor_block_header(m2.header)
        r3.finalize()
        state.add_root_block(r3)
        self.assertEqual(state.root_tip, r3.header)
        self.assertEqual(state.header_tip, m2.header)
