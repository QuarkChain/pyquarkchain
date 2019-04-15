import random
import unittest
from fractions import Fraction

from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import (
    get_test_env,
    create_transfer_transaction,
    create_contract_creation_transaction,
)
from quarkchain.config import ConsensusType
from quarkchain.core import CrossShardTransactionDeposit, CrossShardTransactionList
from quarkchain.core import Identity, Address, TokenBalanceMap
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.evm import opcodes
from quarkchain.genesis import GenesisManager


def create_default_shard_state(
    env, shard_id=0, diff_calc=None, posw_override=False, no_coinbase=False
):
    genesis_manager = GenesisManager(env.quark_chain_config)
    shard_size = next(iter(env.quark_chain_config.shards.values())).SHARD_SIZE
    full_shard_id = shard_size | shard_id
    if posw_override:
        posw_config = env.quark_chain_config.shards[full_shard_id].POSW_CONFIG
        posw_config.ENABLED = True
    if no_coinbase:
        env.quark_chain_config.shards[full_shard_id].COINBASE_AMOUNT = 0
    shard_state = ShardState(env=env, full_shard_id=full_shard_id, diff_calc=diff_calc)
    shard_state.init_genesis_state(genesis_manager.create_root_block())
    return shard_state


class TestShardState(unittest.TestCase):
    def setUp(self):
        super().setUp()
        config = get_test_env().quark_chain_config
        self.root_coinbase = config.ROOT.COINBASE_AMOUNT
        self.shard_coinbase = next(iter(config.shards.values())).COINBASE_AMOUNT
        # to make test verification easier, assume following tax rate
        assert config.REWARD_TAX_RATE == 0.5
        self.tax_rate = config.reward_tax_rate  # type: Fraction
        self.genesis_token = config.genesis_token  # type: int

    def get_after_tax_reward(self, value: int) -> int:
        return value * self.tax_rate.numerator // self.tax_rate.denominator

    def test_shard_state_simple(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        self.assertEqual(state.root_tip.height, 0)
        self.assertEqual(state.header_tip.height, 0)
        # make sure genesis minor block has the right coinbase after-tax
        self.assertEqual(
            state.header_tip.coinbase_amount_map.balance_map,
            {self.genesis_token: 2500000000000000000},
        )

    def test_init_genesis_state(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        genesis_header = state.header_tip
        root_block = state.root_tip.create_block_to_append(nonce=1234)
        root_block.header.height = 0
        root_block.finalize()

        new_genesis_block, _ = state.init_genesis_state(root_block)
        self.assertNotEqual(
            new_genesis_block.header.get_hash(), genesis_header.get_hash()
        )
        # header tip is still the old genesis header
        self.assertEqual(state.header_tip, genesis_header)

        block = new_genesis_block.create_block_to_append()
        state.finalize_and_add_block(block)
        # extending new_genesis_block doesn't change header_tip due to root chain first consensus
        self.assertEqual(state.header_tip, genesis_header)
        self.assertEqual(genesis_header, state.db.get_minor_block_by_height(0).header)

        # extending the root block will change the header_tip
        root_block = root_block.create_block_to_append(nonce=1234).finalize()
        root_block.finalize()
        self.assertTrue(state.add_root_block(root_block))
        # ideally header_tip should be block.header but we don't track tips on fork chains for the moment
        # and thus it reverted all the way back to genesis
        self.assertEqual(state.header_tip, new_genesis_block.header)
        self.assertEqual(new_genesis_block, state.db.get_minor_block_by_height(0))

    def test_gas_price(self):
        id_list = [Identity.create_random_identity() for _ in range(5)]
        acc_list = [Address.create_from_identity(i, full_shard_key=0) for i in id_list]
        env = get_test_env(genesis_account=acc_list[0], genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)
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
        acc1 = Address.create_from_identity(id1, full_shard_key=1)
        acc2 = Address.create_random_account(full_shard_key=1)
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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - opcodes.GTXCOST - 12345,
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token), 12345
        )
        # shard miner only receives a percentage of reward because of REWARD_TAX_RATE
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
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

        # Check Account has full_shard_key
        self.assertEqual(
            state.evm_state.get_full_shard_key(acc2.recipient), acc2.full_shard_key
        )

        tx_list, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(tx_list[0].value, 12345)
        tx_list, _ = state.db.get_transactions_by_address(acc2)
        self.assertEqual(tx_list[0].value, 12345)

    def test_duplicated_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - opcodes.GTXCOST - 12345,
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token), 12345
        )
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=3)  # not acc1's neighbor
        acc3 = Address.create_random_account(full_shard_key=8)  # acc1's neighbor

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        # a huge number to make xshard tx limit become 0 so that no xshard tx can be
        # included in the block
        env.quark_chain_config.MAX_NEIGHBORS = 10 ** 18
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        # add a xshard tx with large startgas
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=state.get_xshard_gas_limit() + 1,
        )
        self.assertFalse(state.add_tx(tx))

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
        self.assertEqual(len(b1.tx_list), 1)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token), 1000000
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token), 1000000
        )
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
        )

        # Check Account has full_shard_key
        self.assertEqual(
            state.evm_state.get_full_shard_key(acc2.recipient), acc2.full_shard_key
        )

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address(
                    acc2.recipient, acc2.full_shard_key + 2
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
        # Inshard gas limit is 40000 - 20000
        b1 = state.create_block_to_mine(
            address=acc3, gas_limit=40000, xshard_gas_limit=20000
        )
        self.assertEqual(len(b1.tx_list), 0)
        b1 = state.create_block_to_mine(
            address=acc3, gas_limit=40000, xshard_gas_limit=0
        )
        self.assertEqual(len(b1.tx_list), 1)
        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 2)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            1000000 - opcodes.GTXCOST - 12345 + 54321,
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token),
            1000000 - opcodes.GTXCOST + 12345 - 54321,
        )
        # 2 block rewards: 3 tx, 2 block rewards
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST * 3 + self.shard_coinbase * 2),
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

        # Check acc2 full_shard_key doesn't change
        self.assertEqual(
            state.evm_state.get_full_shard_key(acc2.recipient), acc2.full_shard_key
        )

    def test_fork_does_not_confirm_tx(self):
        """Tx should only be confirmed and removed from tx queue by the best chain"""
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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

        # tx is added back to queue in the end of create_block_to_mine
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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

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
                gas_token_id=self.genesis_token,
                transfer_token_id=self.genesis_token,
            ),
        )
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - 888888 - (opcodes.GTXCOST + opcodes.GTXXSHARDCOST),
        )
        # Make sure the xshard gas is not used by local block
        self.assertEqual(
            state.evm_state.gas_used, opcodes.GTXCOST + opcodes.GTXXSHARDCOST
        )
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
        )

    def test_xshard_tx_insufficient_gas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

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
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
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

        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 888888,
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXXSHARDCOST * 2 + self.shard_coinbase),
        )

        # X-shard gas used
        evmState0 = state0.evm_state
        self.assertEqual(evmState0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

    def test_xshard_tx_received_exclude_non_neighbor(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=3)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=3)

        b0 = state0.get_tip()

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

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        b2 = state0.create_block_to_mine(address=acc3)
        state0.finalize_and_add_block(b2)

        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token), 10000000
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(self.shard_coinbase),
        )

        # No xshard tx is processed on the receiving side due to non-neighbor
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, 0)

    def test_xshard_from_root_block(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        # Add a root block to update block gas limit so that xshard tx can be included
        root_block = (
            state.root_tip.create_block_to_append()
            .add_minor_block_header(state.header_tip)
            .finalize(
                coinbase_tokens={env.quark_chain_config.genesis_token: 1000000},
                coinbase_address=acc2,
            )
        )
        state.add_root_block(root_block)

        b0 = state.create_block_to_mine()
        state.finalize_and_add_block(b0)

        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token), 1000000
        )

    def test_xshard_for_two_root_blocks(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

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
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
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
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
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
        b6 = state0.create_block_to_mine(address=acc3, gas_limit=opcodes.GTXXSHARDCOST)
        self.assertEqual(b6.header.hash_prev_root_block, root_block1.header.get_hash())
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

        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 888888 + 385723,
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(
                opcodes.GTXXSHARDCOST * (2 + 3) + self.shard_coinbase
            ),
        )

        # Check gas used for receiving x-shard tx
        self.assertEqual(state0.evm_state.gas_used, 18000)
        self.assertEqual(state0.evm_state.xshard_receive_gas_used, 18000)

    def test_xshard_gas_limit(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

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

        # Add one block in shard 1 with 2 x-shard txs
        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx0 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx0)
        tx1 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=111111,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx1)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx0.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                    CrossShardTransactionDeposit(
                        tx_hash=tx1.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=111111,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b1.header)
            .finalize(
                coinbase_tokens={env0.quark_chain_config.genesis_token: 1000000},
                coinbase_address=acc1,
            )
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure one x-shard tx's are added
        b2 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 888888,
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXXSHARDCOST * 2 + self.shard_coinbase),
        )

        # X-shard gas used
        evmState0 = state0.evm_state
        self.assertEqual(evmState0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

        # Add b2 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 888888 + 111111,
        )

        # Add b3 and make sure no x-shard tx's are added
        b3 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b3, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 888888 + 111111,
        )

        b4 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b4, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        self.assertNotEqual(
            b2.meta.xshard_tx_cursor_info, b3.meta.xshard_tx_cursor_info
        )
        self.assertEqual(b3.meta.xshard_tx_cursor_info, b4.meta.xshard_tx_cursor_info)

        b5 = state0.create_block_to_mine(
            address=acc3,
            gas_limit=opcodes.GTXXSHARDCOST,
            xshard_gas_limit=2 * opcodes.GTXXSHARDCOST,
        )
        with self.assertRaises(ValueError):
            # xsahrd_gas_limit should be smaller than gas_limit
            state0.finalize_and_add_block(
                b5,
                gas_limit=opcodes.GTXXSHARDCOST,
                xshard_gas_limit=2 * opcodes.GTXXSHARDCOST,
            )

        b6 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        with self.assertRaises(ValueError):
            # xshard_gas_limit should be gas_limit // 2
            state0.finalize_and_add_block(b6)

    def test_xshard_gas_limit_from_multiple_shards(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_from_identity(id1, full_shard_key=8)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env2 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=16)
        state2 = create_default_shard_state(env=env1, shard_id=8)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .add_minor_block_header(state2.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)
        state2.add_root_block(root_block)

        # Add one block in shard 1 with 2 x-shard txs
        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx0 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx0)
        tx1 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=111111,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx1)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx0.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                    CrossShardTransactionDeposit(
                        tx_hash=tx1.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=111111,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                ]
            ),
        )

        # Add one block in shard 1 with 2 x-shard txs
        b2 = state2.get_tip().create_block_to_append()
        b2.header.hash_prev_root_block = root_block.header.get_hash()
        tx3 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=12345,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b2.add_tx(tx3)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b2.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx3.get_hash(),
                        from_address=acc3,
                        to_address=acc1,
                        value=12345,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b1.header)
            .finalize(
                coinbase_tokens={env0.quark_chain_config.genesis_token: 1000000},
                coinbase_address=acc1,
            )
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure one x-shard tx's are added
        b2 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 12345,
        )

        # X-shard gas used
        evmState0 = state0.evm_state
        self.assertEqual(evmState0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

        # Add b2 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 12345 + 888888,
        )

        # Add b3 and make sure no x-shard tx's are added
        b3 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b3, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 12345 + 888888 + 111111,
        )

    def test_xshard_rootblock_coinbase(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)

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

        # Create a root block containing the block with the x-shard tx
        root_block = state0.root_tip.create_block_to_append().finalize(
            coinbase_tokens={env0.quark_chain_config.genesis_token: 1000000},
            coinbase_address=acc1,
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add b0 and make sure one x-shard tx's are added
        b2 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000,
        )

        # Add b0 and make sure one x-shard tx's are added
        b3 = state1.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state1.finalize_and_add_block(b3, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state1.get_token_balance(acc1.recipient, self.genesis_token), 10000000
        )

    def test_xshard_smart_contract(self):
        pass

    def test_xshard_sender_gas_limit(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)

        b0 = state0.get_tip().create_block_to_append()
        b0.header.hash_prev_root_block = root_block.header.get_hash()
        tx0 = create_transfer_transaction(
            shard_state=state0,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=b0.meta.evm_xshard_gas_limit + 1,
            gas_price=1,
        )
        self.assertFalse(state0.add_tx(tx0))
        b0.add_tx(tx0)
        with self.assertRaisesRegexp(
            RuntimeError, "xshard evm tx exceeds xshard gas limit"
        ):
            state0.finalize_and_add_block(b0)

        b2 = state0.create_block_to_mine(
            xshard_gas_limit=opcodes.GTXCOST * 9, include_tx=False
        )
        b2.header.hash_prev_root_block = root_block.header.get_hash()
        tx2 = create_transfer_transaction(
            shard_state=state0,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=opcodes.GTXCOST * 10,
            gas_price=1,
        )
        self.assertFalse(state0.add_tx(tx2, xshard_gas_limit=opcodes.GTXCOST * 9))
        b2.add_tx(tx2)
        with self.assertRaisesRegexp(
            RuntimeError, "xshard evm tx exceeds xshard gas limit"
        ):
            state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXCOST * 9)

        b1 = state0.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx1 = create_transfer_transaction(
            shard_state=state0,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=b1.meta.evm_xshard_gas_limit,
            gas_price=1,
        )
        b1.add_tx(tx1)
        state0.finalize_and_add_block(b1)

    def test_fork_resolve(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        genesis = state0.header_tip

        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(
            address=Address.create_empty_account()
        )

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        evm_state = state1.run_block(b1)
        b1.finalize(
            evm_state=evm_state,
            coinbase_amount_map=TokenBalanceMap(evm_state.block_fee_tokens),
        )

        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        genesis = state0.header_tip
        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(
            address=Address.create_empty_account()
        )

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        evm_state = state1.run_block(b1)
        b1.finalize(
            evm_state=evm_state,
            coinbase_amount_map=TokenBalanceMap(evm_state.block_fee_tokens),
        )

        # Add one empty root block
        empty_root = state0.root_tip.create_block_to_append().finalize()
        state0.add_root_block(empty_root)

        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        root_block1 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=1
        )
        state = create_default_shard_state(env=env, shard_id=0)

        max_mblock_in_rblock = state.shard_config.max_blocks_per_shard_in_one_root_block

        headers = [state.header_tip]
        for i in range(max_mblock_in_rblock):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            headers.append(b.header)

        root_block = (
            state.root_tip.create_block_to_append()
            .extend_minor_block_header_list(headers)
            .finalize()
        )

        # Too many blocks
        with self.assertRaisesRegexp(
            ValueError, "too many minor blocks in the root block"
        ):
            state.add_root_block(root_block)

        self.assertEqual(
            state.get_unconfirmed_header_list(), headers[:max_mblock_in_rblock]
        )

        # 10 blocks is okay
        root_block.minor_block_header_list = headers[:max_mblock_in_rblock]
        root_block.finalize()
        state.add_root_block(root_block)

    def test_shard_state_fork_resolve_with_higher_root_chain(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        b0 = state.get_tip()  # genesis
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
        for shard_config in env.quark_chain_config.shards.values():
            shard_config.GENESIS.DIFFICULTY = 10000

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        block_headers = [state.header_tip]
        block_meta = [state.meta_tip]
        for i in range(12):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            block_headers.append(b.header)
            block_meta.append(b.meta)

        # add a fork
        b1 = state.db.get_minor_block_by_height(3)
        b1.header.create_time += 1
        state.finalize_and_add_block(b1)
        self.assertEqual(state.db.get_minor_block_by_hash(b1.header.get_hash()), b1)

        root_block = state.root_tip.create_block_to_append()
        root_block.minor_block_header_list = block_headers[:5]
        root_block.finalize()

        state.add_root_block(root_block)

        recovered_state = ShardState(env=env, full_shard_id=2 | 0)

        recovered_state.init_from_root_block(root_block)
        self.assertEqual(
            recovered_state.db.get_minor_block_by_hash(b1.header.get_hash()), b1
        )

        self.assertEqual(recovered_state.root_tip, root_block.header)
        self.assertEqual(recovered_state.header_tip, block_headers[4])
        self.assertEqual(recovered_state.confirmed_header_tip, block_headers[4])
        self.assertEqual(recovered_state.meta_tip, block_meta[4])
        self.assertEqual(
            recovered_state.evm_state.trie.root_hash, block_meta[4].hash_evm_state_root
        )

    def test_shard_state_recovery_from_genesis(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        block_headers = [state.header_tip]
        block_meta = [state.meta_tip]
        for i in range(12):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            block_headers.append(b.header)
            block_meta.append(b.meta)

        # Add a few empty root blocks
        root_block = None
        for i in range(3):
            root_block = state.root_tip.create_block_to_append()
            root_block.finalize()
            state.add_root_block(root_block)

        recovered_state = ShardState(env=env, full_shard_id=2 | 0)

        # expect to recover from genesis
        recovered_state.init_from_root_block(root_block)

        genesis = state.db.get_minor_block_by_height(0)
        self.assertEqual(recovered_state.root_tip, root_block.header)
        self.assertEqual(recovered_state.header_tip, genesis.header)
        self.assertIsNone(recovered_state.confirmed_header_tip)
        self.assertEqual(recovered_state.meta_tip, genesis.meta)
        self.assertEqual(
            recovered_state.evm_state.trie.root_hash, genesis.meta.hash_evm_state_root
        )

    def test_add_block_receipt_root_not_match(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.create_block_to_mine(address=acc3)

        # Should succeed
        state.finalize_and_add_block(b1)
        evm_state = state.run_block(b1)
        b1.finalize(
            evm_state=evm_state, coinbase_amount_map=b1.header.coinbase_amount_map
        )
        b1.meta.hash_evm_receipt_root = bytes(32)

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
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        # m1 is the genesis block
        m1 = state.db.get_minor_block_by_height(0)

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
          |    \   |       \
          |     \+--+.     +--+
          |      |r2|<-----|r3| (r3 includes m2)
          |      +--+      +--+
          |
          |      +--+
          +-----+|r4| (r4 includes m1)
                 +--+

        Initial state: r0 <- m1 <- m2
        Adding r1, r2, m3 makes r1 the root_tip, m3 the header_tip
        Adding r3 should change the root_tip to r3, header_tip to m2
        Adding r4 (greater total diff) will reset root_tip to r4, header_tip to m2
        """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        # m1 is the genesis block
        m1 = state.db.get_minor_block_by_height(0)

        m2 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m2)

        r0 = state.root_tip
        r1 = r0.create_block_to_append()
        r2 = r0.create_block_to_append()
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

        # greater total diff
        r4 = r0.create_block_to_append(difficulty=r3.header.total_difficulty * 2)
        r4.minor_block_header_list.append(m1.header)
        r4.finalize()
        state.add_root_block(r4)
        self.assertEqual(state.root_tip, r4.header)
        self.assertEqual(state.header_tip, m2.header)

    def test_posw_fetch_previous_coinbase_address(self):
        acc = Address.create_from_identity(
            Identity.create_random_identity(), full_shard_key=0
        )
        env = get_test_env(genesis_account=acc, genesis_minor_quarkash=0)
        state = create_default_shard_state(env=env, shard_id=0)

        m = state.get_tip().create_block_to_append(address=acc)
        coinbase_blockcnt = state._get_posw_coinbase_blockcnt(
            m.header.hash_prev_minor_block
        )
        self.assertEqual(len(coinbase_blockcnt), 1)  # Genesis
        state.finalize_and_add_block(m)

        # Note PoSW window size is 2
        prev_addr = None
        for i in range(4):
            random_acc = Address.create_random_account(full_shard_key=0)
            m = state.get_tip().create_block_to_append(address=random_acc)
            coinbase_blockcnt = state._get_posw_coinbase_blockcnt(
                m.header.hash_prev_minor_block
            )
            self.assertEqual(len(coinbase_blockcnt), 2)
            # Count should all equal 1
            self.assertEqual(len(set(coinbase_blockcnt.values())), 1)
            self.assertEqual(list(coinbase_blockcnt.values())[0], 1)
            if prev_addr:  # Should always contain previous block's coinbase
                self.assertTrue(prev_addr in coinbase_blockcnt)
            state.finalize_and_add_block(m)
            prev_addr = random_acc.recipient

        # Cached should have certain items
        self.assertEqual(len(state.coinbase_addr_cache), 5)

    def test_posw_coinbase_lockup(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        id2 = Identity.create_random_identity()
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=0)
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)

        # Add a root block to have all the shards initialized, also include the genesis from
        # another shard to allow x-shard tx TO that shard
        root_block = state.root_tip.create_block_to_append()
        root_block.add_minor_block_header(
            create_default_shard_state(env=env, shard_id=1).header_tip
        )
        state.add_root_block(root_block.finalize())

        # Coinbase in genesis should be disallowed
        self.assertEqual(len(state.evm_state.sender_disallow_list), 1)
        self.assertTrue(bytes(20) in state.evm_state.sender_disallow_list)

        m = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m)
        self.assertEqual(len(state.evm_state.sender_disallow_list), 2)
        self.assertGreater(
            state.get_token_balance(acc1.recipient, self.genesis_token), 0
        )

        # Try to send money from that account
        for i, is_xshard in enumerate([False, True]):
            full_shard_key = 1 if is_xshard else 0
            tx = create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address.create_empty_account(full_shard_key),
                value=1,
                gas=30000 if is_xshard else 21000,
            )
            res = state.execute_tx(tx, acc1)
            self.assertIsNone(res, "tx should fail")

            # Create a block including that tx, receipt should also report error
            self.assertTrue(state.add_tx(tx))
            m = state.create_block_to_mine(address=acc2)
            state.finalize_and_add_block(m)
            r = state.get_transaction_receipt(tx.get_hash())
            self.assertEqual(r[2].success, b"")  # Failure

            if i == 0:
                # Make sure the disallow rolling window now discards the first addr
                self.assertEqual(len(state.evm_state.sender_disallow_list), 2)
                self.assertTrue(bytes(20) not in state.evm_state.sender_disallow_list)
            else:
                # Disallow list should only have acc2
                self.assertEqual(len(state.evm_state.sender_disallow_list), 1)
                self.assertTrue(acc2.recipient in state.evm_state.sender_disallow_list)

    def test_posw_validate_minor_block_seal(self):
        acc = Address(b"\x01" * 20, full_shard_key=0)
        env = get_test_env(genesis_account=acc, genesis_minor_quarkash=256)
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)
        # Force PoSW
        state.shard_config.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 1
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 256
        state.shard_config.POSW_CONFIG.DIFF_DIVIDER = 1000

        self.assertEqual(
            state.get_token_balance(acc.recipient, self.genesis_token), 256
        )
        genesis = Address(bytes(20), 0)
        self.assertEqual(
            state.get_token_balance(genesis.recipient, self.genesis_token), 0
        )

        # Genesis already has 1 block but zero stake, so no change to block diff
        m = state.get_tip().create_block_to_append(address=genesis, difficulty=1000)
        with self.assertRaises(ValueError):
            state.finalize_and_add_block(m)

        # Total stake * block PoSW is 256, so acc should pass the check no matter
        # how many blocks he mined before
        for _ in range(4):
            for nonce in range(4):  # Try different nonce
                m = state.get_tip().create_block_to_append(
                    address=acc, difficulty=1000, nonce=nonce
                )
                state.validate_minor_block_seal(m)
            state.finalize_and_add_block(m)

    def test_posw_window_edge_cases(self):
        acc = Address(b"\x01" * 20, full_shard_key=0)
        env = get_test_env(genesis_account=acc, genesis_minor_quarkash=500)
        state = create_default_shard_state(
            env=env, shard_id=0, posw_override=True, no_coinbase=True
        )
        # Force PoSW
        state.shard_config.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 500
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 2
        state.shard_config.POSW_CONFIG.DIFF_DIVIDER = 1000

        # Use 0 to denote blocks mined by others, 1 for blocks mined by acc,
        # stake * state per block = 1 for acc, 0 <- [curr], so current block
        # should enjoy the diff adjustment
        m = state.get_tip().create_block_to_append(address=acc, difficulty=1000)
        state.finalize_and_add_block(m)

        # Make sure stakes didn't change
        self.assertEqual(
            state.get_token_balance(acc.recipient, self.genesis_token), 500
        )
        # 0 <- 1 <- [curr], the window already has one block with PoSW benefit,
        # mining new blocks should fail
        m = state.get_tip().create_block_to_append(address=acc, difficulty=1000)
        with self.assertRaises(ValueError):
            state.finalize_and_add_block(m)

    def test_incorrect_coinbase_amount(self):
        env = get_test_env()
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        b = state.create_block_to_mine()
        evm_state = state.run_block(b)
        b.finalize(
            evm_state=evm_state,
            coinbase_amount_map=state.get_coinbase_amount_map(b.header.height),
        )
        state.add_block(b)

        b = state.create_block_to_mine()
        evm = state.run_block(b)
        wrong_coinbase = state.get_coinbase_amount_map(b.header.height)
        wrong_coinbase.add({self.genesis_token: +1})
        b.finalize(evm_state=evm_state, coinbase_amount_map=wrong_coinbase)
        with self.assertRaises(ValueError):
            state.add_block(b)

    def test_shard_coinbase_decay(self):
        env = get_test_env()
        state = create_default_shard_state(env=env)
        coinbase = state.get_coinbase_amount_map(state.shard_config.EPOCH_INTERVAL)
        self.assertEqual(
            coinbase.balance_map,
            {
                env.quark_chain_config.genesis_token: state.shard_config.COINBASE_AMOUNT
                * env.quark_chain_config.BLOCK_REWARD_DECAY_FACTOR
                * env.quark_chain_config.REWARD_TAX_RATE
            },
        )
        coinbase = state.get_coinbase_amount_map(state.shard_config.EPOCH_INTERVAL + 1)
        self.assertEqual(
            coinbase.balance_map,
            {
                env.quark_chain_config.genesis_token: state.shard_config.COINBASE_AMOUNT
                * env.quark_chain_config.BLOCK_REWARD_DECAY_FACTOR
                * env.quark_chain_config.REWARD_TAX_RATE
            },
        )
        coinbase = state.get_coinbase_amount_map(state.shard_config.EPOCH_INTERVAL * 2)
        self.assertEqual(
            coinbase.balance_map,
            {
                env.quark_chain_config.genesis_token: state.shard_config.COINBASE_AMOUNT
                * env.quark_chain_config.BLOCK_REWARD_DECAY_FACTOR ** 2
                * env.quark_chain_config.REWARD_TAX_RATE
            },
        )

    def test_enable_tx_timestamp(self):
        # whitelist acc1, make tx to acc2
        # but do not whitelist acc2 and tx fails
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        id2 = Identity.create_random_identity()
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=5000000,
            gas=50000,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 1)

        env.quark_chain_config.ENABLE_TX_TIMESTAMP = b1.header.create_time + 100
        env.quark_chain_config.TX_WHITELIST_SENDERS = [acc1.recipient.hex()]
        b2 = state.create_block_to_mine()
        self.assertEqual(len(b2.tx_list), 1)
        state.finalize_and_add_block(b2)

        tx2 = create_transfer_transaction(
            shard_state=state,
            key=id2.get_key(),
            from_address=acc2,
            to_address=acc3,
            value=12345,
            gas=50000,
        )
        env.quark_chain_config.ENABLE_TX_TIMESTAMP = None
        self.assertTrue(state.add_tx(tx2))
        b3 = state.create_block_to_mine()
        self.assertEqual(len(b3.tx_list), 1)
        env.quark_chain_config.ENABLE_TX_TIMESTAMP = b1.header.create_time + 100
        b4 = state.create_block_to_mine()
        self.assertEqual(len(b4.tx_list), 0)

        with self.assertRaisesRegexp(
            RuntimeError, "unwhitelisted senders not allowed before tx is enabled"
        ):
            state.finalize_and_add_block(b3)

    def test_enable_evm_timestamp_with_contract_create(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_contract_creation_transaction(
            shard_state=state, key=id1.get_key(), from_address=acc1, to_full_shard_key=0
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 1)

        env.quark_chain_config.ENABLE_EVM_TIMESTAMP = b1.header.create_time + 100
        b2 = state.create_block_to_mine()
        self.assertEqual(len(b2.tx_list), 0)

        with self.assertRaisesRegexp(
            RuntimeError, "smart contract tx is not allowed before evm is enabled"
        ):
            state.finalize_and_add_block(b1)

    def test_enable_evm_timestamp_with_contract_call(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
            data=b"1234",
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 1)

        env.quark_chain_config.ENABLE_EVM_TIMESTAMP = b1.header.create_time + 100
        b2 = state.create_block_to_mine()
        self.assertEqual(len(b2.tx_list), 0)

        with self.assertRaisesRegexp(
            RuntimeError, "smart contract tx is not allowed before evm is enabled"
        ):
            state.finalize_and_add_block(b1)
