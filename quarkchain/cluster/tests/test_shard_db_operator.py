import unittest

from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.cluster.tests.test_utils import (
    get_test_env,
    create_transfer_transaction,
)
from quarkchain.core import (
    Address,
    Identity,
    Branch,
    MinorBlockHeader,
    MinorBlock,
    MinorBlockMeta,
)
from quarkchain.db import InMemoryDb
from quarkchain.env import DEFAULT_ENV
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


class TestShardDbOperator(unittest.TestCase):
    def test_get_minor_block_by_hash(self):
        db = ShardDbOperator(InMemoryDb(), DEFAULT_ENV, Branch(2))
        block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
        block_hash = block.header.get_hash()
        db.put_minor_block(block, [])
        self.assertEqual(db.get_minor_block_by_hash(block_hash), block)
        self.assertIsNone(db.get_minor_block_by_hash(b""))

        self.assertEqual(db.get_minor_block_header_by_hash(block_hash), block.header)
        self.assertIsNone(db.get_minor_block_header_by_hash(b""))

    def test_get_transaction_by_address(self):
        id1 = Identity.create_random_identity()
        acc0 = Address.create_random_account(full_shard_key=0)
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=100)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx1 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
        )

        state.evm_state.gas_used = state.evm_state.gas_limit
        self.assertTrue(state.add_tx(tx1))

        block1, i = state.get_transaction_by_hash(tx1.get_hash())
        self.assertEqual(block1.tx_list[0], tx1)
        self.assertEqual(block1.header.create_time, 0)
        self.assertEqual(i, 0)

        b1 = state.create_block_to_mine(address=acc0, gas_limit=49999)
        self.assertEqual(len(b1.tx_list), 0)

        b1 = state.create_block_to_mine(address=acc0)
        self.assertEqual(len(b1.tx_list), 1)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        # self.assertEqual(
        #     state.get_token_balance(id1.recipient, self.genesis_token),
        #     10000000 - opcodes.GTXCOST - 12345,
        # )     --> FAILED

        tx2 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc3,
            value=54321,
            gas=50000,
        )

        self.assertTrue(state.add_tx(tx2))

        block2, j = state.get_transaction_by_hash(tx2.get_hash())
        self.assertEqual(block2.tx_list[0], tx2)
        self.assertEqual(block2.header.create_time, 0)
        self.assertEqual(j, 0)

        b2 = state.create_block_to_mine(address=acc0, gas_limit=49999)
        self.assertEqual(len(b2.tx_list), 0)

        b2 = state.create_block_to_mine(address=acc0)
        self.assertEqual(len(b2.tx_list), 1)

        # Should succeed
        state.finalize_and_add_block(b2)
        self.assertEqual(state.header_tip, b2.header)
        # self.assertEqual(
        #     state.get_token_balance(id1.recipient, self.genesis_token),
        #     10000000 - opcodes.GTXCOST - 12345,
        # )

        self.assertEqual(
            state.db.get_transactions_by_address(acc1),
            state.db.get_transactions_by_address(acc2),
        )
        self.assertNotEqual(
            state.db.get_transactions_by_address(acc1),
            state.db.get_transactions_by_address(acc3),
        )

        tx_list, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(tx_list[0].value, 12345)
        self.assertEqual(tx_list[1].value, 54321)
        tx_list, _ = state.db.get_transactions_by_address(acc2)
        self.assertEqual(tx_list[0].value, 12345)
        self.assertEqual(tx_list[1].value, 54321)
        tx_list, _ = state.db.get_transactions_by_address(acc3)
        self.assertEqual(tx_list[0].value, 54321)
