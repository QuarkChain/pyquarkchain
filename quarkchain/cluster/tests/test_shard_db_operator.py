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
        miner_addr = Address.create_random_account(full_shard_key=0)
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
        )
        self.assertTrue(state.add_tx(tx1))
        b1 = state.create_block_to_mine(address=miner_addr)
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)

        tx2 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc3,
            value=54321,
        )
        self.assertTrue(state.add_tx(tx2))
        b2 = state.create_block_to_mine(address=miner_addr)
        state.finalize_and_add_block(b2)
        self.assertEqual(state.header_tip, b2.header)

        # acc1 and acc2 should have the same transaction history
        # while acc3 is different
        tx_list1, _ = state.db.get_transactions_by_address(acc1)
        self.assertListEqual([t.value for t in tx_list1], [12345, 54321])
        tx_list2, _ = state.db.get_transactions_by_address(acc2)
        self.assertListEqual(tx_list2, tx_list1)
        tx_list3, _ = state.db.get_transactions_by_address(acc3)
        self.assertEqual(tx_list3[0].value, 54321)
        self.assertNotEqual(tx_list3, tx_list1)
