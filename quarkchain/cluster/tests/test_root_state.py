import unittest

import quarkchain.db
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.core import Address
from quarkchain.core import CrossShardTransactionList
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.genesis import GenesisManager


def create_default_state(env, diff_calc=None):
    r_state = RootState(env=env, diff_calc=diff_calc)
    s_state_list = []
    for shard_id in range(env.quark_chain_config.SHARD_SIZE):
        shard_state = ShardState(
            env=env, shard_id=shard_id, db=quarkchain.db.InMemoryDb()
        )
        shard_state.init_genesis_state(r_state.get_tip_block())
        s_state_list.append(shard_state)

    for state in s_state_list:
        block_hash = state.header_tip.get_hash()
        for dst_state in s_state_list:
            if state == dst_state:
                continue
            dst_state.add_cross_shard_tx_list_by_minor_block_hash(
                block_hash, CrossShardTransactionList(tx_list=[])
            )
        r_state.add_validated_minor_block_hash(block_hash)

    return (r_state, s_state_list)


def add_minor_block_to_cluster(s_states, block):
    shard_id = block.header.branch.get_shard_id()
    s_states[shard_id].finalize_and_add_block(block)
    block_hash = block.header.get_hash()
    for i in range(block.header.branch.get_shard_size()):
        if i == shard_id:
            continue
        s_states[i].add_cross_shard_tx_list_by_minor_block_hash(
            block_hash, CrossShardTransactionList(tx_list=[])
        )


class TestRootState(unittest.TestCase):
    def test_root_state_simple(self):
        env = get_test_env()
        state = RootState(env=env)
        self.assertEqual(state.tip.height, 0)

    def test_root_state_add_block(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        b0 = s_states[0].get_tip().create_block_to_append()
        s_states[0].finalize_and_add_block(b0)
        b1 = s_states[1].get_tip().create_block_to_append()
        s_states[1].finalize_and_add_block(b1)

        r_state.add_validated_minor_block_hash(b0.header.get_hash())
        r_state.add_validated_minor_block_hash(b1.header.get_hash())
        root_block = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(s_states[0].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(s_states[1].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block))

        self.assertIsNone(r_state.get_root_block_by_height(2))
        self.assertEqual(r_state.get_root_block_by_height(1), root_block)
        self.assertEqual(
            r_state.get_root_block_by_height(0),
            r_state.get_root_block_by_hash(root_block.header.hash_prev_block),
        )

    def test_root_state_and_shard_state_add_block(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        b0 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_states[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(b0.header.get_hash())
        r_state.add_validated_minor_block_hash(b1.header.get_hash())
        root_block = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(s_states[0].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(s_states[1].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block))

    def test_root_state_add_block_missing_minor_block_header(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        b0 = s_states[0].get_tip().create_block_to_append()
        s_states[0].finalize_and_add_block(b0)
        b1 = s_states[1].get_tip().create_block_to_append()
        s_states[1].finalize_and_add_block(b1)

        r_state.add_validated_minor_block_hash(b0.header.get_hash())
        r_state.add_validated_minor_block_hash(b1.header.get_hash())
        root_block = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(b1.header)
            .finalize()
        )

        with self.assertRaises(ValueError):
            r_state.add_block(root_block)

        root_block = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .finalize()
        )

        with self.assertRaises(ValueError):
            r_state.add_block(root_block)

    def test_root_state_and_shard_state_add_two_blocks(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)

        b0 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_states[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(b0.header.get_hash())
        r_state.add_validated_minor_block_hash(b1.header.get_hash())
        root_block0 = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(s_states[0].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(s_states[1].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block0))

        b2 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b2)
        b3 = s_states[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b3)

        r_state.add_validated_minor_block_hash(b2.header.get_hash())
        r_state.add_validated_minor_block_hash(b3.header.get_hash())
        root_block1 = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b3.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block1))

    def test_root_state_and_shard_state_fork(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)

        b0 = s_states[0].get_tip().create_block_to_append()
        b2 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_states[1].get_tip().create_block_to_append(nonce=1)
        b3 = s_states[1].get_tip().create_block_to_append(nonce=1)
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(b0.header.get_hash())
        r_state.add_validated_minor_block_hash(b1.header.get_hash())
        root_block0 = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(s_states[0].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(s_states[1].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        root_block1 = r_state.tip.create_block_to_append()

        self.assertTrue(r_state.add_block(root_block0))
        self.assertTrue(s_states[0].add_root_block(root_block0))
        self.assertTrue(s_states[1].add_root_block(root_block0))

        add_minor_block_to_cluster(s_states, b2)
        add_minor_block_to_cluster(s_states, b3)

        r_state.add_validated_minor_block_hash(b2.header.get_hash())
        r_state.add_validated_minor_block_hash(b3.header.get_hash())
        root_block1 = (
            root_block1.add_minor_block_header(
                s_states[0].db.get_minor_block_by_height(0).header
            )
            .add_minor_block_header(b2.header)
            .add_minor_block_header(s_states[1].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b3.header)
            .finalize()
        )

        self.assertFalse(r_state.add_block(root_block1))
        self.assertFalse(s_states[0].add_root_block(root_block1))
        self.assertFalse(s_states[1].add_root_block(root_block1))

        b4 = b2.create_block_to_append()
        b5 = b3.create_block_to_append()
        add_minor_block_to_cluster(s_states, b4)
        add_minor_block_to_cluster(s_states, b5)

        r_state.add_validated_minor_block_hash(b4.header.get_hash())
        r_state.add_validated_minor_block_hash(b5.header.get_hash())
        root_block2 = (
            root_block1.create_block_to_append()
            .add_minor_block_header(b4.header)
            .add_minor_block_header(b5.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block2))
        self.assertTrue(s_states[0].add_root_block(root_block2))
        self.assertTrue(s_states[1].add_root_block(root_block2))
        self.assertEqual(r_state.tip, root_block2.header)
        self.assertEqual(s_states[0].root_tip, root_block2.header)
        self.assertEqual(s_states[1].root_tip, root_block2.header)

    def test_root_state_difficulty(self):
        env = get_test_env()
        env.quark_chain_config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.quark_chain_config.ROOT.GENESIS.DIFFICULTY = 1000
        diff_calc = EthDifficultyCalculator(cutoff=9, diff_factor=2048, minimum_diff=1)
        env.quark_chain_config.NETWORK_ID = (
            1
        )  # other network ids will skip difficulty check

        r_state, s_states = create_default_state(env, diff_calc=diff_calc)
        g0 = s_states[0].header_tip
        b0 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b0)
        g1 = s_states[1].header_tip
        b1 = s_states[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(b0.header.get_hash())
        r_state.add_validated_minor_block_hash(b1.header.get_hash())

        # Check new difficulty
        root_block0 = r_state.create_block_to_mine(
            m_header_list=[b0.header, b1.header],
            address=Address.create_empty_account(),
            create_time=r_state.tip.create_time + 9,
        )
        self.assertEqual(r_state.tip.difficulty, root_block0.header.difficulty)
        root_block0 = r_state.create_block_to_mine(
            m_header_list=[b0.header, b1.header],
            address=Address.create_empty_account(),
            create_time=r_state.tip.create_time + 3,
        )
        self.assertEqual(
            r_state.tip.difficulty + r_state.tip.difficulty // 2048,
            root_block0.header.difficulty,
        )

        root_block0 = r_state.create_block_to_mine(
            m_header_list=[g0, b0.header, g1, b1.header],
            address=Address.create_empty_account(),
            create_time=r_state.tip.create_time + 26,
        ).finalize()
        self.assertEqual(
            r_state.tip.difficulty - r_state.tip.difficulty // 2048,
            root_block0.header.difficulty,
        )

    def test_root_state_recovery(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)

        b0 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_states[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(b0.header.get_hash())
        r_state.add_validated_minor_block_hash(b1.header.get_hash())
        root_block0 = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(s_states[0].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(s_states[1].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        root_block00 = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(s_states[0].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(s_states[1].db.get_minor_block_by_height(0).header)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block0))

        # create a fork
        root_block00.header.create_time += 1
        root_block00.finalize()
        self.assertNotEqual(
            root_block0.header.get_hash(), root_block00.header.get_hash()
        )

        self.assertFalse(r_state.add_block(root_block00))
        self.assertEqual(
            r_state.db.get_root_block_by_hash(root_block00.header.get_hash()),
            root_block00,
        )

        b2 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b2)
        b3 = s_states[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b3)

        r_state.add_validated_minor_block_hash(b2.header.get_hash())
        r_state.add_validated_minor_block_hash(b3.header.get_hash())
        root_block1 = (
            r_state.tip.create_block_to_append()
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b3.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block1))

        # now the longest chain is root_block0 <-- root_block1

        recovered_state = RootState(env=env)
        self.assertEqual(recovered_state.tip, root_block1.header)
        self.assertEqual(recovered_state.db.get_root_block_by_height(1), root_block0)
        self.assertEqual(recovered_state.db.get_root_block_by_height(2), root_block1)

        # fork is pruned from recovered state
        self.assertIsNone(
            recovered_state.db.get_root_block_by_hash(root_block00.header.get_hash())
        )
        self.assertEqual(
            recovered_state.db.get_root_block_by_hash(
                root_block00.header.get_hash(), consistency_check=False
            ),
            root_block00,
        )

    def test_add_root_block_with_minor_block_with_wrong_root_block_hash(self):
        """ Test for the following case
                 +--+    +--+
                 |r1|<---|r3|
                /+--+    +--+
               /   |      |
        +--+  /  +--+    +--+
        |r0|<----|m1|<---|m2|
        +--+  \  +--+    +--+
               \   |      |
                \+--+     |
                 |r2|<----+
                 +--+

        where r3 is invalid because m2 depends on r2, which is not in the r3 chain.
        """
        env = get_test_env(shard_size=1)
        r_state, s_states = create_default_state(env)
        genesis_header = s_states[0].header_tip

        root_block0 = r_state.get_tip_block()

        m1 = s_states[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, m1)

        r_state.add_validated_minor_block_hash(m1.header.get_hash())
        root_block1 = (
            root_block0.create_block_to_append(nonce=0)
            .add_minor_block_header(genesis_header)
            .add_minor_block_header(m1.header)
            .finalize()
        )
        root_block2 = (
            root_block0.create_block_to_append(nonce=1)
            .add_minor_block_header(genesis_header)
            .add_minor_block_header(m1.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block1))
        self.assertFalse(r_state.add_block(root_block2))
        self.assertTrue(s_states[0].add_root_block(root_block1))
        self.assertFalse(s_states[0].add_root_block(root_block2))

        m2 = m1.create_block_to_append()
        m2.header.hash_prev_root_block = root_block2.header.get_hash()
        add_minor_block_to_cluster(s_states, m2)

        r_state.add_validated_minor_block_hash(m2.header.get_hash())
        root_block3 = (
            root_block1.create_block_to_append()
            .add_minor_block_header(m2.header)
            .finalize()
        )

        with self.assertRaises(ValueError):
            r_state.add_block(root_block3)

        root_block4 = (
            root_block2.create_block_to_append()
            .add_minor_block_header(m2.header)
            .finalize()
        )
        self.assertTrue(r_state.add_block(root_block4))

    def test_add_minor_block_with_wrong_root_block_hash(self):
        """ Test for the following case
                 +--+
                 |r1|
                /+--+
               /   |
        +--+  /  +--+    +--+
        |r0|<----|m1|<---|m3|
        +--+  \  +--+    +--+
          ^    \          |
          |     \+--+     |
          |      |r2|<----+
          |      +--+
          |        |
          |      +--+
          +------|m2|
                 +--+
        where m3 is invalid because m3 depends on r2, whose minor chain is not the same chain as m3
        """
        env = get_test_env(shard_size=1)
        r_state, s_states = create_default_state(env)
        genesis_header = s_states[0].header_tip

        root_block0 = r_state.get_tip_block()

        m1 = s_states[0].get_tip().create_block_to_append(nonce=0)
        m2 = s_states[0].get_tip().create_block_to_append(nonce=1)
        add_minor_block_to_cluster(s_states, m1)
        add_minor_block_to_cluster(s_states, m2)

        r_state.add_validated_minor_block_hash(m1.header.get_hash())
        r_state.add_validated_minor_block_hash(m2.header.get_hash())
        root_block1 = (
            root_block0.create_block_to_append(nonce=0)
            .add_minor_block_header(genesis_header)
            .add_minor_block_header(m1.header)
            .finalize()
        )
        root_block2 = (
            root_block0.create_block_to_append(nonce=1)
            .add_minor_block_header(genesis_header)
            .add_minor_block_header(m2.header)
            .finalize()
        )

        self.assertTrue(r_state.add_block(root_block1))
        self.assertFalse(r_state.add_block(root_block2))
        self.assertTrue(s_states[0].add_root_block(root_block1))
        self.assertFalse(s_states[0].add_root_block(root_block2))

        m3 = m1.create_block_to_append()
        m3.header.hash_prev_root_block = root_block2.header.get_hash()
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(s_states, m3)

        m4 = m1.create_block_to_append()
        m4.header.hash_prev_root_block = root_block1.header.get_hash()
        add_minor_block_to_cluster(s_states, m4)

        # Test recovery
        s_state0_recovered = ShardState(env, shard_id=0, db=s_states[0].raw_db)
        s_state0_recovered.init_from_root_block(root_block1)
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(s_states, m3)
