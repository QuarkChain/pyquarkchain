import unittest

import quarkchain.db
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.core import Address
from quarkchain.core import CrossShardTransactionList
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.p2p import ecies


def create_default_state(env, diff_calc=None):
    r_state = RootState(env=env, diff_calc=diff_calc)
    s_state_list = dict()
    for full_shard_id in env.quark_chain_config.get_full_shard_ids():
        shard_state = ShardState(
            env=env, full_shard_id=full_shard_id, db=quarkchain.db.InMemoryDb()
        )
        mblock, coinbase_amount_map = shard_state.init_genesis_state(
            r_state.get_tip_block()
        )
        block_hash = mblock.header.get_hash()
        r_state.add_validated_minor_block_hash(
            block_hash, coinbase_amount_map.balance_map
        )
        s_state_list[full_shard_id] = shard_state

    # add a root block so that later minor blocks will be broadcasted to neighbor shards
    minor_header_list = []
    for state in s_state_list.values():
        minor_header_list.append(state.header_tip)

    root_block = r_state.create_block_to_mine(minor_header_list)
    assert r_state.add_block(root_block)
    for state in s_state_list.values():
        assert state.add_root_block(root_block)

    return (r_state, s_state_list)


def add_minor_block_to_cluster(s_states, block):
    """Add block to corresponding shard state and broadcast xshard list to other shards"""
    full_shard_id = block.header.branch.get_full_shard_id()
    s_states[full_shard_id].finalize_and_add_block(block)
    block_hash = block.header.get_hash()
    for dst_full_shard_id, state in s_states.items():
        if dst_full_shard_id == full_shard_id:
            continue
        state.add_cross_shard_tx_list_by_minor_block_hash(
            block_hash, CrossShardTransactionList(tx_list=[])
        )


class TestRootState(unittest.TestCase):
    def test_root_state_simple(self):
        env = get_test_env()
        state = RootState(env=env)
        self.assertEqual(state.tip.height, 0)

    def test_root_state_and_shard_state_add_block(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        s_state0 = s_states[2 | 0]
        s_state1 = s_states[2 | 1]
        b0 = s_state0.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_state1.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(
            b0.header.get_hash(), b0.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b1.header.get_hash(), b1.header.coinbase_amount_map.balance_map
        )
        root_block = r_state.create_block_to_mine([b0.header, b1.header])

        self.assertTrue(r_state.add_block(root_block))
        self.assertIsNone(r_state.get_root_block_by_height(3))
        self.assertEqual(r_state.get_root_block_by_height(2), root_block)
        self.assertEqual(r_state.get_root_block_by_height(None), root_block)
        self.assertEqual(
            r_state.get_root_block_by_height(1),
            r_state.get_root_block_by_hash(root_block.header.hash_prev_block),
        )

        self.assertTrue(s_state0.add_root_block(root_block))
        self.assertEqual(s_state0.root_tip, root_block.header)
        self.assertTrue(s_state1.add_root_block(root_block))
        self.assertEqual(s_state1.root_tip, root_block.header)

    def test_root_state_add_block_no_proof_of_progress(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        s_state0 = s_states[2 | 0]
        s_state1 = s_states[2 | 1]
        b0 = s_state0.create_block_to_mine()
        s_state0.finalize_and_add_block(b0)
        b1 = s_state1.create_block_to_mine()
        s_state1.finalize_and_add_block(b1)

        r_state.add_validated_minor_block_hash(
            b0.header.get_hash(), b0.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b1.header.get_hash(), b1.header.coinbase_amount_map.balance_map
        )

        root_block = r_state.create_block_to_mine([])
        self.assertTrue(r_state.add_block(root_block))
        root_block = r_state.create_block_to_mine([b0.header])
        self.assertTrue(r_state.add_block(root_block))
        root_block = r_state.create_block_to_mine([b1.header])
        self.assertTrue(r_state.add_block(root_block))

    def test_root_state_add_two_blocks(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        s_state0 = s_states[2 | 0]
        s_state1 = s_states[2 | 1]
        b0 = s_state0.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_state1.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(
            b0.header.get_hash(), b0.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b1.header.get_hash(), b1.header.coinbase_amount_map.balance_map
        )
        root_block0 = r_state.create_block_to_mine([b0.header, b1.header])

        self.assertTrue(r_state.add_block(root_block0))

        b2 = s_state0.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b2)
        b3 = s_state1.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b3)

        r_state.add_validated_minor_block_hash(
            b2.header.get_hash(), b2.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b3.header.get_hash(), b3.header.coinbase_amount_map.balance_map
        )
        root_block1 = r_state.create_block_to_mine([b2.header, b3.header])

        self.assertTrue(r_state.add_block(root_block1))

    def test_root_state_and_shard_state_fork(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)

        s_state0 = s_states[2 | 0]
        s_state1 = s_states[2 | 1]

        b0 = s_state0.create_block_to_mine()
        b2 = s_state0.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_state1.create_block_to_mine()
        b3 = s_state1.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(
            b0.header.get_hash(), b0.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b1.header.get_hash(), b1.header.coinbase_amount_map.balance_map
        )

        root_block0 = r_state.create_block_to_mine([b0.header, b1.header])
        root_block1 = r_state.create_block_to_mine([])

        self.assertTrue(r_state.add_block(root_block0))
        self.assertTrue(s_state0.add_root_block(root_block0))
        self.assertTrue(s_state1.add_root_block(root_block0))

        add_minor_block_to_cluster(s_states, b2)
        add_minor_block_to_cluster(s_states, b3)

        r_state.add_validated_minor_block_hash(
            b2.header.get_hash(), b2.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b3.header.get_hash(), b3.header.coinbase_amount_map.balance_map
        )

        root_block1.add_minor_block_header(b2.header).add_minor_block_header(
            b3.header
        ).finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block1.minor_block_header_list],
                root_block1.header.height,
            )
        )

        self.assertFalse(r_state.add_block(root_block1))
        self.assertFalse(s_state0.add_root_block(root_block1))
        self.assertFalse(s_state1.add_root_block(root_block1))

        b4 = b2.create_block_to_append()
        b5 = b3.create_block_to_append()
        add_minor_block_to_cluster(s_states, b4)
        add_minor_block_to_cluster(s_states, b5)

        r_state.add_validated_minor_block_hash(
            b4.header.get_hash(), b4.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b5.header.get_hash(), b5.header.coinbase_amount_map.balance_map
        )
        root_block2 = (
            root_block1.create_block_to_append()
            .add_minor_block_header(b4.header)
            .add_minor_block_header(b5.header)
        )
        root_block2.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block2.minor_block_header_list],
                root_block2.header.height,
            )
        )

        self.assertTrue(r_state.add_block(root_block2))
        self.assertTrue(s_state0.add_root_block(root_block2))
        self.assertTrue(s_state1.add_root_block(root_block2))
        self.assertEqual(r_state.tip, root_block2.header)
        self.assertEqual(s_state0.root_tip, root_block2.header)
        self.assertEqual(s_state1.root_tip, root_block2.header)

    def test_root_state_difficulty_and_coinbase(self):
        env = get_test_env()
        env.quark_chain_config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.quark_chain_config.ROOT.GENESIS.DIFFICULTY = 1000
        diff_calc = EthDifficultyCalculator(cutoff=9, diff_factor=2048, minimum_diff=1)
        env.quark_chain_config.NETWORK_ID = (
            1
        )  # other network ids will skip difficulty check
        env.quark_chain_config.REWARD_TAX_RATE = 0.8
        env.quark_chain_config.ROOT.COINBASE_AMOUNT = 5
        for c in env.quark_chain_config.shards.values():
            c.COINBASE_AMOUNT = 5

        r_state, s_states = create_default_state(env, diff_calc=diff_calc)
        s_state0 = s_states[2 | 0]
        s_state1 = s_states[2 | 1]
        g0 = s_state0.header_tip
        b0 = s_state0.get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b0)
        g1 = s_state1.header_tip
        b1 = s_state1.get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, b1)
        self.assertEqual(
            b0.header.coinbase_amount_map.balance_map,
            {env.quark_chain_config.genesis_token: 1},
        )
        self.assertEqual(
            b1.header.coinbase_amount_map.balance_map,
            {env.quark_chain_config.genesis_token: 1},
        )

        r_state.add_validated_minor_block_hash(
            b0.header.get_hash(), b0.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b1.header.get_hash(), b1.header.coinbase_amount_map.balance_map
        )

        # Test coinbase
        original_reward_tax_rate = env.quark_chain_config.REWARD_TAX_RATE
        for tax_rate in [0.8, 0.6, 0.9]:
            env.quark_chain_config.REWARD_TAX_RATE = tax_rate
            root_block_tmp = r_state.create_block_to_mine(
                m_header_list=[b0.header, b1.header],
                address=Address.create_empty_account(),
                create_time=r_state.tip.create_time + 9,
            )
            self.assertEqual(root_block_tmp.header.signature, bytes(65))  # empty sig
            # still use minor block's coinbase amount, 1
            self.assertEqual(
                root_block_tmp.header.coinbase_amount_map.balance_map[
                    env.quark_chain_config.genesis_token
                ],
                round((1 + 1) / (1 - tax_rate) * tax_rate + 5),
            )
        env.quark_chain_config.REWARD_TAX_RATE = original_reward_tax_rate

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
        )
        self.assertEqual(
            r_state.tip.difficulty - r_state.tip.difficulty // 2048,
            root_block0.header.difficulty,
        )

    def test_root_state_recovery(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)

        s_state0 = s_states[2 | 0]
        s_state1 = s_states[2 | 1]
        b0 = s_state0.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b0)
        b1 = s_state1.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b1)

        r_state.add_validated_minor_block_hash(
            b0.header.get_hash(), b0.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b1.header.get_hash(), b1.header.coinbase_amount_map.balance_map
        )
        root_block0 = r_state.create_block_to_mine([b0.header, b1.header])

        root_block00 = r_state.create_block_to_mine([b0.header, b1.header])

        self.assertTrue(r_state.add_block(root_block0))

        # create a fork
        root_block00.header.create_time += 1
        root_block00.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block00.minor_block_header_list],
                root_block00.header.height,
            )
        )
        self.assertNotEqual(
            root_block0.header.get_hash(), root_block00.header.get_hash()
        )

        self.assertFalse(r_state.add_block(root_block00))
        self.assertEqual(
            r_state.db.get_root_block_by_hash(root_block00.header.get_hash()),
            root_block00,
        )

        b2 = s_state0.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b2)
        b3 = s_state1.create_block_to_mine()
        add_minor_block_to_cluster(s_states, b3)

        r_state.add_validated_minor_block_hash(
            b2.header.get_hash(), b2.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            b3.header.get_hash(), b3.header.coinbase_amount_map.balance_map
        )
        root_block1 = r_state.create_block_to_mine([b2.header, b3.header])

        self.assertTrue(r_state.add_block(root_block1))

        # now the longest chain is root_block0 <-- root_block1
        # but root_block0 will become the new tip after recovery

        recovered_state = RootState(env=env)
        self.assertEqual(recovered_state.tip, root_block0.header)
        self.assertEqual(recovered_state.db.get_root_block_by_height(2), root_block0)
        self.assertEqual(recovered_state.get_root_block_by_height(None), root_block0)

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
        s_state0 = s_states[1 | 0]

        root_block0 = r_state.get_tip_block()

        m1 = s_state0.get_tip().create_block_to_append()
        add_minor_block_to_cluster(s_states, m1)

        r_state.add_validated_minor_block_hash(
            m1.header.get_hash(), m1.header.coinbase_amount_map.balance_map
        )
        root_block1 = root_block0.create_block_to_append(
            nonce=0
        ).add_minor_block_header(m1.header)
        root_block1.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block1.minor_block_header_list],
                root_block1.header.height,
            )
        )
        root_block2 = root_block0.create_block_to_append(
            nonce=1
        ).add_minor_block_header(m1.header)
        root_block2.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block2.minor_block_header_list],
                root_block2.header.height,
            )
        )

        self.assertTrue(r_state.add_block(root_block1))
        self.assertFalse(r_state.add_block(root_block2))
        self.assertTrue(s_state0.add_root_block(root_block1))
        self.assertFalse(s_state0.add_root_block(root_block2))

        m2 = m1.create_block_to_append()
        m2.header.hash_prev_root_block = root_block2.header.get_hash()
        add_minor_block_to_cluster(s_states, m2)

        r_state.add_validated_minor_block_hash(
            m2.header.get_hash(), m2.header.coinbase_amount_map.balance_map
        )
        root_block3 = root_block1.create_block_to_append().add_minor_block_header(
            m2.header
        )
        root_block3.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block3.minor_block_header_list],
                root_block3.header.height,
            )
        )

        with self.assertRaises(ValueError):
            r_state.add_block(root_block3)

        root_block4 = root_block2.create_block_to_append().add_minor_block_header(
            m2.header
        )
        root_block4.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block4.minor_block_header_list],
                root_block4.header.height,
            )
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
        s_state0 = s_states[1 | 0]

        root_block0 = r_state.get_tip_block()

        m1 = s_state0.get_tip().create_block_to_append(nonce=0)
        m2 = s_state0.get_tip().create_block_to_append(nonce=1)
        add_minor_block_to_cluster(s_states, m1)
        add_minor_block_to_cluster(s_states, m2)

        r_state.add_validated_minor_block_hash(
            m1.header.get_hash(), m1.header.coinbase_amount_map.balance_map
        )
        r_state.add_validated_minor_block_hash(
            m2.header.get_hash(), m2.header.coinbase_amount_map.balance_map
        )
        root_block1 = root_block0.create_block_to_append(
            nonce=0
        ).add_minor_block_header(m1.header)
        root_block1.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block1.minor_block_header_list],
                root_block1.header.height,
            )
        )
        root_block2 = root_block0.create_block_to_append(
            nonce=1
        ).add_minor_block_header(m2.header)
        root_block2.finalize(
            coinbase_tokens=r_state._calculate_root_block_coinbase(
                [header.get_hash() for header in root_block2.minor_block_header_list],
                root_block2.header.height,
            )
        )

        self.assertTrue(r_state.add_block(root_block1))
        self.assertFalse(r_state.add_block(root_block2))
        self.assertTrue(s_state0.add_root_block(root_block1))
        self.assertFalse(s_state0.add_root_block(root_block2))

        m3 = m1.create_block_to_append()
        m3.header.hash_prev_root_block = root_block2.header.get_hash()
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(s_states, m3)

        m4 = m1.create_block_to_append()
        m4.header.hash_prev_root_block = root_block1.header.get_hash()
        add_minor_block_to_cluster(s_states, m4)

        # Test recovery
        s_state0_recovered = ShardState(env, full_shard_id=1 | 0, db=s_state0.raw_db)
        s_state0_recovered.init_from_root_block(root_block1)
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(s_states, m3)

    def test_root_state_add_root_block_too_many_minor_blocks(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        s_state0 = s_states[2 | 0]
        headers = []
        max_mblock_in_rblock = s_state0.shard_config.max_blocks_per_shard_in_one_root_block

        for i in range(max_mblock_in_rblock + 1):
            b = s_state0.create_block_to_mine()
            add_minor_block_to_cluster(s_states, b)
            headers.append(b.header)
            r_state.add_validated_minor_block_hash(
                b.header.get_hash(), b.header.coinbase_amount_map.balance_map
            )

        root_block = r_state.create_block_to_mine(
            m_header_list=headers,
            create_time=headers[-1].create_time + 1
        )
        with self.assertRaisesRegexp(ValueError, "too many minor blocks in the root block for shard"):
            r_state.add_block(root_block)

        headers = headers[:max_mblock_in_rblock]
        root_block = r_state.create_block_to_mine(
            m_header_list=headers,
            create_time=headers[-1].create_time + 1
        )
        r_state.add_block(root_block)
