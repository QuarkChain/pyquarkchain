import unittest

import quarkchain.db
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.core import Address
from quarkchain.core import CrossShardTransactionList
from quarkchain.diff import EthDifficultyCalculator


def create_default_state(env):
    rState = RootState(env=env)
    sStateList = [
        ShardState(
            env=env,
            shardId=shardId,
            db=quarkchain.db.InMemoryDb()) for shardId in range(env.config.SHARD_SIZE)]
    return (rState, sStateList)


def add_minor_block_to_cluster(sStates, block):
    shardId = block.header.branch.get_shard_id()
    sStates[shardId].finalize_and_add_block(block)
    blockHash = block.header.get_hash()
    for i in range(block.header.branch.get_shard_size()):
        if i == shardId:
            continue
        sStates[i].add_cross_shard_tx_list_by_minor_block_hash(blockHash, CrossShardTransactionList(txList=[]))


class TestRootState(unittest.TestCase):

    def test_root_state_simple(self):
        env = get_test_env()
        state = RootState(env=env)
        self.assertEqual(state.tip.height, 1)

    def test_root_state_add_block(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].get_tip().create_block_to_append()
        sStates[0].finalize_and_add_block(b0)
        b1 = sStates[1].get_tip().create_block_to_append()
        sStates[1].finalize_and_add_block(b1)

        rState.add_validated_minor_block_hash(b0.header.get_hash())
        rState.add_validated_minor_block_hash(b1.header.get_hash())
        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB))

        self.assertIsNone(rState.get_root_block_by_height(3))
        self.assertEqual(rState.get_root_block_by_height(2), rB)
        self.assertEqual(rState.get_root_block_by_height(1),
                         rState.get_root_block_by_hash(rB.header.hashPrevBlock))

    def test_root_state_and_shard_state_add_block(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.add_validated_minor_block_hash(b0.header.get_hash())
        rState.add_validated_minor_block_hash(b1.header.get_hash())
        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB))

    def test_root_state_add_block_missing_minor_block_header(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].get_tip().create_block_to_append()
        sStates[0].finalize_and_add_block(b0)
        b1 = sStates[1].get_tip().create_block_to_append()
        sStates[1].finalize_and_add_block(b1)

        rState.add_validated_minor_block_hash(b0.header.get_hash())
        rState.add_validated_minor_block_hash(b1.header.get_hash())
        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b1.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.add_block(rB)

        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.add_block(rB)

    def test_root_state_and_shard_state_add_two_blocks(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.add_validated_minor_block_hash(b0.header.get_hash())
        rState.add_validated_minor_block_hash(b1.header.get_hash())
        rB0 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB0))

        b2 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b2)
        b3 = sStates[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b3)

        rState.add_validated_minor_block_hash(b2.header.get_hash())
        rState.add_validated_minor_block_hash(b3.header.get_hash())
        rB1 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b3.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB1))

    def test_root_state_and_shard_state_fork(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].get_tip().create_block_to_append()
        b2 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].get_tip().create_block_to_append(nonce=1)
        b3 = sStates[1].get_tip().create_block_to_append(nonce=1)
        add_minor_block_to_cluster(sStates, b1)

        rState.add_validated_minor_block_hash(b0.header.get_hash())
        rState.add_validated_minor_block_hash(b1.header.get_hash())
        rB0 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()
        rB1 = rState.tip.create_block_to_append()

        self.assertTrue(rState.add_block(rB0))
        self.assertTrue(sStates[0].add_root_block(rB0))
        self.assertTrue(sStates[1].add_root_block(rB0))

        add_minor_block_to_cluster(sStates, b2)
        add_minor_block_to_cluster(sStates, b3)

        rState.add_validated_minor_block_hash(b2.header.get_hash())
        rState.add_validated_minor_block_hash(b3.header.get_hash())
        rB1 = rB1 \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b3.header) \
            .finalize()

        self.assertFalse(rState.add_block(rB1))
        self.assertFalse(sStates[0].add_root_block(rB1))
        self.assertFalse(sStates[1].add_root_block(rB1))

        b4 = b2.create_block_to_append()
        b5 = b3.create_block_to_append()
        add_minor_block_to_cluster(sStates, b4)
        add_minor_block_to_cluster(sStates, b5)

        rState.add_validated_minor_block_hash(b4.header.get_hash())
        rState.add_validated_minor_block_hash(b5.header.get_hash())
        rB2 = rB1.create_block_to_append() \
            .add_minor_block_header(b4.header) \
            .add_minor_block_header(b5.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB2))
        self.assertTrue(sStates[0].add_root_block(rB2))
        self.assertTrue(sStates[1].add_root_block(rB2))
        self.assertEqual(rState.tip, rB2.header)
        self.assertEqual(sStates[0].rootTip, rB2.header)
        self.assertEqual(sStates[1].rootTip, rB2.header)

    def test_root_state_difficulty(self):
        env = get_test_env()
        env.config.GENESIS_DIFFICULTY = 1000
        env.config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.config.ROOT_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=9,
            diffFactor=2048,
            minimumDiff=1)
        env.config.NETWORK_ID = 1  # other network ids will skip difficulty check

        rState, sStates = create_default_state(env)
        b0 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.add_validated_minor_block_hash(b0.header.get_hash())
        rState.add_validated_minor_block_hash(b1.header.get_hash())

        # Check new difficulty
        rB0 = rState.create_block_to_mine(
            mHeaderList=[b0.header, b1.header],
            address=Address.create_empty_account(),
            createTime=rState.tip.createTime + 9)
        self.assertEqual(rState.tip.difficulty, rB0.header.difficulty)
        rB0 = rState.create_block_to_mine(
            mHeaderList=[b0.header, b1.header],
            address=Address.create_empty_account(),
            createTime=rState.tip.createTime + 3)
        self.assertEqual(rState.tip.difficulty + rState.tip.difficulty // 2048, rB0.header.difficulty)

        rB0 = rState.create_block_to_mine(
            mHeaderList=[b0.header, b1.header],
            address=Address.create_empty_account(),
            createTime=rState.tip.createTime + 26).finalize()
        self.assertEqual(rState.tip.difficulty - rState.tip.difficulty // 2048, rB0.header.difficulty)

        for i in range(0, 2 ** 32):
            rB0.header.nonce = i
            if int.from_bytes(rB0.header.get_hash(), byteorder="big") * env.config.GENESIS_DIFFICULTY < 2 ** 256:
                self.assertTrue(rState.add_block(rB0))
                break
            else:
                with self.assertRaises(ValueError):
                    rState.add_block(rB0)

    def test_root_state_recovery(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.add_validated_minor_block_hash(b0.header.get_hash())
        rState.add_validated_minor_block_hash(b1.header.get_hash())
        rB0 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        rB00 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB0))

        # create a fork
        rB00.header.createTime += 1
        rB00.finalize()
        self.assertNotEqual(rB0.header.get_hash(), rB00.header.get_hash())

        self.assertFalse(rState.add_block(rB00))
        self.assertEqual(rState.db.get_root_block_by_hash(rB00.header.get_hash()), rB00)

        b2 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b2)
        b3 = sStates[1].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b3)

        rState.add_validated_minor_block_hash(b2.header.get_hash())
        rState.add_validated_minor_block_hash(b3.header.get_hash())
        rB1 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b3.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB1))

        recoveredState = RootState(env=env)
        self.assertEqual(recoveredState.tip, rB0.header)
        self.assertEqual(recoveredState.db.get_root_block_by_height(2), rB0)
        # fork is pruned from recovered state
        self.assertIsNone(recoveredState.db.get_root_block_by_hash(rB00.header.get_hash()))
        self.assertEqual(
            recoveredState.db.get_root_block_by_hash(rB00.header.get_hash(), consistencyCheck=False),
            rB00)

    def test_add_root_block_with_minor_block_with_wrong_root_block_hash(self):
        ''' Test for the following case
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
        '''
        env = get_test_env(shardSize=1)
        rState, sStates = create_default_state(env)

        rB0 = rState.get_tip_block()

        m1 = sStates[0].get_tip().create_block_to_append()
        add_minor_block_to_cluster(sStates, m1)

        rState.add_validated_minor_block_hash(m1.header.get_hash())
        rB1 = rB0.create_block_to_append(nonce=0) \
            .add_minor_block_header(m1.header) \
            .finalize()
        rB2 = rB0.create_block_to_append(nonce=1) \
            .add_minor_block_header(m1.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB1))
        self.assertFalse(rState.add_block(rB2))
        self.assertTrue(sStates[0].add_root_block(rB1))
        self.assertFalse(sStates[0].add_root_block(rB2))

        m2 = m1.create_block_to_append()
        m2.header.hashPrevRootBlock = rB2.header.get_hash()
        add_minor_block_to_cluster(sStates, m2)

        rState.add_validated_minor_block_hash(m2.header.get_hash())
        rB3 = rB1.create_block_to_append() \
            .add_minor_block_header(m2.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.add_block(rB3)

        rB4 = rB2.create_block_to_append() \
            .add_minor_block_header(m2.header) \
            .finalize()
        self.assertTrue(rState.add_block(rB4))

    def test_add_minor_block_with_wrong_root_block_hash(self):
        ''' Test for the following case
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
        where m3 is invalid because m3 depeonds on r2, whose minor chain is not the same chain as m3
        '''
        env = get_test_env(shardSize=1)
        rState, sStates = create_default_state(env)

        rB0 = rState.get_tip_block()

        m1 = sStates[0].get_tip().create_block_to_append(nonce=0)
        m2 = sStates[0].get_tip().create_block_to_append(nonce=1)
        add_minor_block_to_cluster(sStates, m1)
        add_minor_block_to_cluster(sStates, m2)

        rState.add_validated_minor_block_hash(m1.header.get_hash())
        rState.add_validated_minor_block_hash(m2.header.get_hash())
        rB1 = rB0.create_block_to_append(nonce=0) \
            .add_minor_block_header(m1.header) \
            .finalize()
        rB2 = rB0.create_block_to_append(nonce=1) \
            .add_minor_block_header(m2.header) \
            .finalize()

        self.assertTrue(rState.add_block(rB1))
        self.assertFalse(rState.add_block(rB2))
        self.assertTrue(sStates[0].add_root_block(rB1))
        self.assertFalse(sStates[0].add_root_block(rB2))

        m3 = m1.create_block_to_append()
        m3.header.hashPrevRootBlock = rB2.header.get_hash()
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(sStates, m3)

        m4 = m1.create_block_to_append()
        m4.header.hashPrevRootBlock = rB1.header.get_hash()
        add_minor_block_to_cluster(sStates, m4)

        # Test recovery
        sState0Recovered = ShardState(env, shardId=0, db=sStates[0].rawDb)
        sState0Recovered.init_from_root_block(rB1)
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(sStates, m3)
