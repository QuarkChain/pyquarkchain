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
    sStates[shardId].finalizeAndAddBlock(block)
    blockHash = block.header.get_hash()
    for i in range(block.header.branch.get_shard_size()):
        if i == shardId:
            continue
        sStates[i].addCrossShardTxListByMinorBlockHash(blockHash, CrossShardTransactionList(txList=[]))


class TestRootState(unittest.TestCase):

    def testRootStateSimple(self):
        env = get_test_env()
        state = RootState(env=env)
        self.assertEqual(state.tip.height, 1)

    def testRootStateAddBlock(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].getTip().create_block_to_append()
        sStates[0].finalizeAndAddBlock(b0)
        b1 = sStates[1].getTip().create_block_to_append()
        sStates[1].finalizeAndAddBlock(b1)

        rState.addValidatedMinorBlockHash(b0.header.get_hash())
        rState.addValidatedMinorBlockHash(b1.header.get_hash())
        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB))

        self.assertIsNone(rState.getRootBlockByHeight(3))
        self.assertEqual(rState.getRootBlockByHeight(2), rB)
        self.assertEqual(rState.getRootBlockByHeight(1),
                         rState.getRootBlockByHash(rB.header.hashPrevBlock))

    def testRootStateAndShardStateAddBlock(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.get_hash())
        rState.addValidatedMinorBlockHash(b1.header.get_hash())
        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB))

    def testRootStateAddBlockMissingMinorBlockHeader(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].getTip().create_block_to_append()
        sStates[0].finalizeAndAddBlock(b0)
        b1 = sStates[1].getTip().create_block_to_append()
        sStates[1].finalizeAndAddBlock(b1)

        rState.addValidatedMinorBlockHash(b0.header.get_hash())
        rState.addValidatedMinorBlockHash(b1.header.get_hash())
        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b1.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.addBlock(rB)

        rB = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.addBlock(rB)

    def testRootStateAndShardStateAddTwoBlocks(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.get_hash())
        rState.addValidatedMinorBlockHash(b1.header.get_hash())
        rB0 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB0))

        b2 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b2)
        b3 = sStates[1].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b3)

        rState.addValidatedMinorBlockHash(b2.header.get_hash())
        rState.addValidatedMinorBlockHash(b3.header.get_hash())
        rB1 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b3.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))

    def testRootStateAndShardStateFork(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].getTip().create_block_to_append()
        b2 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().create_block_to_append(nonce=1)
        b3 = sStates[1].getTip().create_block_to_append(nonce=1)
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.get_hash())
        rState.addValidatedMinorBlockHash(b1.header.get_hash())
        rB0 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()
        rB1 = rState.tip.create_block_to_append()

        self.assertTrue(rState.addBlock(rB0))
        self.assertTrue(sStates[0].addRootBlock(rB0))
        self.assertTrue(sStates[1].addRootBlock(rB0))

        add_minor_block_to_cluster(sStates, b2)
        add_minor_block_to_cluster(sStates, b3)

        rState.addValidatedMinorBlockHash(b2.header.get_hash())
        rState.addValidatedMinorBlockHash(b3.header.get_hash())
        rB1 = rB1 \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b3.header) \
            .finalize()

        self.assertFalse(rState.addBlock(rB1))
        self.assertFalse(sStates[0].addRootBlock(rB1))
        self.assertFalse(sStates[1].addRootBlock(rB1))

        b4 = b2.create_block_to_append()
        b5 = b3.create_block_to_append()
        add_minor_block_to_cluster(sStates, b4)
        add_minor_block_to_cluster(sStates, b5)

        rState.addValidatedMinorBlockHash(b4.header.get_hash())
        rState.addValidatedMinorBlockHash(b5.header.get_hash())
        rB2 = rB1.create_block_to_append() \
            .add_minor_block_header(b4.header) \
            .add_minor_block_header(b5.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB2))
        self.assertTrue(sStates[0].addRootBlock(rB2))
        self.assertTrue(sStates[1].addRootBlock(rB2))
        self.assertEqual(rState.tip, rB2.header)
        self.assertEqual(sStates[0].rootTip, rB2.header)
        self.assertEqual(sStates[1].rootTip, rB2.header)

    def testRootStateDifficulty(self):
        env = get_test_env()
        env.config.GENESIS_DIFFICULTY = 1000
        env.config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.config.ROOT_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=9,
            diffFactor=2048,
            minimumDiff=1)
        env.config.NETWORK_ID = 1  # other network ids will skip difficulty check

        rState, sStates = create_default_state(env)
        b0 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.get_hash())
        rState.addValidatedMinorBlockHash(b1.header.get_hash())

        # Check new difficulty
        rB0 = rState.createBlockToMine(
            mHeaderList=[b0.header, b1.header],
            address=Address.create_empty_account(),
            createTime=rState.tip.createTime + 9)
        self.assertEqual(rState.tip.difficulty, rB0.header.difficulty)
        rB0 = rState.createBlockToMine(
            mHeaderList=[b0.header, b1.header],
            address=Address.create_empty_account(),
            createTime=rState.tip.createTime + 3)
        self.assertEqual(rState.tip.difficulty + rState.tip.difficulty // 2048, rB0.header.difficulty)

        rB0 = rState.createBlockToMine(
            mHeaderList=[b0.header, b1.header],
            address=Address.create_empty_account(),
            createTime=rState.tip.createTime + 26).finalize()
        self.assertEqual(rState.tip.difficulty - rState.tip.difficulty // 2048, rB0.header.difficulty)

        for i in range(0, 2 ** 32):
            rB0.header.nonce = i
            if int.from_bytes(rB0.header.get_hash(), byteorder="big") * env.config.GENESIS_DIFFICULTY < 2 ** 256:
                self.assertTrue(rState.addBlock(rB0))
                break
            else:
                with self.assertRaises(ValueError):
                    rState.addBlock(rB0)

    def testRootStateRecovery(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.get_hash())
        rState.addValidatedMinorBlockHash(b1.header.get_hash())
        rB0 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        rB00 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b0.header) \
            .add_minor_block_header(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB0))

        # create a fork
        rB00.header.createTime += 1
        rB00.finalize()
        self.assertNotEqual(rB0.header.get_hash(), rB00.header.get_hash())

        self.assertFalse(rState.addBlock(rB00))
        self.assertEqual(rState.db.getRootBlockByHash(rB00.header.get_hash()), rB00)

        b2 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b2)
        b3 = sStates[1].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, b3)

        rState.addValidatedMinorBlockHash(b2.header.get_hash())
        rState.addValidatedMinorBlockHash(b3.header.get_hash())
        rB1 = rState.tip.create_block_to_append() \
            .add_minor_block_header(b2.header) \
            .add_minor_block_header(b3.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))

        recoveredState = RootState(env=env)
        self.assertEqual(recoveredState.tip, rB0.header)
        self.assertEqual(recoveredState.db.getRootBlockByHeight(2), rB0)
        # fork is pruned from recovered state
        self.assertIsNone(recoveredState.db.getRootBlockByHash(rB00.header.get_hash()))
        self.assertEqual(
            recoveredState.db.getRootBlockByHash(rB00.header.get_hash(), consistencyCheck=False),
            rB00)

    def testAddRootBlockWithMinorBlockWithWrongRootBlockHash(self):
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

        rB0 = rState.getTipBlock()

        m1 = sStates[0].getTip().create_block_to_append()
        add_minor_block_to_cluster(sStates, m1)

        rState.addValidatedMinorBlockHash(m1.header.get_hash())
        rB1 = rB0.create_block_to_append(nonce=0) \
            .add_minor_block_header(m1.header) \
            .finalize()
        rB2 = rB0.create_block_to_append(nonce=1) \
            .add_minor_block_header(m1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))
        self.assertFalse(rState.addBlock(rB2))
        self.assertTrue(sStates[0].addRootBlock(rB1))
        self.assertFalse(sStates[0].addRootBlock(rB2))

        m2 = m1.create_block_to_append()
        m2.header.hashPrevRootBlock = rB2.header.get_hash()
        add_minor_block_to_cluster(sStates, m2)

        rState.addValidatedMinorBlockHash(m2.header.get_hash())
        rB3 = rB1.create_block_to_append() \
            .add_minor_block_header(m2.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.addBlock(rB3)

        rB4 = rB2.create_block_to_append() \
            .add_minor_block_header(m2.header) \
            .finalize()
        self.assertTrue(rState.addBlock(rB4))

    def testAddMinorBlockWithWrongRootBlockHash(self):
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

        rB0 = rState.getTipBlock()

        m1 = sStates[0].getTip().create_block_to_append(nonce=0)
        m2 = sStates[0].getTip().create_block_to_append(nonce=1)
        add_minor_block_to_cluster(sStates, m1)
        add_minor_block_to_cluster(sStates, m2)

        rState.addValidatedMinorBlockHash(m1.header.get_hash())
        rState.addValidatedMinorBlockHash(m2.header.get_hash())
        rB1 = rB0.create_block_to_append(nonce=0) \
            .add_minor_block_header(m1.header) \
            .finalize()
        rB2 = rB0.create_block_to_append(nonce=1) \
            .add_minor_block_header(m2.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))
        self.assertFalse(rState.addBlock(rB2))
        self.assertTrue(sStates[0].addRootBlock(rB1))
        self.assertFalse(sStates[0].addRootBlock(rB2))

        m3 = m1.create_block_to_append()
        m3.header.hashPrevRootBlock = rB2.header.get_hash()
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(sStates, m3)

        m4 = m1.create_block_to_append()
        m4.header.hashPrevRootBlock = rB1.header.get_hash()
        add_minor_block_to_cluster(sStates, m4)

        # Test recovery
        sState0Recovered = ShardState(env, shardId=0, db=sStates[0].rawDb)
        sState0Recovered.initFromRootBlock(rB1)
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(sStates, m3)
