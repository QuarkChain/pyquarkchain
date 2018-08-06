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
    shardId = block.header.branch.getShardId()
    sStates[shardId].finalizeAndAddBlock(block)
    blockHash = block.header.getHash()
    for i in range(block.header.branch.getShardSize()):
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
        b0 = sStates[0].getTip().createBlockToAppend()
        sStates[0].finalizeAndAddBlock(b0)
        b1 = sStates[1].getTip().createBlockToAppend()
        sStates[1].finalizeAndAddBlock(b1)

        rState.addValidatedMinorBlockHash(b0.header.getHash())
        rState.addValidatedMinorBlockHash(b1.header.getHash())
        rB = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB))

        self.assertIsNone(rState.getRootBlockByHeight(3))
        self.assertEqual(rState.getRootBlockByHeight(2), rB)
        self.assertEqual(rState.getRootBlockByHeight(1),
                         rState.getRootBlockByHash(rB.header.hashPrevBlock))

    def testRootStateAndShardStateAddBlock(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.getHash())
        rState.addValidatedMinorBlockHash(b1.header.getHash())
        rB = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB))

    def testRootStateAddBlockMissingMinorBlockHeader(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)
        b0 = sStates[0].getTip().createBlockToAppend()
        sStates[0].finalizeAndAddBlock(b0)
        b1 = sStates[1].getTip().createBlockToAppend()
        sStates[1].finalizeAndAddBlock(b1)

        rState.addValidatedMinorBlockHash(b0.header.getHash())
        rState.addValidatedMinorBlockHash(b1.header.getHash())
        rB = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b1.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.addBlock(rB)

        rB = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.addBlock(rB)

    def testRootStateAndShardStateAddTwoBlocks(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.getHash())
        rState.addValidatedMinorBlockHash(b1.header.getHash())
        rB0 = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB0))

        b2 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b2)
        b3 = sStates[1].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b3)

        rState.addValidatedMinorBlockHash(b2.header.getHash())
        rState.addValidatedMinorBlockHash(b3.header.getHash())
        rB1 = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b2.header) \
            .addMinorBlockHeader(b3.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))

    def testRootStateAndShardStateFork(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].getTip().createBlockToAppend()
        b2 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().createBlockToAppend(nonce=1)
        b3 = sStates[1].getTip().createBlockToAppend(nonce=1)
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.getHash())
        rState.addValidatedMinorBlockHash(b1.header.getHash())
        rB0 = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()
        rB1 = rState.tip.createBlockToAppend()

        self.assertTrue(rState.addBlock(rB0))
        self.assertTrue(sStates[0].addRootBlock(rB0))
        self.assertTrue(sStates[1].addRootBlock(rB0))

        add_minor_block_to_cluster(sStates, b2)
        add_minor_block_to_cluster(sStates, b3)

        rState.addValidatedMinorBlockHash(b2.header.getHash())
        rState.addValidatedMinorBlockHash(b3.header.getHash())
        rB1 = rB1 \
            .addMinorBlockHeader(b2.header) \
            .addMinorBlockHeader(b3.header) \
            .finalize()

        self.assertFalse(rState.addBlock(rB1))
        self.assertFalse(sStates[0].addRootBlock(rB1))
        self.assertFalse(sStates[1].addRootBlock(rB1))

        b4 = b2.createBlockToAppend()
        b5 = b3.createBlockToAppend()
        add_minor_block_to_cluster(sStates, b4)
        add_minor_block_to_cluster(sStates, b5)

        rState.addValidatedMinorBlockHash(b4.header.getHash())
        rState.addValidatedMinorBlockHash(b5.header.getHash())
        rB2 = rB1.createBlockToAppend() \
            .addMinorBlockHeader(b4.header) \
            .addMinorBlockHeader(b5.header) \
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
        b0 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.getHash())
        rState.addValidatedMinorBlockHash(b1.header.getHash())

        # Check new difficulty
        rB0 = rState.createBlockToMine(
            mHeaderList=[b0.header, b1.header],
            address=Address.createEmptyAccount(),
            createTime=rState.tip.createTime + 9)
        self.assertEqual(rState.tip.difficulty, rB0.header.difficulty)
        rB0 = rState.createBlockToMine(
            mHeaderList=[b0.header, b1.header],
            address=Address.createEmptyAccount(),
            createTime=rState.tip.createTime + 3)
        self.assertEqual(rState.tip.difficulty + rState.tip.difficulty // 2048, rB0.header.difficulty)

        rB0 = rState.createBlockToMine(
            mHeaderList=[b0.header, b1.header],
            address=Address.createEmptyAccount(),
            createTime=rState.tip.createTime + 26).finalize()
        self.assertEqual(rState.tip.difficulty - rState.tip.difficulty // 2048, rB0.header.difficulty)

        for i in range(0, 2 ** 32):
            rB0.header.nonce = i
            if int.from_bytes(rB0.header.getHash(), byteorder="big") * env.config.GENESIS_DIFFICULTY < 2 ** 256:
                self.assertTrue(rState.addBlock(rB0))
                break
            else:
                with self.assertRaises(ValueError):
                    rState.addBlock(rB0)

    def testRootStateRecovery(self):
        env = get_test_env()
        rState, sStates = create_default_state(env)

        b0 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b0)
        b1 = sStates[1].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b1)

        rState.addValidatedMinorBlockHash(b0.header.getHash())
        rState.addValidatedMinorBlockHash(b1.header.getHash())
        rB0 = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()

        rB00 = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB0))

        # create a fork
        rB00.header.createTime += 1
        rB00.finalize()
        self.assertNotEqual(rB0.header.getHash(), rB00.header.getHash())

        self.assertFalse(rState.addBlock(rB00))
        self.assertEqual(rState.db.getRootBlockByHash(rB00.header.getHash()), rB00)

        b2 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b2)
        b3 = sStates[1].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, b3)

        rState.addValidatedMinorBlockHash(b2.header.getHash())
        rState.addValidatedMinorBlockHash(b3.header.getHash())
        rB1 = rState.tip.createBlockToAppend() \
            .addMinorBlockHeader(b2.header) \
            .addMinorBlockHeader(b3.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))

        recoveredState = RootState(env=env)
        self.assertEqual(recoveredState.tip, rB0.header)
        self.assertEqual(recoveredState.db.getRootBlockByHeight(2), rB0)
        # fork is pruned from recovered state
        self.assertIsNone(recoveredState.db.getRootBlockByHash(rB00.header.getHash()))
        self.assertEqual(
            recoveredState.db.getRootBlockByHash(rB00.header.getHash(), consistencyCheck=False),
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

        m1 = sStates[0].getTip().createBlockToAppend()
        add_minor_block_to_cluster(sStates, m1)

        rState.addValidatedMinorBlockHash(m1.header.getHash())
        rB1 = rB0.createBlockToAppend(nonce=0) \
            .addMinorBlockHeader(m1.header) \
            .finalize()
        rB2 = rB0.createBlockToAppend(nonce=1) \
            .addMinorBlockHeader(m1.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))
        self.assertFalse(rState.addBlock(rB2))
        self.assertTrue(sStates[0].addRootBlock(rB1))
        self.assertFalse(sStates[0].addRootBlock(rB2))

        m2 = m1.createBlockToAppend()
        m2.header.hashPrevRootBlock = rB2.header.getHash()
        add_minor_block_to_cluster(sStates, m2)

        rState.addValidatedMinorBlockHash(m2.header.getHash())
        rB3 = rB1.createBlockToAppend() \
            .addMinorBlockHeader(m2.header) \
            .finalize()

        with self.assertRaises(ValueError):
            rState.addBlock(rB3)

        rB4 = rB2.createBlockToAppend() \
            .addMinorBlockHeader(m2.header) \
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

        m1 = sStates[0].getTip().createBlockToAppend(nonce=0)
        m2 = sStates[0].getTip().createBlockToAppend(nonce=1)
        add_minor_block_to_cluster(sStates, m1)
        add_minor_block_to_cluster(sStates, m2)

        rState.addValidatedMinorBlockHash(m1.header.getHash())
        rState.addValidatedMinorBlockHash(m2.header.getHash())
        rB1 = rB0.createBlockToAppend(nonce=0) \
            .addMinorBlockHeader(m1.header) \
            .finalize()
        rB2 = rB0.createBlockToAppend(nonce=1) \
            .addMinorBlockHeader(m2.header) \
            .finalize()

        self.assertTrue(rState.addBlock(rB1))
        self.assertFalse(rState.addBlock(rB2))
        self.assertTrue(sStates[0].addRootBlock(rB1))
        self.assertFalse(sStates[0].addRootBlock(rB2))

        m3 = m1.createBlockToAppend()
        m3.header.hashPrevRootBlock = rB2.header.getHash()
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(sStates, m3)

        m4 = m1.createBlockToAppend()
        m4.header.hashPrevRootBlock = rB1.header.getHash()
        add_minor_block_to_cluster(sStates, m4)

        # Test recovery
        sState0Recovered = ShardState(env, shardId=0, db=sStates[0].rawDb)
        sState0Recovered.initFromRootBlock(rB1)
        with self.assertRaises(ValueError):
            add_minor_block_to_cluster(sStates, m3)
