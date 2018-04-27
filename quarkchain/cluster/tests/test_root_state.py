import unittest
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.cluster.core import CrossShardTransactionList
import quarkchain.db


def create_default_state(env):
    rState = RootState(env=env, createGenesis=True)
    sState0 = ShardState(
        env=env,
        shardId=0,
        createGenesis=True,
        db=quarkchain.db.InMemoryDb())
    sState1 = ShardState(
        env=env,
        shardId=1,
        createGenesis=True,
        db=quarkchain.db.InMemoryDb())
    return (rState, [sState0, sState1])


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
        state = RootState(
            env=env,
            createGenesis=True)
        self.assertEqual(state.tip.height, 0)

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
