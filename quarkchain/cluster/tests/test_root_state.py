import unittest
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.cluster.core import CrossShardTransactionList


def create_default_state(env):
    rState = RootState(env=env, createGenesis=True)
    sState0 = ShardState(
        env=env,
        shardId=0,
        createGenesis=True)
    sState1 = ShardState(
        env=env,
        shardId=1,
        createGenesis=True)
    return (rState, [sState0, sState1])


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
        sStates[0].finalizeAndAddBlock(b0)
        sStates[1].addCrossShardTxListByMinorBlockHash(b0.header.getHash(), CrossShardTransactionList(txList=[]))
        b1 = sStates[1].getTip().createBlockToAppend()
        sStates[1].finalizeAndAddBlock(b1)
        sStates[0].addCrossShardTxListByMinorBlockHash(b1.header.getHash(), CrossShardTransactionList(txList=[]))

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
