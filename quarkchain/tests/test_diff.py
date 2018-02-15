import unittest
from quarkchain.diff import MADifficultyCalculator
from quarkchain.tests.test_utils import get_test_env
from quarkchain.core import Identity, Address
from quarkchain.chain import QuarkChainState


class TestMADifficulty(unittest.TestCase):

    def testDifficulty(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(acc1)
        env.config.setShardSize(1)
        env.config.GENESIS_CREATE_TIME = 0
        env.config.MINOR_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=2,
            targetIntervalSec=15,
            bootstrapSamples=1)
        env.config.ROOT_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=2,
            targetIntervalSec=150,
            bootstrapSamples=1)

        qcState = QuarkChainState(env)
        self.assertEqual(qcState.getNextMinorBlockDifficulty(0), env.config.GENESIS_MINOR_DIFFICULTY)
        b1 = qcState.createMinorBlockToAppend(0, createTime=10).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        # 25 * 15 / 10 = 37
        self.assertEqual(qcState.getNextMinorBlockDifficulty(0), 37)
        b2 = qcState.createMinorBlockToAppend(0, createTime=30).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b2))
        # (25 + 37) * 15 / 30 = 31
        self.assertEqual(qcState.getNextMinorBlockDifficulty(0), 31)

        self.assertEqual(qcState.getNextRootBlockDifficulty(), env.config.GENESIS_DIFFICULTY)
        rB = qcState.createRootBlockToAppend(160).extendMinorBlockHeaderList([b1.header, b2.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB))
        # 100 * 150 / 160 = 93
        self.assertEqual(qcState.getNextRootBlockDifficulty(), 93)

        b3 = qcState.createMinorBlockToAppend(0, createTime=45).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b3))
        # (37 + 31) * 15 / (15 + 20)
        self.assertEqual(qcState.getNextMinorBlockDifficulty(0), 29)

        rB1 = qcState.createRootBlockToAppend(300).extendMinorBlockHeaderList([b3.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB1))
        #  (93 + 100) * 150 / (300)
        self.assertEqual(qcState.getNextRootBlockDifficulty(), 96)
