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
        # 1000 * 150 / 160 = 937
        self.assertEqual(qcState.getNextRootBlockDifficulty(), 937)

        b3 = qcState.createMinorBlockToAppend(0, createTime=45).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b3))
        # (37 + 31) * 15 / (15 + 20)
        self.assertEqual(qcState.getNextMinorBlockDifficulty(0), 29)

        rB1 = qcState.createRootBlockToAppend(300).extendMinorBlockHeaderList([b3.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB1))
        #  (937 + 1000) * 150 / (300)
        self.assertEqual(qcState.getNextRootBlockDifficulty(), 968)

    def testFindBestBlockToMine(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(acc1)
        env.config.setShardSize(2)
        env.config.GENESIS_CREATE_TIME = 0
        env.config.MINOR_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=2,
            targetIntervalSec=15,
            bootstrapSamples=1)
        env.config.ROOT_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=2,
            targetIntervalSec=150,
            bootstrapSamples=1)
        env.config.GENESIS_DIFFICULTY = 110
        env.config.GENESIS_MINOR_DIFFICULTY = 50

        qcState = QuarkChainState(env)
        isRootBlock, block = qcState.findBestBlockToMine(createTime=10)
        self.assertFalse(isRootBlock)
        self.assertEqual(block.header.branch.getShardId(), 0)
        self.assertIsNone(qcState.appendMinorBlock(block.finalizeMerkleRoot()))

        isRootBlock, block = qcState.findBestBlockToMine(createTime=15)
        self.assertFalse(isRootBlock)
        self.assertEqual(block.header.branch.getShardId(), 1)
        self.assertIsNone(qcState.appendMinorBlock(block.finalizeMerkleRoot()))

        isRootBlock, block = qcState.findBestBlockToMine(createTime=20)
        self.assertFalse(isRootBlock)
        self.assertEqual(block.header.branch.getShardId(), 1)
        self.assertIsNone(qcState.appendMinorBlock(block.finalizeMerkleRoot()))

        isRootBlock, block = qcState.findBestBlockToMine(createTime=30)
        self.assertTrue(isRootBlock)
        self.assertIsNone(qcState.appendRootBlock(block.finalize()))

    def testPoW(self):
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
        env.config.SKIP_MINOR_DIFFICULTY_CHECK = False
        env.config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.config.GENESIS_MINOR_DIFFICULTY = 1000
        env.config.GENESIS_DIFFICULTY = 2000

        qcState = QuarkChainState(env)
        self.assertEqual(qcState.getNextMinorBlockDifficulty(0), env.config.GENESIS_MINOR_DIFFICULTY)
        b1 = qcState.createMinorBlockToAppend(0, createTime=10).finalizeMerkleRoot()
        for i in range(0, 2 ** 32):
            b1.header.nonce = i
            if int.from_bytes(b1.header.getHash(), byteorder="big") * env.config.GENESIS_MINOR_DIFFICULTY < 2 ** 256:
                self.assertIsNone(qcState.appendMinorBlock(b1))
                break
            else:
                self.assertIsNotNone(qcState.appendMinorBlock(b1))

        rB = qcState.createRootBlockToMine(createTime=140).finalize()
        for i in range(0, 2 ** 32):
            rB.header.nonce = i
            if int.from_bytes(rB.header.getHash(), byteorder="big") * env.config.GENESIS_DIFFICULTY < 2 ** 256:
                self.assertIsNone(qcState.appendRootBlock(rB))
                break
            else:
                self.assertIsNotNone(qcState.appendRootBlock(rB))
