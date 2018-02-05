import unittest
from quarkchain.chain import QuarkChain
from quarkchain.tests.test_utils import get_test_env


class TestQuarkChain(unittest.TestCase):

    def testQuarkChainBasic(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize()

        self.assertTrue(qChain.rootChain.addNewBlock(rB))

    def testProofOfProgress(self):
        env = get_test_env()
        self.assertEqual(env.config.PROOF_OF_PROGRESS_BLOCKS, 1)
        qChain = QuarkChain(env)

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header]
        rB.finalize()

        self.assertFalse(qChain.rootChain.addNewBlock(rB))

    def testUnordered(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b2.header, b1.header]
        rB.finalize()

        self.assertFalse(qChain.rootChain.addNewBlock(rB))

    def testQuarkChainMultiple(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        b3 = b1.createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b2))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b3))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b3.header, b2.header]
        rB.finalize()

        self.assertTrue(qChain.rootChain.addNewBlock(rB))

        b4 = b3.createBlockToAppend()
        b5 = b2.createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b4))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b5))
        rB = rB.createBlockToAppend()
        rB.minorBlockHeaderList = [b4.header, b5.header]
        rB.finalize()
        self.assertTrue(qChain.rootChain.addNewBlock(rB))
