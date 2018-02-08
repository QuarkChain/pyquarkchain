import unittest
from quarkchain.chain import QuarkChain, ShardState, RootChain
from quarkchain.core import Address, Identity
from quarkchain.genesis import create_genesis_minor_block
from quarkchain.tests.test_utils import get_test_env, create_test_transaction


class TestQuarkChain(unittest.TestCase):

    def testQuarkChainBasic(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(
            0).createBlockToAppend(quarkash=100)
        b2 = qChain.minorChainManager.getGenesisBlock(
            1).createBlockToAppend(quarkash=200)
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=300)

        self.assertTrue(qChain.rootChain.appendBlock(rB))

    def testProofOfProgress(self):
        env = get_test_env()
        self.assertEqual(env.config.PROOF_OF_PROGRESS_BLOCKS, 1)
        qChain = QuarkChain(env)

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header]
        rB.finalize()

        self.assertFalse(qChain.rootChain.appendBlock(rB))

    def testUnordered(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b2.header, b1.header]
        rB.finalize()

        self.assertFalse(qChain.rootChain.appendBlock(rB))

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

        self.assertTrue(qChain.rootChain.appendBlock(rB))

        b4 = b3.createBlockToAppend()
        b5 = b2.createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b4))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b5))
        rB = rB.createBlockToAppend()
        rB.minorBlockHeaderList = [b4.header, b5.header]
        rB.finalize()
        self.assertTrue(qChain.rootChain.appendBlock(rB))

    def testQuarkChainRollBack(self):
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

        self.assertTrue(qChain.rootChain.appendBlock(rB))

        b4 = b3.createBlockToAppend()
        b5 = b2.createBlockToAppend()
        self.assertTrue(qChain.minorChainManager.addNewBlock(b4))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b5))
        rB = rB.createBlockToAppend()
        rB.minorBlockHeaderList = [b4.header, b5.header]
        rB.finalize()
        self.assertTrue(qChain.rootChain.appendBlock(rB))

        qChain.rootChain.rollBack()

        self.assertTrue(qChain.rootChain.appendBlock(rB))

    def testQuarkChainCoinbase(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        b1.header.coinbaseValue = 100
        b2.header.coinbaseValue = 200
        self.assertTrue(qChain.minorChainManager.addNewBlock(b1))
        self.assertTrue(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=b1.header.coinbaseValue +
                    b2.header.coinbaseValue + 1)

        self.assertFalse(qChain.rootChain.appendBlock(rB))


class TestShardState(unittest.TestCase):

    def testSimpleTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend()
        rootChain = RootChain(env)
        sState = ShardState(env, gBlock, rootChain)

        tx = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 6000, 4000)
        nBlock.addTx(tx)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertTrue(sState.appendBlock(nBlock))

    def testTwoTransactions(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend()
        rootChain = RootChain(env)
        sState = ShardState(env, gBlock, rootChain)

        tx1 = create_test_transaction(
            id1,
            gBlock.txList[0].getHash(),
            acc2,
            6000,
            4000)
        tx2 = create_test_transaction(id1, tx1.getHash(), acc2, 2000, 2000)
        nBlock.addTx(tx1)
        nBlock.addTx(tx2)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertTrue(sState.appendBlock(nBlock))

    def testNoneExistTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend()
        rootChain = RootChain(env)
        sState = ShardState(get_test_env(), gBlock, rootChain)

        tx = create_test_transaction(id1, bytes(32), acc2, 6000, 4000)
        nBlock.txList = [tx]
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertFalse(sState.appendBlock(nBlock))

    def testTwoBlocks(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend(acc1, quarkash=5000)
        rootChain = RootChain(env)
        sState = ShardState(get_test_env(), gBlock, rootChain)

        tx1 = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 6000, 4000)
        nBlock.addTx(tx1)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertTrue(sState.appendBlock(nBlock))

        nBlock1 = nBlock.createBlockToAppend(acc1, quarkash=5000)
        tx2 = create_test_transaction(
            id1, nBlock.txList[0].getHash(), acc2, 4000, 1000)
        nBlock1.addTx(tx2)
        nBlock1.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock1.finalizeMerkleRoot()
        self.assertTrue(sState.appendBlock(nBlock1))

    def testIncorrectRemaining(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend()
        rootChain = RootChain(env)
        sState = ShardState(env, gBlock, rootChain)

        tx = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 6000, 5000)
        nBlock.addTx(tx)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertFalse(sState.appendBlock(nBlock))

    def testSpentUTXO(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend()
        rootChain = RootChain(env)
        sState = ShardState(env, gBlock, rootChain)

        tx1 = create_test_transaction(
            id1,
            gBlock.txList[0].getHash(),
            acc2,
            6000,
            4000)
        tx2 = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 2000, 2000)
        nBlock.addTx(tx1)
        nBlock.addTx(tx2)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertFalse(sState.appendBlock(nBlock))

    def testRollBackOneTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, shardId=0)
        acc2 = Address.createRandomAccount(shardId=0)
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend()
        rootChain = RootChain(env)
        sState = ShardState(env, gBlock, rootChain)

        tx = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 6000, 4000)
        nBlock.addTx(tx)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertTrue(sState.appendBlock(nBlock))

        sState.rollBackTip()

        self.assertTrue(sState.appendBlock(nBlock))
