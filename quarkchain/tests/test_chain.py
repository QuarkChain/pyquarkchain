import unittest
from quarkchain.chain import QuarkChain, ShardState, RootChain, QuarkChainState
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
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=300)

        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, {b1.header, b2.header}))

    def testProofOfProgress(self):
        env = get_test_env()
        self.assertEqual(env.config.PROOF_OF_PROGRESS_BLOCKS, 1)
        qChain = QuarkChain(env)

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header]
        rB.finalize()

        self.assertIsNotNone(qChain.rootChain.appendBlock(rB, {b1.header}))

    def testUnordered(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b2.header, b1.header]
        rB.finalize()

        self.assertIsNotNone(qChain.rootChain.appendBlock(
            rB, {b1.header, b2.header}))

    def testQuarkChainMultiple(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        b3 = b1.createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b2))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b3))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b3.header, b2.header]
        rB.finalize()

        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, {b1.header, b2.header, b3.header}))

        b4 = b3.createBlockToAppend()
        b5 = b2.createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b4))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b5))
        rB = rB.createBlockToAppend()
        rB.minorBlockHeaderList = [b4.header, b5.header]
        rB.finalize()
        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, {b4.header, b5.header}))

    def testQuarkChainRollBack(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        b3 = b1.createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b2))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b3))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b3.header, b2.header]
        rB.finalize()

        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, {b1.header, b2.header, b3.header}))

        b4 = b3.createBlockToAppend()
        b5 = b2.createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b4))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b5))
        rB = rB.createBlockToAppend()
        rB.minorBlockHeaderList = [b4.header, b5.header]
        rB.finalize()
        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, {b4.header, b5.header}))

        qChain.rootChain.rollBack()

        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, {b4.header, b5.header}))

    def testQuarkChainCoinbase(self):
        qChain = QuarkChain(get_test_env())

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend()
        b1.header.coinbaseValue = 100
        b2.header.coinbaseValue = 200
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=b1.header.coinbaseValue +
                    b2.header.coinbaseValue + 1)

        self.assertIsNotNone(qChain.rootChain.appendBlock(
            rB, {b1.header, b2.header}))

    def testQuarkChainTestnet(self):
        env = get_test_env()
        env.config.SKIP_MINOR_DIFFICULTY_CHECK = False
        env.config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.config.NETWORK_ID = 1
        qChain = QuarkChain(env)

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend(address=env.config.TESTNET_MASTER_ACCOUNT)
        b2 = qChain.minorChainManager.getGenesisBlock(1).createBlockToAppend(address=env.config.TESTNET_MASTER_ACCOUNT)
        b1.header.coinbaseValue = 100
        b2.header.coinbaseValue = 200
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=b1.header.coinbaseValue + b2.header.coinbaseValue + 1,
                    address=env.config.TESTNET_MASTER_ACCOUNT)

        self.assertIsNotNone(qChain.rootChain.appendBlock(
            rB, {b1.header, b2.header}))


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
        self.assertIsNone(sState.appendBlock(nBlock))

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
        self.assertIsNone(sState.appendBlock(nBlock))

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
        self.assertIsNotNone(sState.appendBlock(nBlock))

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
        self.assertIsNone(sState.appendBlock(nBlock))

        nBlock1 = nBlock.createBlockToAppend(acc1, quarkash=5000)
        tx2 = create_test_transaction(
            id1, nBlock.txList[0].getHash(), acc2, 4000, 1000)
        nBlock1.addTx(tx2)
        nBlock1.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock1.finalizeMerkleRoot()
        self.assertIsNone(sState.appendBlock(nBlock1))

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
        self.assertIsNotNone(sState.appendBlock(nBlock))

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
        self.assertIsNotNone(sState.appendBlock(nBlock))

    def testRollBackOneTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=0)
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
        self.assertIsNone(sState.appendBlock(nBlock))

        sState.rollBackTip()

        self.assertIsNone(sState.appendBlock(nBlock))

    def testTestnetMaster(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=0)
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        env.config.SKIP_MINOR_DIFFICULTY_CHECK = False
        env.config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.config.NETWORK_ID = 1
        gBlock = create_genesis_minor_block(env, 0)
        nBlock = gBlock.createBlockToAppend(address=acc1)
        rootChain = RootChain(env)
        sState = ShardState(env, gBlock, rootChain)

        tx = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 6000, 4000)
        nBlock.addTx(tx)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertIsNone(sState.appendBlock(nBlock))

        sState.rollBackTip()

        self.assertIsNone(sState.appendBlock(nBlock))


class TestQuarkChainState(unittest.TestCase):

    def testSimpleQuarkChainState(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend(
            quarkash=200).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))

        rB = qcState.getGenesisRootBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=300)

        self.assertIsNone(qcState.appendRootBlock(rB))

    def testSingleTx(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()

        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=100)
        tx = create_test_transaction(
            id1, qcState.getGenesisMinorBlock(0).txList[0].getHash(), acc2, 6000, 4000)
        b1.addTx(tx)
        b1.finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend(
            quarkash=200).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))

        rB = qcState.getGenesisRootBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=300)

        self.assertIsNone(qcState.appendRootBlock(rB))

    def testInShardTx(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        id2 = Identity.createRandomIdentity()
        acc2 = Address.createFromIdentity(id2, fullShardId=0)

        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=100)
        tx1 = create_test_transaction(
            id1, qcState.getGenesisMinorBlock(0).txList[0].getHash(), acc2, 6000, 4000)
        b1.addTx(tx1)
        b1.finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend(
            quarkash=200).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))

        # We could use in shard tx asap
        b3 = b1.createBlockToAppend(quarkash=200) \
            .addTx(create_test_transaction(id2, tx1.getHash(), acc1, 1500, 2500, shardId=0, outputIndex=1)) \
            .finalizeMerkleRoot()
        b4 = b2.createBlockToAppend(quarkash=300).finalizeMerkleRoot()

        self.assertIsNone(qcState.appendMinorBlock(b3))
        self.assertIsNone(qcState.appendMinorBlock(b4))

        rB = qcState.getGenesisRootBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b3.header, b2.header, b4.header]
        rB.finalize(quarkash=300)

        self.assertIsNone(qcState.appendRootBlock(rB))

    def testCrossShardTx(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        id2 = Identity.createRandomIdentity()
        acc2 = Address.createFromIdentity(id2, fullShardId=1)

        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=100)
        tx1 = create_test_transaction(
            id1, qcState.getGenesisMinorBlock(0).txList[0].getHash(), acc2, 6000, 4000, shardId=1)
        b1.addTx(tx1)
        b1.finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend(
            quarkash=200).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))

        # The output is not in the shard
        b3 = b1.createBlockToAppend(quarkash=200) \
            .addTx(create_test_transaction(id2, tx1.getHash(), acc1, 1500, 2500, shardId=0, outputIndex=1)) \
            .finalizeMerkleRoot()
        self.assertIsNotNone(qcState.appendMinorBlock(b3))

        # We cannot perform the cross tx until it is confirmed by root chain
        b4 = b2.createBlockToAppend(quarkash=200) \
            .addTx(create_test_transaction(id2, tx1.getHash(), acc1, 1500, 2500, shardId=0, outputIndex=1)) \
            .finalizeMerkleRoot()
        self.assertIsNotNone(qcState.appendMinorBlock(b4))

        rB = qcState.getGenesisRootBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=300)

        self.assertIsNone(qcState.appendRootBlock(rB))

        b4.header.hashPrevRootBlock = rB.header.getHash()
        self.assertIsNone(qcState.appendMinorBlock(b4))
