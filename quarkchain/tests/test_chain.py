import unittest
from quarkchain.chain import QuarkChain, ShardState, RootChain, QuarkChainState
from quarkchain.core import Address, Identity
from quarkchain.genesis import create_genesis_minor_block
from quarkchain.tests.test_utils import get_test_env, create_test_transaction
from collections import deque


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
            rB, [deque([b1.header]), deque([b2.header])]))

    def testProofOfProgress(self):
        env = get_test_env()
        self.assertEqual(env.config.PROOF_OF_PROGRESS_BLOCKS, 1)
        qChain = QuarkChain(env)

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header]
        rB.finalize()

        self.assertIsNotNone(qChain.rootChain.appendBlock(
            rB, [deque([b1.header]), deque([])]))

    def testNoProofOfProgress(self):
        env = get_test_env()
        env.config.PROOF_OF_PROGRESS_BLOCKS = 0
        qChain = QuarkChain(env)

        b1 = qChain.minorChainManager.getGenesisBlock(0).createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend()
        rB.minorBlockHeaderList = [b1.header]
        rB.finalize()

        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, [deque([b1.header]), deque([])]))

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
            rB, [deque([b1.header]), deque([b2.header])]))

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
            rB, [deque([b1.header, b3.header]), deque([b2.header])]))

        b4 = b3.createBlockToAppend()
        b5 = b2.createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b4))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b5))
        rB = rB.createBlockToAppend()
        rB.minorBlockHeaderList = [b4.header, b5.header]
        rB.finalize()
        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, [deque([b4.header]), deque([b5.header])]))

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
            rB, [deque([b1.header, b3.header]), deque([b2.header])]))

        b4 = b3.createBlockToAppend()
        b5 = b2.createBlockToAppend()
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b4))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b5))
        rB = rB.createBlockToAppend()
        rB.minorBlockHeaderList = [b4.header, b5.header]
        rB.finalize()
        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, [deque([b4.header]), deque([b5.header])]))

        qChain.rootChain.rollBack()

        self.assertIsNone(qChain.rootChain.appendBlock(
            rB, [deque([b4.header]), deque([b5.header])]))

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
            rB, [deque([b1.header]), deque([b2.header])]))

    def testQuarkChainTestnet(self):
        env = get_test_env()
        env.config.SKIP_MINOR_DIFFICULTY_CHECK = False
        env.config.SKIP_ROOT_DIFFICULTY_CHECK = False
        env.config.NETWORK_ID = 1
        qChain = QuarkChain(env)

        b1 = qChain.minorChainManager.getGenesisBlock(
            0).createBlockToAppend(address=env.config.TESTNET_MASTER_ACCOUNT)
        b2 = qChain.minorChainManager.getGenesisBlock(
            1).createBlockToAppend(address=env.config.TESTNET_MASTER_ACCOUNT)
        b1.header.coinbaseValue = 100
        b2.header.coinbaseValue = 200
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b1))
        self.assertIsNone(qChain.minorChainManager.addNewBlock(b2))

        rB = qChain.rootChain.getGenesisBlock().createBlockToAppend(
            address=env.config.TESTNET_MASTER_ACCOUNT)
        rB.minorBlockHeaderList = [b1.header, b2.header]
        rB.finalize(quarkash=b1.header.coinbaseValue + b2.header.coinbaseValue + 1)

        self.assertIsNotNone(qChain.rootChain.appendBlock(
            rB, [deque([b1.header]), deque([b2.header])]))


class TestShardState(unittest.TestCase):

    def testSimpleTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend()
        sState = ShardState(env, gBlock, rootChain)

        tx = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 6000, 4000)
        nBlock.addTx(tx)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.finalizeMerkleRoot()
        self.assertIsNone(sState.appendBlock(nBlock))

    def testMinorBlockCoinbase(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)

        env.config.SKIP_MINOR_COINBASE_CHECK = False
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend()
        sState = ShardState(env, gBlock, rootChain)

        tx = create_test_transaction(
            id1, gBlock.txList[0].getHash(), acc2, 6000, 3000)
        nBlock.addTx(tx)
        nBlock.header.hashPrevRootBlock = rootChain.getGenesisBlock().header.getHash()
        nBlock.txList[0].outList[0].quarkash = env.config.MINOR_BLOCK_DEFAULT_REWARD + 501
        nBlock.finalizeMerkleRoot()
        self.assertIsNotNone(sState.appendBlock(nBlock))

        nBlock.txList[0].outList[0].quarkash = env.config.MINOR_BLOCK_DEFAULT_REWARD + 500
        nBlock.finalizeMerkleRoot()
        self.assertIsNone(sState.appendBlock(nBlock))

    def testTwoTransactions(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)
        acc2 = Address.createRandomAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend()
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
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend()
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
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend(acc1, quarkash=5000)
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
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend()
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
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend()
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
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend()
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
        rootChain = RootChain(env)
        gBlock = create_genesis_minor_block(
            env, shardId=0, hashRootBlock=rootChain.getGenesisBlock().header.getHash())
        nBlock = gBlock.createBlockToAppend(address=acc1)
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

        b5 = b4.createBlockToAppend(quarkash=300).finalizeMerkleRoot()
        b5.header.hashPrevRootBlock = qcState.getGenesisRootBlock().header.getHash()
        self.assertIsNotNone(qcState.appendMinorBlock(b5))

        b5.header.hashPrevRootBlock = b4.header.hashPrevRootBlock
        self.assertIsNone(qcState.appendMinorBlock(b5))

    def testRollBack(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        id2 = Identity.createRandomIdentity()
        acc2 = Address.createFromIdentity(id2, fullShardId=1)

        env = get_test_env(acc1, genesisQuarkash=10000,
                           genesisMinorQuarkash=10000)
        qcState = QuarkChainState(env)
        self.assertEqual(qcState.getBalance(id1.recipient), 30000)
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=100)
        tx1 = create_test_transaction(
            id1, qcState.getGenesisMinorBlock(0).txList[0].getHash(), acc2, amount=6000, remaining=4000, shardId=0)
        b1.addTx(tx1)
        b1.finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend(
            quarkash=200).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))

        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        # Cross-shard TX is not confirmed
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

        rB = qcState.getGenesisRootBlock()                      \
            .createBlockToAppend()                              \
            .extendMinorBlockHeaderList([b1.header, b2.header]) \
            .finalize(quarkash=300)

        self.assertIsNone(qcState.appendRootBlock(rB))
        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        self.assertEqual(qcState.getBalance(id2.recipient), 6000)

        b4 = b2.createBlockToAppend(quarkash=200) \
            .addTx(create_test_transaction(id2, tx1.getHash(), acc1, 1500, 4500, shardId=0, outputIndex=1)) \
            .finalizeMerkleRoot()

        b4.header.hashPrevRootBlock = rB.header.getHash()
        self.assertIsNone(qcState.appendMinorBlock(b4))

        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

        self.assertIsNotNone(qcState.rollBackRootBlock())
        self.assertIsNotNone(qcState.rollBackMinorBlock(0))

        self.assertIsNone(qcState.rollBackMinorBlock(1))   # b4

        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        self.assertEqual(qcState.getBalance(id2.recipient), 6000)

        self.assertIsNone(qcState.rollBackRootBlock())

        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

        self.assertIsNone(qcState.rollBackMinorBlock(0))

        self.assertEqual(qcState.getBalance(id1.recipient), 30000)
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

    def testRollBackRootBlockTo(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        id2 = Identity.createRandomIdentity()
        acc2 = Address.createFromIdentity(id2, fullShardId=1)

        env = get_test_env(acc1, genesisQuarkash=10000,
                           genesisMinorQuarkash=10000)
        qcState = QuarkChainState(env)
        self.assertEqual(qcState.getBalance(id1.recipient), 30000)
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=100)
        tx1 = create_test_transaction(
            id1, qcState.getGenesisMinorBlock(0).txList[0].getHash(), acc2, amount=6000, remaining=4000, shardId=0)
        b1.addTx(tx1)
        b1.finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend(
            quarkash=200).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))

        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        # Cross-shard TX is not confirmed
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

        rB = qcState.getGenesisRootBlock()                      \
            .createBlockToAppend()                              \
            .extendMinorBlockHeaderList([b1.header, b2.header]) \
            .finalize(quarkash=300)

        self.assertIsNone(qcState.appendRootBlock(rB))
        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        self.assertEqual(qcState.getBalance(id2.recipient), 6000)

        b3 = b1.createBlockToAppend(quarkash=100, address=acc1).finalizeMerkleRoot()
        b4 = b2.createBlockToAppend(quarkash=200) \
            .addTx(create_test_transaction(id2, tx1.getHash(), acc1, 1500, 4500, shardId=0, outputIndex=1)) \
            .finalizeMerkleRoot()

        b4.header.hashPrevRootBlock = rB.header.getHash()
        self.assertIsNone(qcState.appendMinorBlock(b3))
        self.assertIsNone(qcState.appendMinorBlock(b4))

        self.assertEqual(qcState.getBalance(id1.recipient), 24100)
        self.assertEqual(qcState.getBalance(id2.recipient), 0)

        rB1 = rB.createBlockToAppend().extendMinorBlockHeaderList([b3.header, b4.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB1))

        self.assertEqual(qcState.getBalance(id1.recipient), 25600)
        self.assertEqual(qcState.getBalance(id2.recipient), 4500)

        self.assertIsNone(qcState.rollBackRootChainTo(qcState.getGenesisRootBlock().header))
        self.assertEqual(qcState.getBalance(id1.recipient), 24000)
        self.assertEqual(qcState.getBalance(id2.recipient), 0)
        self.assertEqual(qcState.getShardTip(0), b1.header)
        self.assertEqual(qcState.getShardTip(1), b2.header)

    def testMinorCoinbaseOutputMustInShard(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        acc1 = Address.createRandomAccount(fullShardId=1)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100, address=acc1).finalizeMerkleRoot()
        b1.txList[0].outList[0].address = acc1
        self.assertIsNotNone(qcState.appendMinorBlock(b1))

        b1.txList[0].outList[0].address = acc1.addressInShard(0)
        self.assertIsNone(qcState.appendMinorBlock(b1))

    def testCreateBlockToMine(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createEmptyAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)

        qcState = QuarkChainState(env)
        tx1 = create_test_transaction(
            id1,
            qcState.getGenesisMinorBlock(0).txList[0].getHash(),
            acc2,
            6000,
            3000)
        tx2 = create_test_transaction(id1, tx1.getHash(), acc2, 1000, 1000)
        qcState.addTransactionToQueue(0, tx1)
        qcState.addTransactionToQueue(0, tx2)

        b1 = qcState.createMinorBlockToMine(0, address=acc3)
        self.assertEqual(len(b1.txList), 3)
        self.assertEqual(qcState.getBalance(acc3.recipient), 0)
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertEqual(qcState.getBalance(acc3.recipient), env.config.MINOR_BLOCK_DEFAULT_REWARD + 2000)

    def testCreateBlockToMineWithConflictingTxs(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createEmptyAccount()
        env = get_test_env(acc1, genesisMinorQuarkash=10000)

        qcState = QuarkChainState(env)
        tx1 = create_test_transaction(
            id1,
            qcState.getGenesisMinorBlock(0).txList[0].getHash(),
            acc2,
            6000,
            3000)
        tx2 = create_test_transaction(
            id1,
            qcState.getGenesisMinorBlock(0).txList[0].getHash(),
            acc2,
            4000,
            5500)
        qcState.addTransactionToQueue(0, tx1)
        qcState.addTransactionToQueue(0, tx2)

        b1 = qcState.createMinorBlockToMine(0)
        self.assertEqual(len(b1.txList), 2)
        self.assertEqual(qcState.getBalance(acc3.recipient), 0)
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertEqual(qcState.getBalance(acc3.recipient), env.config.MINOR_BLOCK_DEFAULT_REWARD + 1000)
