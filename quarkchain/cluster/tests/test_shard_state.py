import unittest
from quarkchain.cluster.core import RootBlock, RootBlockHeader
from quarkchain.cluster.tests.test_utils import get_test_env, create_transfer_transaction
from quarkchain.cluster.shard_state import ShardState
from quarkchain.core import Identity, Address
from quarkchain.evm import opcodes
from quarkchain.cluster.core import CrossShardTransactionDeposit, CrossShardTransactionList
from quarkchain.diff import EthDifficultyCalculator


def create_default_shard_state(env, shardId=0):
    shardState = ShardState(
        env=env,
        shardId=shardId,
    )
    return shardState


class TestShardState(unittest.TestCase):

    def testShardStateSimple(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        self.assertEqual(state.rootTip.height, 1)
        self.assertEqual(state.headerTip.height, 1)

    def testOneTx(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=12345)
        state.addTx(tx)

        block, i = state.getTransactionByHash(tx.getHash())
        self.assertEqual(block.txList[0], tx)
        self.assertEqual(block.header.createTime, 0)
        self.assertEqual(i, 0)

        b1 = state.createBlockToMine(address=acc3)
        self.assertEqual(len(b1.txList), 1)

        # Should succeed
        state.finalizeAndAddBlock(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.getBalance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345)
        self.assertEqual(state.getBalance(acc2.recipient), 12345)
        self.assertEqual(state.getBalance(acc3.recipient), opcodes.GTXCOST // 2)

        block, i = state.getTransactionByHash(tx.getHash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

    def testDuplicatedTx(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=12345)
        self.assertTrue(state.addTx(tx))
        self.assertFalse(state.addTx(tx))

        self.assertEqual(len(state.txQueue), 1)

        block, i = state.getTransactionByHash(tx.getHash())
        self.assertEqual(len(block.txList), 1)
        self.assertEqual(block.txList[0], tx)
        self.assertEqual(block.header.createTime, 0)
        self.assertEqual(i, 0)

        b1 = state.createBlockToMine(address=acc3)
        self.assertEqual(len(b1.txList), 1)

        # Should succeed
        state.finalizeAndAddBlock(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.getBalance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345)
        self.assertEqual(state.getBalance(acc2.recipient), 12345)
        self.assertEqual(state.getBalance(acc3.recipient), opcodes.GTXCOST // 2)

        block, i = state.getTransactionByHash(tx.getHash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        # nonce is old
        self.assertFalse(state.addTx(tx))

    def testTwoTx(self):
        id1 = Identity.createRandomIdentity()
        id2 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createFromIdentity(id2)
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        state.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=1234500))
        state.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id2,
            toAddress=acc1,
            amount=234500))
        b1 = state.createBlockToMine(address=acc3)
        self.assertEqual(len(b1.txList), 2)

        # Should succeed
        state.finalizeAndAddBlock(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.getBalance(id1.recipient), 10000000 - opcodes.GTXCOST - 1234500 + 234500)
        self.assertEqual(state.getBalance(acc2.recipient), 1234500 - 234500 - opcodes.GTXCOST)
        self.assertEqual(state.getBalance(acc3.recipient), opcodes.GTXCOST)

        block, i = state.getTransactionByHash(b1.txList[0].getHash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        block, i = state.getTransactionByHash(b1.txList[1].getHash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 1)

    def testStaleBlockCount(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.createBlockToMine(address=acc3)
        b2 = state.createBlockToMine(address=acc3)
        b2.header.createTime += 1

        state.finalizeAndAddBlock(b1)
        self.assertEqual(state.db.getBlockCountByHeight(2), 1)

        state.finalizeAndAddBlock(b2)
        self.assertEqual(state.db.getBlockCountByHeight(2), 2)

    def testXshardTxSent(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=1)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        state.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            withdraw=888888,
            withdrawTo=bytes(acc1.serialize())))

        b1 = state.createBlockToMine(address=acc3)
        self.assertEqual(len(b1.txList), 1)

        self.assertEqual(state.evmState.gas_used, 0)
        # Should succeed
        state.finalizeAndAddBlock(b1)
        self.assertEqual(len(state.evmState.xshard_list), 1)
        self.assertEqual(
            state.evmState.xshard_list[0],
            CrossShardTransactionDeposit(
                address=acc1,
                amount=888888,
                gasPrice=1))
        self.assertEqual(state.getBalance(id1.recipient), 10000000 - 888888 - opcodes.GTXCOST - opcodes.GTXXSHARDCOST)
        # Make sure the xshard gas is not used by local block
        self.assertEqual(state.evmState.gas_used, opcodes.GTXCOST)
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(state.getBalance(acc3.recipient), opcodes.GTXCOST // 2)

    def testAddBlockXshardTxSentWithIncorrectShardId(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)   # wrong full shard id
        acc2 = Address.createRandomAccount()

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b1 = state.getTip().createBlockToAppend()
        b1.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            withdraw=888888,
            withdrawTo=bytes(acc1.serialize())))

        with self.assertRaises(ValueError):
            state.runBlock(b1)

    def testCreateBlockToMineXshardTxSentWithIncorrectShardId(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)   # wrong full shard id
        acc2 = Address.createRandomAccount()

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        state.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            withdraw=888888,
            withdrawTo=bytes(acc1.serialize())))
        b1 = state.createBlockToMine(address=acc1)
        self.assertEqual(len(b1.txList), 0)
        self.assertEqual(len(state.txQueue), 0)

    def testXshardTxReceived(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block in shard 0
        b0 = state0.createBlockToMine()
        state0.finalizeAndAddBlock(b0)

        b1 = state1.getTip().createBlockToAppend()
        evmTx = create_transfer_transaction(
            shardState=state1,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gasPrice=2,
            withdraw=888888,
            withdrawTo=bytes(acc1.serialize()))
        b1.addTx(evmTx)

        # Add a x-shard tx from remote peer
        state0.addCrossShardTxListByMinorBlockHash(
            h=b1.header.getHash(),
            txList=CrossShardTransactionList(txList=[
                CrossShardTransactionDeposit(
                    address=acc1,
                    amount=888888,
                    gasPrice=2)
            ]))

        # Create a root block containing the block with the x-shard tx
        rB = state0.rootTip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()
        state0.addRootBlock(rB)

        # Add b0 and make sure all x-shard tx's are added
        b2 = state0.createBlockToMine(address=acc3)
        state0.finalizeAndAddBlock(b2)

        self.assertEqual(state0.getBalance(acc1.recipient), 10000000 + 888888)
        # Half collected by root
        self.assertEqual(state0.getBalance(acc3.recipient), opcodes.GTXXSHARDCOST * 2 // 2)

    def testXshardForTwoRootBlocks(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block in shard 0
        b0 = state0.createBlockToMine()
        state0.finalizeAndAddBlock(b0)

        b1 = state1.getTip().createBlockToAppend()
        evmTx = create_transfer_transaction(
            shardState=state1,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gasPrice=2,
            withdraw=888888,
            withdrawTo=bytes(acc1.serialize()))
        b1.addTx(evmTx)

        # Add a x-shard tx from remote peer
        state0.addCrossShardTxListByMinorBlockHash(
            h=b1.header.getHash(),
            txList=CrossShardTransactionList(txList=[
                CrossShardTransactionDeposit(
                    address=acc1,
                    amount=888888,
                    gasPrice=2)
            ]))

        # Create a root block containing the block with the x-shard tx
        rB0 = state0.rootTip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()
        state0.addRootBlock(rB0)

        b2 = state0.getTip().createBlockToAppend()
        state0.finalizeAndAddBlock(b2)

        b3 = b1.createBlockToAppend()
        evmTx = create_transfer_transaction(
            shardState=state1,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gasPrice=3,
            withdraw=385723,
            withdrawTo=bytes(acc1.serialize()))

        # Add a x-shard tx from remote peer
        state0.addCrossShardTxListByMinorBlockHash(
            h=b3.header.getHash(),
            txList=CrossShardTransactionList(txList=[
                CrossShardTransactionDeposit(
                    address=acc1,
                    amount=385723,
                    gasPrice=3)
            ]))

        rB1 = state0.rootTip.createBlockToAppend() \
            .addMinorBlockHeader(b2.header) \
            .addMinorBlockHeader(b3.header) \
            .finalize()
        state0.addRootBlock(rB1)

        # Test x-shard gas limit when createBlockToMine
        b5 = state0.createBlockToMine(address=acc3, gasLimit=0)
        # Current algorithm allows at least one root block to be included
        self.assertEqual(b5.header.hashPrevRootBlock, rB0.header.getHash())
        b6 = state0.createBlockToMine(address=acc3, gasLimit=opcodes.GTXXSHARDCOST)
        self.assertEqual(b6.header.hashPrevRootBlock, rB0.header.getHash())
        # There are two x-shard txs: one is root block coinbase with zero gas, and anonther is from shard 1
        b7 = state0.createBlockToMine(address=acc3, gasLimit=2 * opcodes.GTXXSHARDCOST)
        self.assertEqual(b7.header.hashPrevRootBlock, rB1.header.getHash())
        b8 = state0.createBlockToMine(address=acc3, gasLimit=3 * opcodes.GTXXSHARDCOST)
        self.assertEqual(b8.header.hashPrevRootBlock, rB1.header.getHash())

        # Add b0 and make sure all x-shard tx's are added
        b4 = state0.createBlockToMine(address=acc3)
        self.assertEqual(b4.header.hashPrevRootBlock, rB1.header.getHash())
        state0.finalizeAndAddBlock(b4)

        self.assertEqual(state0.getBalance(acc1.recipient), 10000000 + 888888 + 385723)
        # Half collected by root
        self.assertEqual(state0.getBalance(acc3.recipient), opcodes.GTXXSHARDCOST * (2 + 3) // 2)

    def testForkResolve(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b0 = state.getTip().createBlockToAppend()
        b1 = state.getTip().createBlockToAppend()

        state.finalizeAndAddBlock(b0)
        self.assertEqual(state.headerTip, b0.header)

        # Fork happens, first come first serve
        state.finalizeAndAddBlock(b1)
        self.assertEqual(state.headerTip, b0.header)

        # Longer fork happens, override existing one
        b2 = b1.createBlockToAppend()
        state.finalizeAndAddBlock(b2)
        self.assertEqual(state.headerTip, b2.header)

    def testRootChainFirstConsensus(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block and prepare a fork
        b0 = state0.getTip().createBlockToAppend(address=acc1)
        b2 = state0.getTip().createBlockToAppend(address=Address.createEmptyAccount())

        state0.finalizeAndAddBlock(b0)
        state0.finalizeAndAddBlock(b2)

        b1 = state1.getTip().createBlockToAppend()
        b1.finalize(evmState=state1.runBlock(b1))

        # Create a root block containing the block with the x-shard tx
        state0.addCrossShardTxListByMinorBlockHash(
            h=b1.header.getHash(),
            txList=CrossShardTransactionList(txList=[]))
        rB = state0.rootTip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()
        state0.addRootBlock(rB)

        b00 = b0.createBlockToAppend()
        state0.finalizeAndAddBlock(b00)
        self.assertEqual(state0.headerTip, b00.header)

        # Create another fork that is much longer (however not confirmed by rB)
        b3 = b2.createBlockToAppend()
        state0.finalizeAndAddBlock(b3)
        b4 = b3.createBlockToAppend()
        state0.finalizeAndAddBlock(b4)
        self.assertGreater(b4.header.height, b00.header.height)
        self.assertEqual(state0.headerTip, b00.header)

    def testShardStateAddRootBlock(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env0 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        env1 = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env0, shardId=0)
        state1 = create_default_shard_state(env=env1, shardId=1)

        # Add one block and prepare a fork
        b0 = state0.getTip().createBlockToAppend(address=acc1)
        b2 = state0.getTip().createBlockToAppend(address=Address.createEmptyAccount())

        state0.finalizeAndAddBlock(b0)
        state0.finalizeAndAddBlock(b2)

        b1 = state1.getTip().createBlockToAppend()
        b1.finalize(evmState=state1.runBlock(b1))

        # Create a root block containing the block with the x-shard tx
        state0.addCrossShardTxListByMinorBlockHash(
            h=b1.header.getHash(),
            txList=CrossShardTransactionList(txList=[]))
        rB = state0.rootTip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()
        rB1 = state0.rootTip.createBlockToAppend() \
            .addMinorBlockHeader(b2.header) \
            .addMinorBlockHeader(b1.header) \
            .finalize()

        state0.addRootBlock(rB)

        b00 = b0.createBlockToAppend()
        state0.finalizeAndAddBlock(b00)
        self.assertEqual(state0.headerTip, b00.header)

        # Create another fork that is much longer (however not confirmed by rB)
        b3 = b2.createBlockToAppend()
        state0.finalizeAndAddBlock(b3)
        b4 = b3.createBlockToAppend()
        state0.finalizeAndAddBlock(b4)
        self.assertEqual(state0.headerTip, b00.header)
        self.assertEqual(state0.db.getMinorBlockByHeight(3), b00)
        self.assertIsNone(state0.db.getMinorBlockByHeight(4))

        b5 = b1.createBlockToAppend()
        state0.addCrossShardTxListByMinorBlockHash(
            h=b5.header.getHash(),
            txList=CrossShardTransactionList(txList=[]))
        rB2 = rB1.createBlockToAppend() \
            .addMinorBlockHeader(b3.header) \
            .addMinorBlockHeader(b4.header) \
            .addMinorBlockHeader(b5.header) \
            .finalize()

        self.assertFalse(state0.addRootBlock(rB1))
        self.assertTrue(state0.addRootBlock(rB2))
        self.assertEqual(state0.headerTip, b4.header)
        self.assertEqual(state0.metaTip, b4.meta)
        self.assertEqual(state0.rootTip, rB2.header)

        self.assertEqual(state0.db.getMinorBlockByHeight(3), b3)
        self.assertEqual(state0.db.getMinorBlockByHeight(4), b4)

    def testShardStateForkResolveWithHigherRootChain(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b0 = state.getTip().createBlockToAppend()
        state.finalizeAndAddBlock(b0)
        rB = state.rootTip.createBlockToAppend() \
            .addMinorBlockHeader(b0.header) \
            .finalize()

        self.assertEqual(state.headerTip, b0.header)
        self.assertTrue(state.addRootBlock(rB))

        b1 = state.getTip().createBlockToAppend()
        b2 = state.getTip().createBlockToAppend(nonce=1)
        b2.header.hashPrevRootBlock = rB.header.getHash()
        b3 = state.getTip().createBlockToAppend(nonce=2)
        b3.header.hashPrevRootBlock = rB.header.getHash()

        state.finalizeAndAddBlock(b1)
        self.assertEqual(state.headerTip, b1.header)

        # Fork happens, although they have the same height, b2 survives since it confirms root block
        state.finalizeAndAddBlock(b2)
        self.assertEqual(state.headerTip, b2.header)

        # b3 confirms the same root block as b2, so it will not override b2
        state.finalizeAndAddBlock(b3)
        self.assertEqual(state.headerTip, b2.header)

    def testShardStateDifficulty(self):
        env = get_test_env()
        env.config.GENESIS_MINOR_DIFFICULTY = 10000
        env.config.SKIP_MINOR_DIFFICULTY_CHECK = False
        env.config.MINOR_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=9,
            diffFactor=2048,
            minimumDiff=1)
        env.config.NETWORK_ID = 1  # other network ids will skip difficulty check
        state = create_default_shard_state(env=env, shardId=0)

        # Check new difficulty
        b0 = state.createBlockToMine(state.headerTip.createTime + 8)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty // 2048 + state.headerTip.difficulty)
        b0 = state.createBlockToMine(state.headerTip.createTime + 9)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty)
        b0 = state.createBlockToMine(state.headerTip.createTime + 17)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty)
        b0 = state.createBlockToMine(state.headerTip.createTime + 24)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty - state.headerTip.difficulty // 2048)
        b0 = state.createBlockToMine(state.headerTip.createTime + 35)
        self.assertEqual(b0.header.difficulty, state.headerTip.difficulty - state.headerTip.difficulty // 2048 * 2)

        for i in range(0, 2 ** 32):
            b0.header.nonce = i
            if int.from_bytes(b0.header.getHash(), byteorder="big") * env.config.GENESIS_MINOR_DIFFICULTY < 2 ** 256:
                self.assertEqual(state.addBlock(b0), [])
                break
            else:
                with self.assertRaises(ValueError):
                    state.addBlock(b0)

    def testShardStateRecoveryFromRootBlock(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        blockHeaders = []
        blockMetas = []
        for i in range(12):
            b = state.getTip().createBlockToAppend(address=acc1)
            state.finalizeAndAddBlock(b)
            blockHeaders.append(b.header)
            blockMetas.append(b.meta)

        # add a fork
        b1 = state.db.getMinorBlockByHeight(3)
        b1.header.createTime += 1
        state.finalizeAndAddBlock(b1)
        self.assertEqual(state.db.getMinorBlockByHash(b1.header.getHash()), b1)

        rB = state.rootTip.createBlockToAppend()
        rB.minorBlockHeaderList = blockHeaders[:5]
        rB.finalize()

        state.addRootBlock(rB)

        rB1 = state.rootTip.createBlockToAppend()
        rB1.minorBlockHeaderList = blockHeaders[5:11]
        rB1.finalize()

        # Note that rB1 has not been added to the shard state

        recoveredState = ShardState(env=env, shardId=0)
        self.assertEqual(recoveredState.db.getMinorBlockByHash(b1.header.getHash()), b1)

        recoveredState.initFromRootBlock(rB)
        # forks are pruned
        self.assertIsNone(recoveredState.db.getMinorBlockByHash(b1.header.getHash()))

        self.assertEqual(recoveredState.rootTip, rB.header)
        self.assertEqual(recoveredState.headerTip, blockHeaders[4])
        self.assertEqual(recoveredState.confirmedHeaderTip, blockHeaders[4])
        self.assertEqual(recoveredState.metaTip, blockMetas[4])
        self.assertEqual(recoveredState.confirmedMetaTip, blockMetas[4])
        self.assertEqual(recoveredState.evmState.trie.root_hash, blockMetas[4].hashEvmStateRoot)

        recoveredState = ShardState(env=env, shardId=0)
        recoveredState.initFromRootBlock(rB1)

        self.assertEqual(recoveredState.rootTip, rB1.header)
        self.assertEqual(recoveredState.headerTip, blockHeaders[10])
        self.assertEqual(recoveredState.confirmedHeaderTip, blockHeaders[10])
        self.assertEqual(recoveredState.metaTip, blockMetas[10])
        self.assertEqual(recoveredState.confirmedMetaTip, blockMetas[10])
        self.assertEqual(recoveredState.evmState.trie.root_hash, blockMetas[10].hashEvmStateRoot)

    def testNotUpdateTipOnRootFork(self):
        ''' block's hashPrevRootBlock must be on the same chain with rootTip to update tip.

                 +--+
              a. |r1|
                /+--+
               /   |
        +--+  /  +--+    +--+
        |r0|<----|m1|<---|m2| c.
        +--+  \  +--+    +--+
               \   |      |
                \+--+     |
              b. |r2|<----+
                 +--+

        Initial state: r0 <- m1
        Then adding r1, r2, m2 should not make m2 the tip because r1 is the root tip and r2 and r1
        are not on the same root chain.
        '''
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        m1 = state.getTip().createBlockToAppend(address=acc1)
        state.finalizeAndAddBlock(m1)

        r1 = state.rootTip.createBlockToAppend()
        r2 = state.rootTip.createBlockToAppend()
        r1.minorBlockHeaderList.append(m1.header)
        r1.finalize()

        state.addRootBlock(r1)

        r2.minorBlockHeaderList.append(m1.header)
        r2.header.createTime = r1.header.createTime + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.getHash(), r2.header.getHash())

        state.addRootBlock(r2)

        self.assertEqual(state.rootTip, r1.header)

        m2 = m1.createBlockToAppend(address=acc1)
        m2.header.hashPrevRootBlock = r2.header.getHash()

        state.finalizeAndAddBlock(m2)
        # m2 is added
        self.assertEqual(state.db.getMinorBlockByHash(m2.header.getHash()), m2)
        # but m1 should still be the tip
        self.assertEqual(state.headerTip, m1.header)

    def testAddRootBlockRevertHeaderTip(self):
        ''' block's hashPrevRootBlock must be on the same chain with rootTip to update tip.

                 +--+
                 |r1|<-------------+
                /+--+              |
               /   |               |
        +--+  /  +--+    +--+     +--+
        |r0|<----|m1|<---|m2| <---|m3|
        +--+  \  +--+    +--+     +--+
               \   |       \
                \+--+.     +--+
                 |r2|<-----|r3| (r3 includes m2)
                 +--+      +--+

        Initial state: r0 <- m1 <- m2
        Adding r1, r2, m3 makes r1 the rootTip, m3 the headerTip
        Adding r3 should change the rootTip to r3, headerTip to m2
        '''
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        m1 = state.getTip().createBlockToAppend(address=acc1)
        state.finalizeAndAddBlock(m1)

        m2 = state.getTip().createBlockToAppend(address=acc1)
        state.finalizeAndAddBlock(m2)

        r1 = state.rootTip.createBlockToAppend()
        r2 = state.rootTip.createBlockToAppend()
        r1.minorBlockHeaderList.append(m1.header)
        r1.finalize()

        state.addRootBlock(r1)

        r2.minorBlockHeaderList.append(m1.header)
        r2.header.createTime = r1.header.createTime + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.getHash(), r2.header.getHash())

        state.addRootBlock(r2)

        self.assertEqual(state.rootTip, r1.header)

        m3 = state.createBlockToMine(address=acc1)
        self.assertEqual(m3.header.hashPrevRootBlock, r1.header.getHash())
        state.finalizeAndAddBlock(m3)

        r3 = r2.createBlockToAppend(address=acc1)
        r3.addMinorBlockHeader(m2.header)
        r3.finalize()
        state.addRootBlock(r3)
        self.assertEqual(state.rootTip, r3.header)
        self.assertEqual(state.headerTip, m2.header)
