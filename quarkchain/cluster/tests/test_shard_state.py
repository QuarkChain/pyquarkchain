import unittest
from quarkchain.cluster.tests.test_utils import get_test_env, create_transfer_transaction
from quarkchain.cluster.shard_state import ShardState
from quarkchain.core import Identity, Address
from quarkchain.evm import opcodes
from quarkchain.cluster.core import CrossShardTransactionDeposit, CrossShardTransactionList


def create_default_shard_state(env, shardId=0):
    return ShardState(
        env=env,
        shardId=shardId,
        createGenesis=True)


class TestShardState(unittest.TestCase):

    def testShardStateSimple(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        self.assertEqual(state.rootTip.height, 0)
        self.assertEqual(state.headerTip.height, 0)

    def testShardStateTx(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.getTip().createBlockToAppend(address=acc3)
        b1.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=12345))

        evmState = state.runBlock(b1)
        b1.finalize(evmState=evmState)

        # Should succeed
        state.addBlock(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.getBalance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345)
        self.assertEqual(state.getBalance(acc2.recipient), 12345)
        self.assertEqual(state.getBalance(acc3.recipient), opcodes.GTXCOST // 2)

    def testShardStateTwoTx(self):
        id1 = Identity.createRandomIdentity()
        id2 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createFromIdentity(id2)
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.getTip().createBlockToAppend(address=acc3)
        b1.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=1234500))
        b1.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id2,
            toAddress=acc1,
            amount=234500))

        evmState = state.runBlock(b1)
        b1.finalize(evmState)

        # Should succeed
        state.addBlock(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.getBalance(id1.recipient), 10000000 - opcodes.GTXCOST - 1234500 + 234500)
        self.assertEqual(state.getBalance(acc2.recipient), 1234500 - 234500 - opcodes.GTXCOST)
        self.assertEqual(state.getBalance(acc3.recipient), opcodes.GTXCOST)

    def testShardStateXshardTxSent(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=1)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b1 = state.getTip().createBlockToAppend(address=acc3)
        evmTx = create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            withdraw=888888,
            withdrawTo=bytes(acc1.serialize()))

        b1.addTx(evmTx)
        evmState = state.runBlock(b1)
        b1.finalize(evmState)

        # Should succeed
        state.addBlock(b1)
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

    def testShardStateXshardTxSentWithIncorrectShardId(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)   # wrong full shard id
        acc2 = Address.createRandomAccount()

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b1 = state.getTip().createBlockToAppend()
        evmTx = create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=0,
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            withdraw=888888,
            withdrawTo=bytes(acc1.serialize()))
        b1.addTx(evmTx)

        with self.assertRaises(ValueError):
            state.runBlock(b1)

    def testShardStateXshardTxReceived(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount()
        acc3 = Address.createRandomAccount(fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env, shardId=0)
        state1 = create_default_shard_state(env=env, shardId=1)

        # Add one block in shard 0
        b0 = state0.getTip().createBlockToAppend()
        b0.finalize(evmState=state0.runBlock(b0))
        state0.addBlock(b0)

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
        b2 = state0.getTip().createBlockToAppend(address=acc3)
        b2.meta.hashPrevRootBlock = rB.header.getHash()
        b2.finalize(evmState=state0.runBlock(b2))
        state0.addBlock(b2)

        self.assertEqual(state0.getBalance(acc1.recipient), 10000000 + 888888)
        self.assertEqual(state0.getBalance(acc3.recipient), opcodes.GTXXSHARDCOST * 2 // 2)

    def testShardStateForkResolve(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env, shardId=0)

        b0 = state.getTip().createBlockToAppend()
        b1 = state.getTip().createBlockToAppend()

        b0.finalize(evmState=state.runBlock(b0))
        state.addBlock(b0)
        self.assertEqual(state.headerTip, b0.header)

        # Fork happens, first come first serve
        b1.finalize(evmState=state.runBlock(b0))
        state.addBlock(b1)
        self.assertEqual(state.headerTip, b0.header)

        # Longer fork happens, override existing one
        b2 = b1.createBlockToAppend()
        b2.finalize(evmState=state.runBlock(b1))
        state.addBlock(b2)
        self.assertEqual(state.headerTip, b2.header)

    def testShardStateRootChainFirstConsensus(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env, shardId=0)
        state1 = create_default_shard_state(env=env, shardId=1)

        # Add one block and prepare a fork
        b0 = state0.getTip().createBlockToAppend(address=acc1)
        b2 = state0.getTip().createBlockToAppend(address=Address.createEmptyAccount())

        b0.finalize(evmState=state0.runBlock(b0))
        state0.addBlock(b0)
        b2.finalize(evmState=state0.runBlock(b2))
        state0.addBlock(b2)

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
        b00.finalize(evmState=state0.runBlock(b00))
        state0.addBlock(b00)
        self.assertEqual(state0.headerTip, b00.header)

        # Create another fork that is much longer (however not confirmed by rB)
        b3 = b2.createBlockToAppend()
        b3.finalize(evmState=state0.runBlock(b3))
        state0.addBlock(b3)
        b4 = b3.createBlockToAppend()
        b4.finalize(evmState=state0.runBlock(b4))
        state0.addBlock(b4)
        self.assertGreater(b4.header.height, b00.header.height)
        self.assertEqual(state0.headerTip, b00.header)

    def testShardStateAddRootBlock(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state0 = create_default_shard_state(env=env, shardId=0)
        state1 = create_default_shard_state(env=env, shardId=1)

        # Add one block and prepare a fork
        b0 = state0.getTip().createBlockToAppend(address=acc1)
        b2 = state0.getTip().createBlockToAppend(address=Address.createEmptyAccount())

        b0.finalize(evmState=state0.runBlock(b0))
        state0.addBlock(b0)
        b2.finalize(evmState=state0.runBlock(b2))
        state0.addBlock(b2)

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
        b00.finalize(evmState=state0.runBlock(b00))
        state0.addBlock(b00)
        self.assertEqual(state0.headerTip, b00.header)

        # Create another fork that is much longer (however not confirmed by rB)
        b3 = b2.createBlockToAppend()
        b3.finalize(evmState=state0.runBlock(b3))
        state0.addBlock(b3)
        b4 = b1.createBlockToAppend()
        state0.addCrossShardTxListByMinorBlockHash(
            h=b4.header.getHash(),
            txList=CrossShardTransactionList(txList=[]))
        rB2 = rB1.createBlockToAppend() \
            .addMinorBlockHeader(b3.header) \
            .addMinorBlockHeader(b4.header) \
            .finalize()

        self.assertFalse(state0.addRootBlock(rB1))
        self.assertTrue(state0.addRootBlock(rB2))
        self.assertEqual(state0.headerTip, b3.header)
        self.assertEqual(state0.metaTip, b3.meta)
        self.assertEqual(state0.rootTip, rB2.header)
