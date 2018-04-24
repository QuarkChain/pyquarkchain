import unittest
from quarkchain.cluster.tests.test_utils import get_test_env, create_transfer_transaction
from quarkchain.cluster.shard_state import ShardState
from quarkchain.core import Identity, Address
from quarkchain.evm import opcodes
from quarkchain.cluster.core import CrossShardTransactionDeposit


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

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.getTip().createBlockToAppend()
        b1.addTx(create_transfer_transaction(
            shardState=state,
            fromId=id1,
            toAddress=acc2,
            amount=12345))

        evmState = state.runBlock(b1)
        b1.finalize(evmState)

        # Should succeed
        state.addBlock(b1)
        self.assertEqual(state.headerTip, b1.header)
        self.assertEqual(state.getBalance(id1.recipient), 10000000 - opcodes.GTXCOST - 12345)
        self.assertEqual(state.getBalance(acc2.recipient), 12345)

    def testShardStateTwoTx(self):
        id1 = Identity.createRandomIdentity()
        id2 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1)
        acc2 = Address.createFromIdentity(id2)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.getTip().createBlockToAppend()
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

    def testShardStateXshardTxSent(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=1)
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
