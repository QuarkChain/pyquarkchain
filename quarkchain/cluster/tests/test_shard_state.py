import unittest
from quarkchain.cluster.tests.test_utils import get_test_env, create_transfer_transaction
from quarkchain.cluster.shard_state import ShardState
from quarkchain.core import Identity, Address


def create_default_shard_state(env):
    return ShardState(
        env=env,
        shardId=0,
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
        self.assertEqual(state.tip, b1.header)
