import asyncio
import unittest

from quarkchain.cluster.tests.test_utils import create_transfer_transaction, get_test_env
from quarkchain.cluster.utils import create_cluster_config
from quarkchain.cluster.slave import SlaveServer
from quarkchain.cluster.master import MasterServer, ClusterConfig
from quarkchain.cluster.root_state import RootState
from quarkchain.core import Address, Identity, ShardMask
from quarkchain.cluster.simple_network import SimpleNetwork
from quarkchain.utils import call_async


class Cluster:

    def __init__(self, master, slaveList, network, peer):
        self.master = master
        self.slaveList = slaveList
        self.network = network
        self.peer = peer


def create_test_clusters(numCluster, genesisAccount=Address.createEmptyAccount()):
    portStart = 38000
    seedPort = portStart
    clusterList = []
    loop = asyncio.get_event_loop()

    for i in range(numCluster):
        env = get_test_env(genesisAccount, genesisMinorQuarkash=1000000)

        p2pPort = portStart
        config = create_cluster_config(
            slaveCount=env.config.SHARD_SIZE,
            ip="127.0.0.1",
            p2pPort=p2pPort,
            clusterPortStart=portStart + 1,
        )
        portStart += (2 + env.config.SHARD_SIZE)

        env.config.P2P_SERVER_PORT = p2pPort
        env.config.P2P_SEED_PORT = seedPort
        env.clusterConfig.ID = 0
        env.clusterConfig.CONFIG = ClusterConfig(config)

        slaveServerList = []
        for slave in range(env.config.SHARD_SIZE):
            slaveEnv = env.copy()
            slaveEnv.clusterConfig.ID = config["slaves"][slave]["id"]
            slaveEnv.clusterConfig.NODE_PORT = config["slaves"][slave]["port"]
            slaveEnv.clusterConfig.SHARD_MASK_LIST = [ShardMask(v) for v in config["slaves"][slave]["shard_masks"]]
            slaveServer = SlaveServer(slaveEnv)
            slaveServer.start()
            slaveServerList.append(slaveServer)

        rootState = RootState(env, createGenesis=True)
        masterServer = MasterServer(env, rootState)
        masterServer.start()

        # Wait until the cluster is ready
        loop.run_until_complete(masterServer.clusterActiveFuture)

        # Start simple network and connect to seed host
        network = SimpleNetwork(env, rootState)
        network.startServer()
        if i != 0:
            peer = call_async(network.connect("127.0.0.1", seedPort))
        else:
            peer = None

        clusterList.append(Cluster(masterServer, slaveServerList, network, peer))

    return clusterList


def shutdown_clusters(clusterList):
    loop = asyncio.get_event_loop()

    for cluster in clusterList:
        # Shutdown simple network first
        cluster.network.shutdown()

        for slave in cluster.slaveList:
            slave.shutdown()
            loop.run_until_complete(slave.getShutdownFuture())

        cluster.master.shutdown()
        loop.run_until_complete(cluster.master.getShutdownFuture())


def sync_run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class TestCluster(unittest.TestCase):

    def testSingleCluster(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        clusters = create_test_clusters(1, acc1)
        shutdown_clusters(clusters)

    def testThreeClusters(self):
        clusters = create_test_clusters(3)
        shutdown_clusters(clusters)

    def testGetNextBlockToMine(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=0)
        acc3 = Address.createRandomAccount(fullShardId=1)

        clusters = create_test_clusters(1, acc1)
        master = clusters[0].master
        slaves = clusters[0].slaveList

        from quarkchain.evm import opcodes
        tx = create_transfer_transaction(
            shardState=slaves[0].shardStateMap[2 | 0],
            fromId=id1,
            toAddress=acc2,
            amount=12345,
            withdraw=54321,
            withdrawTo=bytes(acc3.serialize()),
            startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        self.assertTrue(slaves[0].addTx(tx))

        # Expect to mine shard 0 since it has one tx
        isRoot, block1 = sync_run(master.getNextBlockToMine(address=acc1))
        self.assertFalse(isRoot)
        self.assertEqual(block1.header.height, 1)
        self.assertEqual(block1.header.branch.value, 2 | 0)
        self.assertEqual(len(block1.txList), 1)

        self.assertTrue(sync_run(slaves[0].addBlock(block1)))
        self.assertEqual(slaves[0].shardStateMap[2].getBalance(acc2.recipient), 12345)
        self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

        # Expect to mine shard 1 due to proof-of-progress
        isRoot, block2 = sync_run(master.getNextBlockToMine(address=acc1.addressInShard(1)))
        self.assertFalse(isRoot)
        self.assertEqual(block2.header.height, 1)
        self.assertEqual(block2.header.branch.value, 2 | 1)
        self.assertEqual(len(block2.txList), 0)

        self.assertTrue(sync_run(slaves[1].addBlock(block2)))

        # Expect to mine root
        isRoot, block = sync_run(master.getNextBlockToMine(address=acc1.addressInShard(1)))
        self.assertTrue(isRoot)
        self.assertEqual(block.header.height, 1)
        self.assertEqual(len(block.minorBlockHeaderList), 2)
        self.assertEqual(block.minorBlockHeaderList[0], block1.header)
        self.assertEqual(block.minorBlockHeaderList[1], block2.header)

        self.assertTrue(master.rootState.addBlock(block))
        slaves[1].shardStateMap[3].addRootBlock(block)
        self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

        # Expect to mine shard 1 for the gas on xshard tx to acc3
        isRoot, block3 = sync_run(master.getNextBlockToMine(address=acc1.addressInShard(1)))
        self.assertFalse(isRoot)
        self.assertEqual(block3.header.height, 2)
        self.assertEqual(block3.header.branch.value, 2 | 1)
        self.assertEqual(len(block3.txList), 0)

        self.assertTrue(sync_run(slaves[1].addBlock(block3)))
        # Expect withdrawTo is included in acc3's balance
        self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 54321)

        shutdown_clusters(clusters)
