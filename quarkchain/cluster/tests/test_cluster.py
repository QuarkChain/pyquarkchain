import asyncio
import unittest

from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.cluster.utils import create_cluster_config
from quarkchain.cluster.slave import SlaveServer
from quarkchain.cluster.master import MasterServer, ClusterConfig
from quarkchain.cluster.root_state import RootState
from quarkchain.core import ShardMask


def create_test_clusters(numCluster):
    portStart = 38000
    seedPort = portStart
    clusterList = []
    loop = asyncio.get_event_loop()

    for i in range(numCluster):
        env = get_test_env()

        p2pPort = portStart
        config = create_cluster_config(
            slaveCount=env.config.SHARD_SIZE,
            ip="127.0.0.1",
            p2pPort=p2pPort,
            clusterPortStart=portStart + 1,
        )
        portStart += (1 + env.config.SHARD_SIZE)

        env.config.P2P_SERVER_PORT = p2pPort
        env.config.P2P_SEED_PORT = seedPort
        env.clusterConfig.ID = 0
        env.clusterConfig.CONFIG = ClusterConfig(config)

        slaveServerList = []
        for slave in range(env.config.SHARD_SIZE):
            slaveEnv = get_test_env()
            slaveEnv.clusterConfig.ID = config["slaves"][slave]["id"]
            slaveEnv.clusterConfig.NODE_PORT = config["slaves"][slave]["port"]
            slaveEnv.clusterConfig.SHARD_MASK_LIST = [ShardMask(v) for v in config["slaves"][slave]["shard_masks"]]
            slaveServer = SlaveServer(slaveEnv)
            slaveServer.start()
            slaveServerList.append(slaveServer)

        masterServer = MasterServer(env, RootState(env, createGenesis=True))
        masterServer.start()

        # Wait until the cluster is ready
        loop.run_until_complete(masterServer.clusterActiveFuture)

        clusterList.append((masterServer, slaveServerList))

    return clusterList


def shutdown_clusters(clusterList):
    loop = asyncio.get_event_loop()

    for cluster in clusterList:
        master, slaveList = cluster

        for slave in slaveList:
            slave.shutdown()
            loop.run_until_complete(slave.getShutdownFuture())

        master.shutdown()
        loop.run_until_complete(master.getShutdownFuture())


class TestCluster(unittest.TestCase):

    def testSingleCluster(self):
        clusters = create_test_clusters(4)
        shutdown_clusters(clusters)
