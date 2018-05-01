import asyncio
import argparse
import unittest

from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.cluster.utils import create_cluster_config
from quarkchain.cluster.slave import SlaveServer
from quarkchain.cluster.master import MasterServer, ClusterConfig
from quarkchain.cluster.root_state import RootState
from quarkchain.core import ShardMask

port_start = 38000


def create_test_clusters(numCluster):
    global port_start
    seedPort = None
    clusterList = []
    loop = asyncio.get_event_loop()

    for i in range(numCluster):
        env = get_test_env()
        args = argparse.Namespace
        args.num_slaves = env.config.SHARD_SIZE
        args.p2p_port = port_start
        args.port_start = port_start + 1
        args.ip = "127.0.0.1"
        args.db_prefix = ""   # not used

        config = create_cluster_config(args)
        port_start += (1 + env.config.SHARD_SIZE)

        env.config.P2P_SERVER_PORT = args.p2p_port
        if seedPort is None:
            seedPort = env.config.P2P_SERVER_PORT
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
        clusters = create_test_clusters(1)
        shutdown_clusters(clusters)
