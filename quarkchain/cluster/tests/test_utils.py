import asyncio
from contextlib import ContextDecorator

from quarkchain.cluster.master import MasterServer, ClusterConfig
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.simple_network import SimpleNetwork
from quarkchain.cluster.slave import SlaveServer
from quarkchain.cluster.utils import create_cluster_config
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Transaction, Code, ShardMask
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import call_async


def get_test_env(
        genesisAccount=Address.createEmptyAccount(),
        genesisQuarkash=0,
        genesisMinorQuarkash=0):
    env = DEFAULT_ENV.copy()
    env.config.setShardSize(2)
    env.config.SKIP_MINOR_DIFFICULTY_CHECK = True
    env.config.SKIP_ROOT_DIFFICULTY_CHECK = True
    env.config.SKIP_MINOR_COINBASE_CHECK = False
    env.config.GENESIS_ACCOUNT = genesisAccount
    env.config.GENESIS_COIN = genesisQuarkash
    env.config.GENESIS_MINOR_COIN = genesisMinorQuarkash
    env.config.TESTNET_MASTER_ACCOUNT = genesisAccount
    env.clusterConfig.MASTER_TO_SLAVE_CONNECT_RETRY_DELAY = 0.1
    return env


def create_transfer_transaction(
        shardState,
        fromId,
        toAddress,
        amount,
        startgas=21000,     # transfer tx min gas
        gasPrice=1,
        withdraw=0,
        withdrawTo=b''):
    """ Create an in-shard xfer tx
    """
    evmTx = EvmTransaction(
        branchValue=shardState.branch.value,
        nonce=shardState.getTransactionCount(fromId.getRecipient()),
        gasprice=gasPrice,
        startgas=startgas,
        to=toAddress.recipient,
        value=amount,
        data=b'',
        withdrawSign=1,
        withdraw=withdraw,
        withdrawTo=withdrawTo)
    evmTx.sign(
        key=fromId.getKey(),
        network_id=shardState.env.config.NETWORK_ID)
    return Transaction(
        inList=[],
        code=Code.createEvmCode(evmTx),
        outList=[])


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
        network = SimpleNetwork(env, masterServer)
        masterServer.network = network
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

        # Sleep 0.1 so that DESTROY_CLUSTER_PEER_ID command could be processed
        loop.run_until_complete(asyncio.sleep(0.1))

        for slave in cluster.slaveList:
            slave.shutdown()
            loop.run_until_complete(slave.getShutdownFuture())

        cluster.master.shutdown()
        loop.run_until_complete(cluster.master.getShutdownFuture())


class ClusterContext(ContextDecorator):

    def __init__(self, numCluster, genesisAccount=Address.createEmptyAccount()):
        self.numCluster = numCluster
        self.genesisAccount = genesisAccount

    def __enter__(self):
        self.clusterList = create_test_clusters(self.numCluster, self.genesisAccount)
        return self.clusterList

    def __exit__(self, *exc):
        shutdown_clusters(self.clusterList)
