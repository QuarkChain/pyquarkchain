import asyncio
from contextlib import ContextDecorator

from quarkchain.cluster.master import MasterServer, ClusterConfig
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.simple_network import SimpleNetwork
from quarkchain.cluster.slave import SlaveServer
from quarkchain.cluster.utils import create_cluster_config
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Transaction, Code, ShardMask
from quarkchain.db import InMemoryDb
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import call_async, check
from quarkchain.protocol import AbstractConnection


def get_test_env(
        genesisAccount=Address.createEmptyAccount(),
        genesisQuarkash=0,
        genesisMinorQuarkash=0,
        shardSize=2):
    env = DEFAULT_ENV.copy()
    env.config.setShardSize(shardSize)
    env.config.SKIP_MINOR_DIFFICULTY_CHECK = True
    env.config.SKIP_ROOT_DIFFICULTY_CHECK = True
    env.config.SKIP_MINOR_COINBASE_CHECK = False
    env.config.GENESIS_ACCOUNT = genesisAccount
    env.config.GENESIS_COIN = genesisQuarkash
    env.config.GENESIS_MINOR_COIN = genesisMinorQuarkash
    env.config.TESTNET_MASTER_ACCOUNT = genesisAccount
    env.clusterConfig.MASTER_TO_SLAVE_CONNECT_RETRY_DELAY = 0.01
    env.db = InMemoryDb()

    env.setNetworkId(1234567890)

    env.config.ACCOUNTS_TO_FUND = None
    env.config.LOADTEST_ACCOUNTS = None
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
        withdrawTo=withdrawTo,
        networkId=shardState.env.config.NETWORK_ID)
    evmTx.sign(key=fromId.getKey())
    return Transaction(
        inList=[],
        code=Code.createEvmCode(evmTx),
        outList=[])


def create_contract_creation_transaction(shardState, fromId):
    evmTx = EvmTransaction(
        branchValue=shardState.branch.value,
        nonce=shardState.getTransactionCount(fromId.getRecipient()),
        gasprice=1,
        startgas=1000000,
        value=0,
        to=b'',
        # a contract creation payload
        data=bytes.fromhex("608060405234801561001057600080fd5b5061013f806100206000396000f300608060405260043610610041576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063942ae0a714610046575b600080fd5b34801561005257600080fd5b5061005b6100d6565b6040518080602001828103825283818151815260200191508051906020019080838360005b8381101561009b578082015181840152602081019050610080565b50505050905090810190601f1680156100c85780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b60606040805190810160405280600a81526020017f68656c6c6f576f726c64000000000000000000000000000000000000000000008152509050905600a165627a7a72305820a45303c36f37d87d8dd9005263bdf8484b19e86208e4f8ed476bf393ec06a6510029"),  # noqa
        networkId=shardState.env.config.NETWORK_ID
    )
    evmTx.sign(key=fromId.getKey())
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
            jsonRpcPort=portStart + 1,
            clusterPortStart=portStart + 2,
            seedHost=env.config.P2P_SEED_HOST,
            seedPort=env.config.P2P_SEED_PORT,
        )
        portStart += (3 + env.config.SHARD_SIZE)

        env.config.P2P_SERVER_PORT = p2pPort
        env.config.P2P_SEED_PORT = seedPort
        env.clusterConfig.ID = 0
        env.clusterConfig.CONFIG = ClusterConfig(config)

        slaveServerList = []
        for slave in range(env.config.SHARD_SIZE):
            slaveEnv = env.copy()
            slaveEnv.db = InMemoryDb()
            slaveEnv.clusterConfig.ID = config["slaves"][slave]["id"]
            slaveEnv.clusterConfig.NODE_PORT = config["slaves"][slave]["port"]
            slaveEnv.clusterConfig.SHARD_MASK_LIST = [ShardMask(v) for v in config["slaves"][slave]["shard_masks"]]
            slaveServer = SlaveServer(slaveEnv, name="cluster{}_slave{}".format(i, slave))
            slaveServer.start()
            slaveServerList.append(slaveServer)

        rootState = RootState(env)
        masterServer = MasterServer(env, rootState, name="cluster{}_master".format(i))
        masterServer.start()

        # Wait until the cluster is ready
        loop.run_until_complete(masterServer.clusterActiveFuture)

        # Start simple network and connect to seed host
        network = SimpleNetwork(env, masterServer)
        network.startServer()
        if i != 0:
            peer = call_async(network.connect("127.0.0.1", seedPort))
        else:
            peer = None

        clusterList.append(Cluster(masterServer, slaveServerList, network, peer))

    return clusterList


def shutdown_clusters(clusterList, expectAbortedRpcCount=0):
    loop = asyncio.get_event_loop()

    # allow pending RPCs to finish to avoid annoying connection reset error messages
    loop.run_until_complete(asyncio.sleep(0.1))

    for cluster in clusterList:
        # Shutdown simple network first
        cluster.network.shutdown()

    # Sleep 0.1 so that DESTROY_CLUSTER_PEER_ID command could be processed
    loop.run_until_complete(asyncio.sleep(0.1))

    for cluster in clusterList:
        for slave in cluster.slaveList:
            slave.shutdown()
            loop.run_until_complete(slave.getShutdownFuture())

        cluster.master.shutdown()
        loop.run_until_complete(cluster.master.getShutdownFuture())

    check(expectAbortedRpcCount == AbstractConnection.abortedRpcCount)


class ClusterContext(ContextDecorator):

    def __init__(self, numCluster, genesisAccount=Address.createEmptyAccount()):
        self.numCluster = numCluster
        self.genesisAccount = genesisAccount

    def __enter__(self):
        self.clusterList = create_test_clusters(self.numCluster, self.genesisAccount)
        return self.clusterList

    def __exit__(self, *exc):
        shutdown_clusters(self.clusterList)
