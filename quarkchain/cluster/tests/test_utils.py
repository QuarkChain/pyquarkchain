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
from quarkchain.protocol import AbstractConnection
from quarkchain.utils import call_async, check


def get_test_env(
        genesisAccount=Address.create_empty_account(),
        genesisQuarkash=0,
        genesisMinorQuarkash=0,
        shardSize=2):
    env = DEFAULT_ENV.copy()
    env.config.set_shard_size(shardSize)
    env.config.SKIP_MINOR_DIFFICULTY_CHECK = True
    env.config.SKIP_ROOT_DIFFICULTY_CHECK = True
    env.config.SKIP_MINOR_COINBASE_CHECK = False
    env.config.GENESIS_ACCOUNT = genesisAccount
    env.config.GENESIS_COIN = genesisQuarkash
    env.config.GENESIS_MINOR_COIN = genesisMinorQuarkash
    env.config.TESTNET_MASTER_ACCOUNT = genesisAccount
    env.clusterConfig.MASTER_TO_SLAVE_CONNECT_RETRY_DELAY = 0.01
    env.db = InMemoryDb()
    env.set_network_id(1234567890)

    env.config.ACCOUNTS_TO_FUND = []
    env.config.LOADTEST_ACCOUNTS = []
    return env


def create_transfer_transaction(
        shardState,
        key,
        fromAddress,
        toAddress,
        value,
        gas=21000,     # transfer tx min gas
        gasPrice=1,
        nonce=None,
):
    """ Create an in-shard xfer tx
    """
    evmTx = EvmTransaction(
        nonce=shardState.get_transaction_count(fromAddress.recipient) if nonce is None else nonce,
        gasprice=gasPrice,
        startgas=gas,
        to=toAddress.recipient,
        value=value,
        data=b'',
        fromFullShardId=fromAddress.fullShardId,
        toFullShardId=toAddress.fullShardId,
        networkId=shardState.env.config.NETWORK_ID,
    )
    evmTx.sign(key=key)
    return Transaction(
        inList=[],
        code=Code.create_evm_code(evmTx),
        outList=[])


def create_contract_creation_transaction(shardState, key, fromAddress, toFullShardId):
    evmTx = EvmTransaction(
        nonce=shardState.get_transaction_count(fromAddress.recipient),
        gasprice=1,
        startgas=1000000,
        value=0,
        to=b'',
        # a contract creation payload
        data=bytes.fromhex("608060405234801561001057600080fd5b5061013f806100206000396000f300608060405260043610610041576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063942ae0a714610046575b600080fd5b34801561005257600080fd5b5061005b6100d6565b6040518080602001828103825283818151815260200191508051906020019080838360005b8381101561009b578082015181840152602081019050610080565b50505050905090810190601f1680156100c85780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b60606040805190810160405280600a81526020017f68656c6c6f576f726c64000000000000000000000000000000000000000000008152509050905600a165627a7a72305820a45303c36f37d87d8dd9005263bdf8484b19e86208e4f8ed476bf393ec06a6510029"),  # noqa
        fromFullShardId=fromAddress.fullShardId,
        toFullShardId=toFullShardId,
        networkId=shardState.env.config.NETWORK_ID
    )
    evmTx.sign(key)
    return Transaction(
        inList=[],
        code=Code.create_evm_code(evmTx),
        outList=[])


class Cluster:

    def __init__(self, master, slaveList, network, peer):
        self.master = master
        self.slaveList = slaveList
        self.network = network
        self.peer = peer


# server.close() does not release the port sometimes even after server.wait_closed() is awaited.
# we have to use unique ports for each test as a workaround.
PORT_START = 38000


def get_next_port():
    global PORT_START
    port = PORT_START
    PORT_START += 1
    return port


def create_test_clusters(numCluster, genesisAccount=Address.create_empty_account()):
    seedPort = get_next_port()  # first cluster will listen on this port
    clusterList = []
    loop = asyncio.get_event_loop()

    for i in range(numCluster):
        env = get_test_env(genesisAccount, genesisMinorQuarkash=1000000)

        config = create_cluster_config(
            slaveCount=env.config.SHARD_SIZE,
            ip="127.0.0.1",
            p2pPort=0,
            jsonRpcPort=0,
            jsonRpcPrivatePort=0,
            clusterPortStart=get_next_port(),
            seedHost="",
            seedPort=0,
            devp2p=False,
            devp2p_ip='',
            devp2p_port=29000,
            devp2p_bootstrap_host='0.0.0.0',
            devp2p_bootstrap_port=29000,
            devp2p_min_peers=2,
            devp2p_max_peers=10,
            devp2p_additional_bootstraps='',
        )
        for j in range(env.config.SHARD_SIZE):
            # skip the ones used by create_cluster_config
            get_next_port()

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

        env.config.P2P_SERVER_PORT = seedPort if i == 0 else get_next_port()
        env.config.P2P_SEED_PORT = seedPort
        env.clusterConfig.ID = 0
        env.clusterConfig.CONFIG = ClusterConfig(config)

        rootState = RootState(env)
        masterServer = MasterServer(env, rootState, name="cluster{}_master".format(i))
        masterServer.start()

        # Wait until the cluster is ready
        loop.run_until_complete(masterServer.clusterActiveFuture)

        # Start simple network and connect to seed host
        network = SimpleNetwork(env, masterServer)
        network.start_server()
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
            loop.run_until_complete(slave.get_shutdown_future())

        cluster.master.shutdown()
        loop.run_until_complete(cluster.master.get_shutdown_future())

    check(expectAbortedRpcCount == AbstractConnection.abortedRpcCount)


class ClusterContext(ContextDecorator):

    def __init__(self, numCluster, genesisAccount=Address.create_empty_account()):
        self.numCluster = numCluster
        self.genesisAccount = genesisAccount

    def __enter__(self):
        self.clusterList = create_test_clusters(self.numCluster, self.genesisAccount)
        return self.clusterList

    def __exit__(self, *exc):
        shutdown_clusters(self.clusterList)
