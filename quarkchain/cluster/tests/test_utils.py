import asyncio
from contextlib import ContextDecorator

from quarkchain.cluster.cluster_config import (
    ClusterConfig,
    SlaveConfig,
    SimpleNetworkConfig,
)
from quarkchain.cluster.master import MasterServer
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.simple_network import SimpleNetwork
from quarkchain.cluster.slave import SlaveServer
from quarkchain.config import ConsensusType
from quarkchain.core import Address, Branch, Transaction, Code, ShardMask
from quarkchain.db import InMemoryDb
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.env import DEFAULT_ENV
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.cluster.shard import Shard
from quarkchain.cluster.shard_state import ShardState
from quarkchain.protocol import AbstractConnection
from quarkchain.utils import call_async, check


def get_test_env(
    genesis_account=Address.create_empty_account(),
    genesis_minor_quarkash=0,
    shard_size=2,
    genesis_root_heights=None,
    remote_mining=False,
):
    env = DEFAULT_ENV.copy()

    env.db = InMemoryDb()
    env.set_network_id(1234567890)

    env.cluster_config = ClusterConfig()
    env.quark_chain_config.update(shard_size, 10, 1)

    if remote_mining:
        env.quark_chain_config.ROOT.CONSENSUS_CONFIG.REMOTE_MINE = True
        env.quark_chain_config.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SHA3SHA3
        env.quark_chain_config.ROOT.GENESIS.DIFFICULTY = 10

    env.quark_chain_config.ROOT.DIFFICULTY_ADJUSTMENT_CUTOFF_TIME = 40
    env.quark_chain_config.ROOT.DIFFICULTY_ADJUSTMENT_FACTOR = 1024

    if genesis_root_heights:
        check(len(genesis_root_heights) == shard_size)
        for shard_id in range(shard_size):
            shard = env.quark_chain_config.SHARD_LIST[shard_id]
            shard.GENESIS.ROOT_HEIGHT = genesis_root_heights[shard_id]

    # fund genesis account in all shards
    for i, shard in enumerate(env.quark_chain_config.SHARD_LIST):
        addr = genesis_account.address_in_shard(i).serialize().hex()
        shard.GENESIS.ALLOC[addr] = genesis_minor_quarkash
        shard.CONSENSUS_CONFIG.REMOTE_MINE = remote_mining
        shard.DIFFICULTY_ADJUSTMENT_CUTOFF_TIME = 7
        shard.DIFFICULTY_ADJUSTMENT_FACTOR = 512
        if remote_mining:
            shard.CONSENSUS_TYPE = ConsensusType.POW_SHA3SHA3
            shard.GENESIS.DIFFICULTY = 10

    env.quark_chain_config.SKIP_MINOR_DIFFICULTY_CHECK = True
    env.quark_chain_config.SKIP_ROOT_DIFFICULTY_CHECK = True
    env.cluster_config.ENABLE_TRANSACTION_HISTORY = True
    env.cluster_config.DB_PATH_ROOT = ""

    check(env.cluster_config.use_mem_db())

    return env


def create_transfer_transaction(
    shard_state,
    key,
    from_address,
    to_address,
    value,
    gas=21000,  # transfer tx min gas
    gas_price=1,
    nonce=None,
    data=b"",
):
    """ Create an in-shard xfer tx
    """
    evm_tx = EvmTransaction(
        nonce=shard_state.get_transaction_count(from_address.recipient)
        if nonce is None
        else nonce,
        gasprice=gas_price,
        startgas=gas,
        to=to_address.recipient,
        value=value,
        data=data,
        from_full_shard_id=from_address.full_shard_id,
        to_full_shard_id=to_address.full_shard_id,
        network_id=shard_state.env.quark_chain_config.NETWORK_ID,
    )
    evm_tx.sign(key=key)
    return Transaction(in_list=[], code=Code.create_evm_code(evm_tx), out_list=[])


CONTRACT_CREATION_BYTECODE = "608060405234801561001057600080fd5b5061013f806100206000396000f300608060405260043610610041576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063942ae0a714610046575b600080fd5b34801561005257600080fd5b5061005b6100d6565b6040518080602001828103825283818151815260200191508051906020019080838360005b8381101561009b578082015181840152602081019050610080565b50505050905090810190601f1680156100c85780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b60606040805190810160405280600a81526020017f68656c6c6f576f726c64000000000000000000000000000000000000000000008152509050905600a165627a7a72305820a45303c36f37d87d8dd9005263bdf8484b19e86208e4f8ed476bf393ec06a6510029"
"""
contract EventContract {
    event Hi(address indexed);
    constructor() public {
        emit Hi(msg.sender);
    }
    function f() public {
        emit Hi(msg.sender);
    }
}
"""
CONTRACT_CREATION_WITH_EVENT_BYTECODE = "608060405234801561001057600080fd5b503373ffffffffffffffffffffffffffffffffffffffff167fa9378d5bd800fae4d5b8d4c6712b2b64e8ecc86fdc831cb51944000fc7c8ecfa60405160405180910390a260c9806100626000396000f300608060405260043610603f576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806326121ff0146044575b600080fd5b348015604f57600080fd5b5060566058565b005b3373ffffffffffffffffffffffffffffffffffffffff167fa9378d5bd800fae4d5b8d4c6712b2b64e8ecc86fdc831cb51944000fc7c8ecfa60405160405180910390a25600a165627a7a72305820e7fc37b0c126b90719ace62d08b2d70da3ad34d3e6748d3194eb58189b1917c30029"
"""
contract Storage {
    uint pos0;
    mapping(address => uint) pos1;
    function Storage() {
        pos0 = 1234;
        pos1[msg.sender] = 5678;
    }
}
"""
CONTRACT_WITH_STORAGE = "6080604052348015600f57600080fd5b506104d260008190555061162e600160003373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002081905550603580606c6000396000f3006080604052600080fd00a165627a7a72305820a6ef942c101f06333ac35072a8ff40332c71d0e11cd0e6d86de8cae7b42696550029"


def _contract_tx_gen(shard_state, key, from_address, to_full_shard_id, bytecode):
    evm_tx = EvmTransaction(
        nonce=shard_state.get_transaction_count(from_address.recipient),
        gasprice=1,
        startgas=1000000,
        value=0,
        to=b"",
        data=bytes.fromhex(bytecode),
        from_full_shard_id=from_address.full_shard_id,
        to_full_shard_id=to_full_shard_id,
        network_id=shard_state.env.quark_chain_config.NETWORK_ID,
    )
    evm_tx.sign(key)
    return Transaction(in_list=[], code=Code.create_evm_code(evm_tx), out_list=[])


def create_contract_creation_transaction(
    shard_state, key, from_address, to_full_shard_id
):
    return _contract_tx_gen(
        shard_state, key, from_address, to_full_shard_id, CONTRACT_CREATION_BYTECODE
    )


def create_contract_creation_with_event_transaction(
    shard_state, key, from_address, to_full_shard_id
):
    return _contract_tx_gen(
        shard_state,
        key,
        from_address,
        to_full_shard_id,
        CONTRACT_CREATION_WITH_EVENT_BYTECODE,
    )


def create_contract_with_storage_transaction(
    shard_state, key, from_address, to_full_shard_id
):
    return _contract_tx_gen(
        shard_state, key, from_address, to_full_shard_id, CONTRACT_WITH_STORAGE
    )


class Cluster:
    def __init__(self, master, slave_list, network, peer):
        self.master = master
        self.slave_list = slave_list
        self.network = network
        self.peer = peer

    def get_shard(self, shard_id) -> Shard:
        branch = Branch.create(self.master.env.quark_chain_config.SHARD_SIZE, shard_id)
        for slave in self.slave_list:
            if branch in slave.shards:
                return slave.shards[branch]
        return None

    def get_shard_state(self, shard_id) -> ShardState:
        shard = self.get_shard(shard_id)
        if not shard:
            return None
        return shard.state


# server.close() does not release the port sometimes even after server.wait_closed() is awaited.
# we have to use unique ports for each test as a workaround.
PORT_START = 38000


def get_next_port():
    global PORT_START
    port = PORT_START
    PORT_START += 1
    return port


def create_test_clusters(
    num_cluster,
    genesis_account,
    shard_size,
    num_slaves,
    genesis_root_heights,
    remote_mining=False,
    small_coinbase=False,
):
    # so we can have lower minimum diff
    easy_diff_calc = EthDifficultyCalculator(
        cutoff=45, diff_factor=2048, minimum_diff=10
    )

    bootstrap_port = get_next_port()  # first cluster will listen on this port
    cluster_list = []
    loop = asyncio.get_event_loop()

    for i in range(num_cluster):
        env = get_test_env(
            genesis_account,
            genesis_minor_quarkash=1000000,
            shard_size=shard_size,
            genesis_root_heights=genesis_root_heights,
            remote_mining=remote_mining,
        )
        env.cluster_config.P2P_PORT = bootstrap_port if i == 0 else get_next_port()
        env.cluster_config.JSON_RPC_PORT = get_next_port()
        env.cluster_config.PRIVATE_JSON_RPC_PORT = get_next_port()
        env.cluster_config.SIMPLE_NETWORK = SimpleNetworkConfig()
        env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_PORT = bootstrap_port

        if small_coinbase:
            # prevent breaking previous tests after tweaking default rewards
            env.quark_chain_config.ROOT.COINBASE_AMOUNT = 5
            for c in env.quark_chain_config.SHARD_LIST:
                c.COINBASE_AMOUNT = 5

        env.cluster_config.SLAVE_LIST = []
        for j in range(num_slaves):
            slave_config = SlaveConfig()
            slave_config.ID = "S{}".format(j)
            slave_config.PORT = get_next_port()
            slave_config.SHARD_MASK_LIST = [ShardMask(num_slaves | j)]
            slave_config.DB_PATH_ROOT = None  # TODO: fix the db in config
            env.cluster_config.SLAVE_LIST.append(slave_config)

        slave_server_list = []
        for j in range(num_slaves):
            slave_env = env.copy()
            slave_env.db = InMemoryDb()
            slave_env.slave_config = env.cluster_config.get_slave_config(
                "S{}".format(j)
            )
            slave_server = SlaveServer(slave_env, name="cluster{}_slave{}".format(i, j))
            slave_server.start()
            slave_server_list.append(slave_server)

        root_state = RootState(env, diff_calc=easy_diff_calc)
        master_server = MasterServer(env, root_state, name="cluster{}_master".format(i))
        master_server.start()

        # Wait until the cluster is ready
        loop.run_until_complete(master_server.cluster_active_future)

        # Substitute diff calculate with an easier one
        for slave in slave_server_list:
            for shard in slave.shards.values():
                shard.state.diff_calc = easy_diff_calc

        # Start simple network and connect to seed host
        network = SimpleNetwork(env, master_server, loop)
        network.start_server()
        if i != 0:
            peer = call_async(network.connect("127.0.0.1", bootstrap_port))
        else:
            peer = None

        cluster_list.append(Cluster(master_server, slave_server_list, network, peer))

    return cluster_list


def shutdown_clusters(cluster_list, expect_aborted_rpc_count=0):
    loop = asyncio.get_event_loop()

    # allow pending RPCs to finish to avoid annoying connection reset error messages
    loop.run_until_complete(asyncio.sleep(0.1))

    for cluster in cluster_list:
        # Shutdown simple network first
        cluster.network.shutdown()

    # Sleep 0.1 so that DESTROY_CLUSTER_PEER_ID command could be processed
    loop.run_until_complete(asyncio.sleep(0.1))

    for cluster in cluster_list:
        for slave in cluster.slave_list:
            slave.shutdown()
            loop.run_until_complete(slave.get_shutdown_future())

        cluster.master.shutdown()
        loop.run_until_complete(cluster.master.get_shutdown_future())

    check(expect_aborted_rpc_count == AbstractConnection.aborted_rpc_count)


class ClusterContext(ContextDecorator):
    def __init__(
        self,
        num_cluster,
        genesis_account=Address.create_empty_account(),
        shard_size=2,
        num_slaves=None,
        genesis_root_heights=None,
        remote_mining=False,
        small_coinbase=False,
    ):
        self.num_cluster = num_cluster
        self.genesis_account = genesis_account
        self.shard_size = shard_size
        self.num_slaves = num_slaves if num_slaves else shard_size
        self.genesis_root_heights = genesis_root_heights
        self.remote_mining = remote_mining
        self.small_coinbase = small_coinbase

    def __enter__(self):
        self.cluster_list = create_test_clusters(
            self.num_cluster,
            self.genesis_account,
            self.shard_size,
            self.num_slaves,
            self.genesis_root_heights,
            remote_mining=self.remote_mining,
            small_coinbase=self.small_coinbase,
        )
        return self.cluster_list

    def __exit__(self, exc_type, exc_val, traceback):
        shutdown_clusters(self.cluster_list)
