import argparse
import copy
import json
import os
import socket
import tempfile
from typing import List

from quarkchain.cluster.monitoring import KafkaSampleLogger
from quarkchain.cluster.rpc import SlaveInfo
from quarkchain.config import BaseConfig, ChainConfig, QuarkChainConfig
from quarkchain.core import Address, ChainMask
from quarkchain.utils import Logger, check, is_p2

DEFAULT_HOST = socket.gethostbyname(socket.gethostname())


def update_genesis_alloc(cluser_config):
    """ Update ShardConfig.GENESIS.ALLOC """
    ALLOC_FILE_TEMPLATE = "alloc/{}.json"
    LOADTEST_FILE = "loadtest.json"

    if not cluser_config.GENESIS_DIR:
        return
    alloc_file_template = os.path.join(cluser_config.GENESIS_DIR, ALLOC_FILE_TEMPLATE)
    loadtest_file = os.path.join(cluser_config.GENESIS_DIR, LOADTEST_FILE)

    qkc_config = cluser_config.QUARKCHAIN

    allocation = {
        qkc_config.GENESIS_TOKEN: 1000000 * (10 ** 18),
        "QETC": 2 * (10 ** 8) * (10 ** 18),
        "QFB": 3 * (10 ** 8) * (10 ** 18),
        "QAAPL": 4 * (10 ** 8) * (10 ** 18),
        "QTSLA": 5 * (10 ** 8) * (10 ** 18),
    }

    old_shards = copy.deepcopy(qkc_config.shards)
    try:
        for chain_id in range(qkc_config.CHAIN_SIZE):
            alloc_file = alloc_file_template.format(chain_id)
            with open(alloc_file, "r") as f:
                items = json.load(f)
            for item in items:
                address = Address.create_from(item["address"])
                full_shard_id = qkc_config.get_full_shard_id_by_full_shard_key(
                    address.full_shard_key
                )
                qkc_config.shards[full_shard_id].GENESIS.ALLOC[
                    item["address"]
                ] = allocation

            Logger.info(
                "[{}] Imported {} genesis accounts into config from {}".format(
                    chain_id, len(items), alloc_file
                )
            )
    except Exception as e:
        Logger.warning(
            "Error importing genesis accounts from {}: {}".format(alloc_file, e)
        )
        qkc_config.shards = old_shards
        Logger.warning("Cleared all partially imported genesis accounts!")

    # each account in loadtest file is funded on all the shards
    try:
        with open(loadtest_file, "r") as f:
            items = json.load(f)
            qkc_config.loadtest_accounts = items

        for item in items:
            address = Address.create_from(item["address"])
            for full_shard_id, shard_config in qkc_config.shards.items():
                shard_config.GENESIS.ALLOC[
                    address.address_in_shard(full_shard_id).serialize().hex()
                ] = allocation

        Logger.info(
            "Imported {} loadtest accounts from {}".format(len(items), loadtest_file)
        )
    except Exception:
        Logger.info("No loadtest accounts imported into genesis alloc")


class MasterConfig(BaseConfig):
    MASTER_TO_SLAVE_CONNECT_RETRY_DELAY = 1.0


class SlaveConfig(BaseConfig):
    HOST = DEFAULT_HOST
    PORT = 38392
    WEBSOCKET_JSON_RPC_PORT = None
    ID = ""
    CHAIN_MASK_LIST = None
    TYPE = "QKCRPC"

    def to_dict(self):
        ret = super().to_dict()
        ret["CHAIN_MASK_LIST"] = [m.value for m in self.CHAIN_MASK_LIST]
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.CHAIN_MASK_LIST = [ChainMask(v) for v in config.CHAIN_MASK_LIST]
        return config


class SimpleNetworkConfig(BaseConfig):
    BOOTSTRAP_HOST = DEFAULT_HOST
    BOOTSTRAP_PORT = 38291


class GrpcServerConfig(BaseConfig):
    GRPC_SERVER_HOST = DEFAULT_HOST
    GRPC_SERVER_PORT = 50051


class P2PConfig(BaseConfig):
    # *new p2p module*
    BOOT_NODES = ""  # comma seperated enodes format: enode://PUBKEY@IP:PORT
    PRIV_KEY = ""
    MAX_PEERS = 25
    UPNP = False
    ALLOW_DIAL_IN_RATIO = 1.0
    PREFERRED_NODES = ""
    DISCOVERY_ONLY = False
    CRAWLING_ROUTING_TABLE_FILE_PATH = None


class MonitoringConfig(BaseConfig):
    """None of the configs here is available in commandline, so just set the json file to change defaults
    """

    NETWORK_NAME = ""
    CLUSTER_ID = DEFAULT_HOST
    KAFKA_REST_ADDRESS = ""  # REST API endpoint for logging to Kafka, IP[:PORT] format
    MINER_TOPIC = "qkc_miner"
    PROPAGATION_TOPIC = "block_propagation"
    ERRORS = "error"


class ClusterConfig(BaseConfig):
    P2P_PORT = 38291
    JSON_RPC_PORT = 38391
    PRIVATE_JSON_RPC_PORT = 38491
    JSON_RPC_HOST = "localhost"
    PRIVATE_JSON_RPC_HOST = "localhost"
    ENABLE_PUBLIC_JSON_RPC = True
    ENABLE_PRIVATE_JSON_RPC = True
    ENABLE_TRANSACTION_HISTORY = False
    ENABLE_GRPC_SERVER = False

    DB_PATH_ROOT = "./db"
    LOG_LEVEL = "info"

    START_SIMULATED_MINING = False
    CLEAN = False
    GENESIS_DIR = None

    QUARKCHAIN = None
    MASTER = None
    SLAVE_LIST = None
    GRPC_SLAVE_LIST = None
    SIMPLE_NETWORK = None
    P2P = None
    GRPC_SERVER = None

    MONITORING = None

    def __init__(self):
        self.QUARKCHAIN = QuarkChainConfig()
        self.MASTER = MasterConfig()
        self.SLAVE_LIST = []  # type: List[SlaveConfig]
        self.GRPC_SLAVE_LIST = []  # type: List[SlaveConfig]
        self.SIMPLE_NETWORK = SimpleNetworkConfig()
        self.GRPC_SERVER = GrpcServerConfig()
        self._json_filepath = None
        self.MONITORING = MonitoringConfig()
        self.kafka_logger = KafkaSampleLogger(self)

        slave_config = SlaveConfig()
        slave_config.PORT = 38000
        slave_config.ID = "S0"
        slave_config.CHAIN_MASK_LIST = [ChainMask(1)]
        self.SLAVE_LIST.append(slave_config)

        fd, self.json_filepath = tempfile.mkstemp()
        with os.fdopen(fd, "w") as tmp:
            tmp.write(self.to_json())

    def get_slave_info_list(self):
        results = []
        for slave in self.SLAVE_LIST:
            results.append(
                SlaveInfo(slave.ID, slave.HOST, slave.PORT, slave.CHAIN_MASK_LIST)
            )
        return results

    def get_slave_config(self, id):
        for slave in self.SLAVE_LIST:
            if slave.ID == id:
                return slave
        raise RuntimeError("Slave id {0} does not exist in cluster config".format(id))

    @property
    def json_filepath(self):
        return self._json_filepath

    @json_filepath.setter
    def json_filepath(self, value):
        self._json_filepath = value

    def use_p2p(self):
        return self.P2P is not None

    def use_mem_db(self):
        return not self.DB_PATH_ROOT

    def apply_env(self):
        for k, v in os.environ.items():
            key_path = k.split("__")
            if key_path[0] != "QKC":
                continue

            print("Applying env {0}: {1}".format(k, v))

            config = self
            for i in range(1, len(key_path) - 1):
                name = key_path[i]
                if not hasattr(config, name):
                    raise ValueError("Cannot apply env {}: key not found".format(k))

                config = getattr(config, name)
                if not isinstance(config, BaseConfig):
                    raise ValueError("Cannot apply env {}: config not found".format(k))

            if not hasattr(config, key_path[-1]):
                raise ValueError("Cannot apply env {}: key not found".format(k))
            setattr(config, key_path[-1], eval(v))

    @classmethod
    def attach_arguments(cls, parser):
        parser.add_argument("--cluster_config", default="", type=str)
        parser.add_argument("--log_level", default=ClusterConfig.LOG_LEVEL, type=str)
        parser.add_argument(
            "--clean", action="store_true", default=ClusterConfig.CLEAN, dest="clean"
        )
        parser.add_argument(
            "--start_simulated_mining",
            action="store_true",
            default=ClusterConfig.START_SIMULATED_MINING,
            dest="start_simulated_mining",
        )
        pwd = os.path.dirname(os.path.abspath(__file__))
        default_genesis_dir = os.path.join(pwd, "../genesis_data")
        parser.add_argument("--genesis_dir", default=default_genesis_dir, type=str)

        parser.add_argument(
            "--num_chains", default=QuarkChainConfig.CHAIN_SIZE, type=int
        )
        parser.add_argument(
            "--num_shards_per_chain", default=ChainConfig.SHARD_SIZE, type=int
        )
        parser.add_argument("--root_block_interval_sec", default=10, type=int)
        parser.add_argument("--minor_block_interval_sec", default=3, type=int)
        parser.add_argument(
            "--network_id", default=QuarkChainConfig.NETWORK_ID, type=int
        )
        parser.add_argument(
            "--default_token",
            default=QuarkChainConfig.GENESIS_TOKEN,
            type=str,
            help="sets GENESIS_TOKEN and DEFAULT_CHAIN_TOKEN",
        )

        parser.add_argument("--num_slaves", default=4, type=int)
        parser.add_argument("--port_start", default=38000, type=int)
        parser.add_argument(
            "--db_path_root", default=ClusterConfig.DB_PATH_ROOT, type=str
        )
        parser.add_argument("--p2p_port", default=ClusterConfig.P2P_PORT, type=int)
        parser.add_argument(
            "--json_rpc_port", default=ClusterConfig.JSON_RPC_PORT, type=int
        )
        parser.add_argument(
            "--json_rpc_private_port",
            default=ClusterConfig.PRIVATE_JSON_RPC_PORT,
            type=int,
        )
        parser.add_argument(
            "--json_rpc_host", default=ClusterConfig.JSON_RPC_HOST, type=str
        )
        parser.add_argument(
            "--json_rpc_private_host",
            default=ClusterConfig.PRIVATE_JSON_RPC_HOST,
            type=str,
        )
        parser.add_argument(
            "--enable_public_json_rpc",
            default=ClusterConfig.ENABLE_PUBLIC_JSON_RPC,
            type=bool,
        )
        parser.add_argument(
            "--enable_private_json_rpc",
            default=ClusterConfig.ENABLE_PRIVATE_JSON_RPC,
            type=bool,
        )
        parser.add_argument(
            "--enable_transaction_history",
            action="store_true",
            default=False,
            dest="enable_transaction_history",
        )
        parser.add_argument(
            "--enable_grpc_server", action="store_true", default=False,
        )

        parser.add_argument(
            "--simple_network_bootstrap_host",
            default=SimpleNetworkConfig.BOOTSTRAP_HOST,
        )
        parser.add_argument(
            "--simple_network_bootstrap_port",
            default=SimpleNetworkConfig.BOOTSTRAP_PORT,
        )
        parser.add_argument(
            "--grpc_server_host", default=GrpcServerConfig.GRPC_SERVER_HOST,
        )
        parser.add_argument(
            "--grpc_server_port", default=GrpcServerConfig.GRPC_SERVER_PORT,
        )
        # p2p module
        parser.add_argument(
            "--p2p",
            action="store_true",
            default=False,
            dest="p2p",
            help="enables new p2p module",
        )
        parser.add_argument(
            "--max_peers",
            default=P2PConfig.MAX_PEERS,
            type=int,
            help="max peer for new p2p module",
        )
        parser.add_argument(
            "--bootnodes",
            default="",
            type=str,
            help="comma seperated enodes in the format: enode://PUBKEY@IP:PORT",
        )
        parser.add_argument(
            "--upnp",
            action="store_true",
            default=False,
            dest="upnp",
            help="if true, automatically runs a upnp service that sets port mapping on upnp-enabled devices",
        )
        parser.add_argument(
            "--privkey",
            default="",
            type=str,
            help="if empty, will be automatically generated; but note that it will be lost upon node reboot",
        )
        parser.add_argument(
            "--check_db",
            default=False,
            type=bool,
            help="if true, will perform integrity check on db only",
        )

        parser.add_argument("--monitoring_kafka_rest_address", default="", type=str)

    @classmethod
    def create_from_args(cls, args):
        """ Create ClusterConfig either from the JSON file or cmd flags.
        """

        def __create_from_args_internal():
            check(
                is_p2(args.num_shards_per_chain),
                "--num_shards_per_chain must be power of 2",
            )
            check(is_p2(args.num_slaves), "--num_slaves must be power of 2")

            config = ClusterConfig()
            config.LOG_LEVEL = args.log_level
            config.DB_PATH_ROOT = args.db_path_root

            config.P2P_PORT = args.p2p_port
            config.JSON_RPC_PORT = args.json_rpc_port
            config.PRIVATE_JSON_RPC_PORT = args.json_rpc_private_port
            config.JSON_RPC_HOST = args.json_rpc_host
            config.PRIVATE_JSON_RPC_HOST = args.json_rpc_private_host

            config.CLEAN = args.clean
            config.START_SIMULATED_MINING = args.start_simulated_mining
            config.ENABLE_TRANSACTION_HISTORY = args.enable_transaction_history

            config.QUARKCHAIN.update(
                args.num_chains,
                args.num_shards_per_chain,
                args.root_block_interval_sec,
                args.minor_block_interval_sec,
                args.default_token,
            )
            config.QUARKCHAIN.NETWORK_ID = args.network_id

            config.GENESIS_DIR = args.genesis_dir

            config.MONITORING.KAFKA_REST_ADDRESS = args.monitoring_kafka_rest_address

            if args.p2p:
                config.SIMPLE_NETWORK = None
                config.P2P = P2PConfig()
                # p2p module
                config.P2P.BOOT_NODES = args.bootnodes
                config.P2P.PRIV_KEY = args.privkey
                config.P2P.MAX_PEERS = args.max_peers
                config.P2P.UPNP = args.upnp
            else:
                config.P2P = None
                config.SIMPLE_NETWORK = SimpleNetworkConfig()
                config.SIMPLE_NETWORK.BOOTSTRAP_HOST = (
                    args.simple_network_bootstrap_host
                )
                config.SIMPLE_NETWORK.BOOTSTRAP_PORT = (
                    args.simple_network_bootstrap_port
                )

            if args.enable_grpc_server:
                config.ENABLE_GRPC_SERVER = args.enable_grpc_server
                config.GRPC_SERVER = GrpcServerConfig()
                config.GRPC_SERVER.GRPC_SERVER_HOST = args.grpc_server_host
                config.GRPC_SERVER.GRPC_SERVER_PORT = args.grpc_server_port

            config.SLAVE_LIST = []
            for i in range(args.num_slaves):
                slave_config = SlaveConfig()
                slave_config.PORT = args.port_start + i
                slave_config.ID = "S{}".format(i)
                slave_config.CHAIN_MASK_LIST = [ChainMask(i | args.num_slaves)]
                config.SLAVE_LIST.append(slave_config)

            fd, config.json_filepath = tempfile.mkstemp()
            with os.fdopen(fd, "w") as tmp:
                tmp.write(config.to_json())
            return config

        if args.cluster_config:
            with open(args.cluster_config) as f:
                config = cls.from_json(f.read())
                config.json_filepath = args.cluster_config
        else:
            config = __create_from_args_internal()
        config.apply_env()
        Logger.set_logging_level(config.LOG_LEVEL)
        Logger.set_kafka_logger(config.kafka_logger)
        update_genesis_alloc(config)
        return config

    def to_dict(self):
        ret = super().to_dict()
        ret["QUARKCHAIN"] = self.QUARKCHAIN.to_dict()
        ret["MONITORING"] = self.MONITORING.to_dict()
        ret["MASTER"] = self.MASTER.to_dict()
        ret["GRPC_SLAVE_LIST"] = [s.to_dict() for s in self.GRPC_SLAVE_LIST]
        ret["SLAVE_LIST"] = [s.to_dict() for s in self.SLAVE_LIST]
        ret["GRPC_SERVER"] = self.GRPC_SERVER.to_dict()

        if self.P2P:
            ret["P2P"] = self.P2P.to_dict()
            del ret["SIMPLE_NETWORK"]
        else:
            ret["SIMPLE_NETWORK"] = self.SIMPLE_NETWORK.to_dict()
            del ret["P2P"]

        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.QUARKCHAIN = QuarkChainConfig.from_dict(config.QUARKCHAIN)
        config.MONITORING = MonitoringConfig.from_dict(config.MONITORING)
        config.MASTER = MasterConfig.from_dict(config.MASTER)
        config.SLAVE_LIST = [SlaveConfig.from_dict(s) for s in config.SLAVE_LIST]
        config.GRPC_SLAVE_LIST = [
            SlaveConfig.from_dict(s) for s in config.GRPC_SLAVE_LIST
        ]
        config.GRPC_SERVER = GrpcServerConfig.from_dict(d["GRPC_SERVER"])

        if "P2P" in d:
            config.P2P = P2PConfig.from_dict(d["P2P"])
        else:
            config.SIMPLE_NETWORK = SimpleNetworkConfig.from_dict(d["SIMPLE_NETWORK"])

        return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    args = parser.parse_args()
    config = ClusterConfig.create_from_args(args)
    print(config.to_json())
