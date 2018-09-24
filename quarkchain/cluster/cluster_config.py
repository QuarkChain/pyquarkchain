import argparse
import ipaddress
import json
import os
import socket
import tempfile

from quarkchain.cluster.monitoring import KafkaSampleLogger
from quarkchain.cluster.rpc import SlaveInfo
from quarkchain.config import QuarkChainConfig, BaseConfig
from quarkchain.core import Address
from quarkchain.core import ShardMask
from quarkchain.utils import is_p2, check, Logger

HOST = socket.gethostbyname(socket.gethostname())


def update_genesis_alloc(cluser_config):
    """ Update ShardConfig.GENESIS.ALLOC """
    ALLOC_FILE = "alloc.json"
    LOADTEST_FILE = "loadtest.json"

    if not cluser_config.GENESIS_DIR:
        return
    alloc_file = os.path.join(cluser_config.GENESIS_DIR, ALLOC_FILE)
    loadtest_file = os.path.join(cluser_config.GENESIS_DIR, LOADTEST_FILE)

    qkc_config = cluser_config.QUARKCHAIN

    # each account in alloc_file is only funded on the shard it belongs to
    try:
        with open(alloc_file, "r") as f:
            items = json.load(f)
            # address_hex -> key_hex for jsonrpc faucet drip
            qkc_config.alloc_accounts = dict()
        for item in items:
            qkc_config.alloc_accounts[item["address"]] = item["key"]
            address = Address.create_from(item["address"])
            shard = address.get_shard_id(qkc_config.SHARD_SIZE)
            qkc_config.SHARD_LIST[shard].GENESIS.ALLOC[item["address"]] = 1000000 * (
                10 ** 18
            )

        Logger.info(
            "Imported {} accounts from genesis alloc at {}".format(
                len(items), alloc_file
            )
        )
    except Exception as e:
        Logger.warning("Unable to load genesis alloc from {}: {}".format(alloc_file, e))

    # each account in loadtest file is funded on all the shards
    try:
        with open(loadtest_file, "r") as f:
            items = json.load(f)
            qkc_config.loadtest_accounts = items

        for item in items:
            address = Address.create_from(item["address"])
            for i, shard in enumerate(qkc_config.SHARD_LIST):
                shard.GENESIS.ALLOC[
                    address.address_in_shard(i).serialize().hex()
                ] = 1000 * (10 ** 18)

        Logger.info(
            "Imported {} loadtest accounts from {}".format(len(items), loadtest_file)
        )
    except Exception:
        Logger.info("No loadtest accounts imported into genesis alloc")


class MasterConfig(BaseConfig):
    MASTER_TO_SLAVE_CONNECT_RETRY_DELAY = 1.0


class SlaveConfig(BaseConfig):
    IP = HOST
    PORT = 38392
    ID = ""
    SHARD_MASK_LIST = None

    def to_dict(self):
        ret = super().to_dict()
        ret["SHARD_MASK_LIST"] = [m.value for m in self.SHARD_MASK_LIST]
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.SHARD_MASK_LIST = [ShardMask(v) for v in config.SHARD_MASK_LIST]
        return config


class SimpleNetworkConfig(BaseConfig):
    BOOTSTRAP_HOST = HOST
    BOOTSTRAP_PORT = 38291


class P2PConfig(BaseConfig):
    IP = HOST
    DISCOVERY_PORT = 29000
    BOOTSTRAP_HOST = HOST
    BOOTSTRAP_PORT = 29000
    MIN_PEERS = 2
    MAX_PEERS = 10
    ADDITIONAL_BOOTSTRAPS = ""


class MonitoringConfig(BaseConfig):
    """None of the configs here is available in commandline, so just set the json file to change defaults
    """

    NETWORK_NAME = ""
    CLUSTER_ID = HOST
    KAFKA_REST_ADDRESS = ""  # REST API endpoint for logging to Kafka, IP[:PORT] format
    MINER_TOPIC = "qkc_miner"
    PROPAGATION_TOPIC = "block_propagation"
    ERRORS = "error"


class ClusterConfig(BaseConfig):
    P2P_PORT = 38291
    JSON_RPC_PORT = 38391
    PRIVATE_JSON_RPC_PORT = 38491
    ENABLE_TRANSACTION_HISTORY = False

    DB_PATH_ROOT = "./db"
    LOG_LEVEL = "info"

    MINE = False
    CLEAN = False
    GENESIS_DIR = None

    QUARKCHAIN = None
    MASTER = None
    SLAVE_LIST = None
    SIMPLE_NETWORK = None
    P2P = None

    MONITORING = None

    def __init__(self):
        self.QUARKCHAIN = QuarkChainConfig()
        self.MASTER = MasterConfig()
        self.SLAVE_LIST = []
        self.SIMPLE_NETWORK = SimpleNetworkConfig()
        self._json_filepath = None
        self.MONITORING = MonitoringConfig()
        self.kafka_logger = KafkaSampleLogger(self)

        slave_config = SlaveConfig()
        slave_config.PORT = 38000
        slave_config.ID = "S0"
        slave_config.SHARD_MASK_LIST = [ShardMask(1)]
        self.SLAVE_LIST.append(slave_config)

        fd, self.json_filepath = tempfile.mkstemp()
        with os.fdopen(fd, "w") as tmp:
            tmp.write(self.to_json())

    def get_slave_info_list(self):
        results = []
        for slave in self.SLAVE_LIST:
            ip = int(ipaddress.ip_address(slave.IP))
            results.append(SlaveInfo(slave.ID, ip, slave.PORT, slave.SHARD_MASK_LIST))
        return results

    def get_slave_config(self, id):
        for slave in self.SLAVE_LIST:
            if slave.ID == id:
                return slave
        raise RuntimeError("Slave id {} does not exist in cluster config".format(id))

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

    @classmethod
    def attach_arguments(cls, parser):
        parser.add_argument("--cluster_config", default="", type=str)
        parser.add_argument("--log_level", default=ClusterConfig.LOG_LEVEL, type=str)
        parser.add_argument(
            "--clean", action="store_true", default=ClusterConfig.CLEAN, dest="clean"
        )
        parser.add_argument(
            "--mine", action="store_true", default=ClusterConfig.MINE, dest="mine"
        )
        pwd = os.path.dirname(os.path.abspath(__file__))
        default_genesis_dir = os.path.join(pwd, "../genesis_data")
        parser.add_argument("--genesis_dir", default=default_genesis_dir, type=str)

        parser.add_argument(
            "--num_shards", default=QuarkChainConfig.SHARD_SIZE, type=int
        )
        parser.add_argument("--root_block_interval_sec", default=10, type=int)
        parser.add_argument("--minor_block_interval_sec", default=3, type=int)
        parser.add_argument(
            "--network_id", default=QuarkChainConfig.NETWORK_ID, type=int
        )

        parser.add_argument("--num_slaves", default=4, type=int)
        parser.add_argument("--port_start", default=38000, type=int)
        parser.add_argument(
            "--db_path_root", default=ClusterConfig.DB_PATH_ROOT, type=str
        )
        parser.add_argument("--p2p_port", default=ClusterConfig.P2P_PORT)
        parser.add_argument(
            "--json_rpc_port", default=ClusterConfig.JSON_RPC_PORT, type=int
        )
        parser.add_argument(
            "--json_rpc_private_port",
            default=ClusterConfig.PRIVATE_JSON_RPC_PORT,
            type=int,
        )
        parser.add_argument(
            "--enable_transaction_history",
            action="store_true",
            default=False,
            dest="enable_transaction_history",
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
            "--devp2p_enable", action="store_true", default=False, dest="devp2p_enable"
        )
        """
        set devp2p_ip so that peers can connect to this cluster
        leave empty if you want to use `socket.gethostbyname()`, but it may cause this cluster to be unreachable by peers
        """
        parser.add_argument("--devp2p_ip", default=P2PConfig.IP, type=str)
        parser.add_argument("--devp2p_port", default=P2PConfig.DISCOVERY_PORT, type=int)
        parser.add_argument(
            "--devp2p_bootstrap_host", default=P2PConfig.BOOTSTRAP_HOST, type=str
        )
        parser.add_argument(
            "--devp2p_bootstrap_port", default=P2PConfig.BOOTSTRAP_PORT, type=int
        )
        parser.add_argument("--devp2p_min_peers", default=P2PConfig.MIN_PEERS, type=int)
        parser.add_argument("--devp2p_max_peers", default=P2PConfig.MAX_PEERS, type=int)
        parser.add_argument("--devp2p_additional_bootstraps", default="", type=str)
        parser.add_argument("--monitoring_kafka_rest_address", default="", type=str)

    @classmethod
    def create_from_args(cls, args):
        """ Create ClusterConfig either from the JSON file or cmd flags.
        """

        def __create_from_args_internal():
            check(is_p2(args.num_shards), "--num_shards must be power of 2")
            check(is_p2(args.num_slaves), "--num_slaves must be power of 2")

            config = ClusterConfig()
            config.LOG_LEVEL = args.log_level
            config.DB_PATH_ROOT = args.db_path_root

            config.P2P_PORT = args.p2p_port
            config.JSON_RPC_PORT = args.json_rpc_port
            config.PRIVATE_JSON_RPC_PORT = args.json_rpc_private_port

            config.CLEAN = args.clean
            config.MINE = args.mine
            config.ENABLE_TRANSACTION_HISTORY = args.enable_transaction_history

            config.QUARKCHAIN.update(
                args.num_shards,
                args.root_block_interval_sec,
                args.minor_block_interval_sec,
            )
            config.QUARKCHAIN.NETWORK_ID = args.network_id

            config.GENESIS_DIR = args.genesis_dir

            config.MONITORING.KAFKA_REST_ADDRESS = args.monitoring_kafka_rest_address

            if args.devp2p_enable:
                config.SIMPLE_NETWORK = None
                config.P2P = P2PConfig()
                config.P2P.IP = args.devp2p_ip
                config.P2P.DISCOVERY_PORT = args.devp2p_port
                config.P2P.BOOTSTRAP_HOST = args.devp2p_bootstrap_host
                config.P2P.BOOTSTRAP_PORT = args.devp2p_bootstrap_port
                config.P2P.MIN_PEERS = args.devp2p_min_peers
                config.P2P.MAX_PEERS = args.devp2p_max_peers
                config.P2P.ADDITIONAL_BOOTSTRAPS = args.devp2p_additional_bootstraps
            else:
                config.P2P = None
                config.SIMPLE_NETWORK = SimpleNetworkConfig()
                config.SIMPLE_NETWORK.BOOTSTRAP_HOST = (
                    args.simple_network_bootstrap_host
                )
                config.SIMPLE_NETWORK.BOOTSTRAP_PORT = (
                    args.simple_network_bootstrap_port
                )

            config.SLAVE_LIST = []
            for i in range(args.num_slaves):
                slave_config = SlaveConfig()
                slave_config.PORT = args.port_start + i
                slave_config.ID = "S{}".format(i)
                slave_config.SHARD_MASK_LIST = [ShardMask(i | args.num_slaves)]

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
        Logger.set_logging_level(config.LOG_LEVEL)
        Logger.set_kafka_logger(config.kafka_logger)
        update_genesis_alloc(config)
        return config

    def to_dict(self):
        ret = super().to_dict()
        ret["QUARKCHAIN"] = self.QUARKCHAIN.to_dict()
        ret["MONITORING"] = self.MONITORING.to_dict()
        ret["MASTER"] = self.MASTER.to_dict()
        ret["SLAVE_LIST"] = [s.to_dict() for s in self.SLAVE_LIST]
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
