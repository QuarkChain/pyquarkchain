import ipaddress
import json
import os
import tempfile

from quarkchain.cluster.rpc import SlaveInfo
from quarkchain.core import ShardMask


class BaseConfig:
    def to_dict(self):
        ret = dict()
        for k, v in self.__class__.__dict__.items():
            if k.startswith("_") or (not isinstance(v, (str, int, list))):
                continue
            ret[k] = getattr(self, k) if k in self.__dict__ else v
        return ret

    @classmethod
    def from_dict(cls, d):
        config = cls()
        for k, v in d.items():
            setattr(config, k, v)
        return config


class MasterConfig(BaseConfig):
    IP = "0.0.0.0"

    MASTER_TO_SLAVE_CONNECT_RETRY_DELAY = 1.0


class SlaveConfig(BaseConfig):
    IP = "0.0.0.0"
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
    BOOTSTRAP_HOST = "0.0.0.0"
    BOOTSTRAP_PORT = 38291


class P2PConfig(BaseConfig):
    IP = ""
    DISCOVERY_PORT = 29000
    BOOTSTRAP_HOST = "0.0.0.0"
    BOOTSTRAP_PORT = 29000
    MIN_PEERS = 2
    MAX_PEERS = 10
    ADDITIONAL_BOOTSTRAP_LIST = []  # list of host:port


class ClusterConfig(BaseConfig):
    P2P_PORT = 38291
    JSON_RPC_PORT = 38391
    PRIVATE_JSON_RPC_PORT = 38491
    ENABLE_TRANSACTION_HISTORY = False

    DB_PATH_ROOT = "./db"
    LOG_LEVEL = "info"

    MINE = False
    CLEAN = False

    MASTER = None
    SLAVE_LIST = None
    SIMPLE_NETWORK = None
    P2P = None

    def __init__(self):
        self.MASTER = MasterConfig()
        self.SLAVE_LIST = []
        self._json_filepath = None

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
        parser.add_argument("--num_slaves", default=4, type=int)
        parser.add_argument("--port_start", default=38000, type=int)
        parser.add_argument("--db_path_root", default=ClusterConfig.DB_PATH_ROOT, type=str)
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

        parser.add_argument("--seed_host", default=SimpleNetworkConfig.BOOTSTRAP_HOST)
        parser.add_argument("--seed_port", default=SimpleNetworkConfig.BOOTSTRAP_PORT)
        parser.add_argument("--devp2p", default=False, type=bool)
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

    @classmethod
    def create_from_args(cls, args):
        """ Create ClusterConfig either from the JSON file or cmd flags.
        """
        if args.cluster_config:
            config_dict = json.load(open(args.cluster_config))
            config = cls.from_dict(config_dict)
            config.json_filepath = args.cluster_config
            return config

        config = ClusterConfig()
        config.LOG_LEVEL = args.log_level
        config.DB_PATH_ROOT = args.db_path_root

        config.P2P_PORT = args.p2p_port
        config.JSON_RPC_PORT = args.json_rpc_port
        config.PRIVATE_JSON_RPC_PORT = args.json_rpc_private_port

        config.CLEAN = args.clean
        config.MINE = args.mine
        config.ENABLE_TRANSACTION_HISTORY = args.enable_transaction_history

        if args.devp2p:
            config.P2P = P2PConfig()
            config.P2P.IP = args.devp2p_ip
            config.P2P.DISCOVERY_PORT = args.devp2p_port
            config.P2P.BOOTSTRAP_HOST = args.devp2p_bootstrap_host
            config.P2P.BOOTSTRAP_PORT = args.devp2p_bootstrap_port
            config.P2P.MIN_PEERS = args.devp2p_min_peers
            config.P2P.MAX_PEERS = args.devp2p_max_peers
            config.P2P.ADDITIONAL_BOOTSTRAP_LIST = args.devp2p_additional_bootstrap_list
        else:
            config.SIMPLE_NETWORK = SimpleNetworkConfig()
            config.SIMPLE_NETWORK.BOOTSTRAP_HOST = args.seed_host
            config.SIMPLE_NETWORK.BOOTSTRAP_PORT = args.seed_port

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

    def to_dict(self):
        ret = super().to_dict()
        ret["MASTER"] = self.MASTER.to_dict()
        ret["SLAVE_LIST"] = [s.to_dict() for s in self.SLAVE_LIST]
        if self.P2P:
            ret["P2P"] = self.P2P.to_dict()
        else:
            ret["SIMPLE_NETWORK"] = self.SIMPLE_NETWORK.to_dict()
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.MASTER = MasterConfig.from_dict(config.MASTER)
        config.SLAVE_LIST = [SlaveConfig.from_dict(s) for s in config.SLAVE_LIST]

        if "P2P" in d:
            config.P2P = P2PConfig.from_dict(d["P2P"])
        else:
            config.SIMPLE_NETWORK = SimpleNetworkConfig.from_dict(d["SIMPLE_NETWORK"])

        return config

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)
