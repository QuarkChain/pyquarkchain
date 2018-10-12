import json
from enum import Enum
from typing import List
from eth_keys import KeyAPI
import quarkchain.db
import quarkchain.evm.config
from quarkchain.core import Address

# Decimal level
QUARKSH_TO_JIAOZI = 10 ** 18


class NetworkId:
    MAINNET = 1
    # TESTNET_FORD = 2
    TESTNET_PORSCHE = 3


def _is_config_field(s: str):
    return s.isupper() and not s.startswith("_")


class BaseConfig:
    def to_dict(self):
        ret = dict()
        for k, v in self.__class__.__dict__.items():
            if _is_config_field(k):
                ret[k] = getattr(self, k) if k in self.__dict__ else v
        return ret

    @classmethod
    def from_dict(cls, d):
        config = cls()
        for k, v in d.items():
            setattr(config, k, v)
        return config

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    @classmethod
    def from_json(cls, j):
        return cls.from_dict(json.loads(j))

    def __eq__(self, other):
        d1 = dict()
        d2 = dict()
        for k, v in self.__class__.__dict__.items():
            if _is_config_field(k):
                d1[k] = getattr(self, k) if k in self.__dict__ else v
        for k, v in other.__class__.__dict__.items():
            if _is_config_field(k):
                d2[k] = getattr(other, k) if k in other.__dict__ else v
        return d1 == d2


class RootGenesis(BaseConfig):
    VERSION = 0
    HEIGHT = 0
    SHARD_SIZE = 32
    COINBASE_ADDRESS = bytes(24).hex()
    COINBASE_AMOUNT = 5
    HASH_PREV_BLOCK = bytes(32).hex()
    HASH_MERKLE_ROOT = bytes(32).hex()
    # 2018/2/2 5 am 7 min 38 sec
    TIMESTAMP = 1519147489
    DIFFICULTY = 1000000
    NONCE = 0


class ShardGenesis(BaseConfig):
    ROOT_HEIGHT = 0  # hash_prev_root_block should be the root block of this height
    VERSION = 0
    HEIGHT = 0
    COINBASE_ADDRESS = bytes(24).hex()
    COINBASE_AMOUNT = 5
    HASH_PREV_MINOR_BLOCK = bytes(32).hex()
    HASH_MERKLE_ROOT = bytes(32).hex()
    EXTRA_DATA = b"It was the best of times, it was the worst of times, ... - Charles Dickens".hex()
    TIMESTAMP = RootGenesis.TIMESTAMP
    DIFFICULTY = 10000
    GAS_LIMIT = 30000 * 400  # 400 xshard tx
    NONCE = 0
    ALLOC = None  # dict() hex address -> qkc amount

    def __init__(self):
        self.ALLOC = dict()

    def to_dict(self):
        ret = super().to_dict()
        ret["ALLOC"] = dict()
        return ret


class ConsensusType(Enum):
    NONE = 0  # no shard
    POW_ETHASH = 1
    POW_SHA3SHA3 = 2
    POW_SIMULATE = 3

    @classmethod
    def pow_types(cls):
        return [cls.POW_ETHASH, cls.POW_SHA3SHA3, cls.POW_SIMULATE]


class POWConfig(BaseConfig):
    TARGET_BLOCK_TIME = 10
    REMOTE_MINE = False


class ShardConfig(BaseConfig):
    CONSENSUS_TYPE = ConsensusType.NONE
    # Only set when CONSENSUS_TYPE is not NONE
    CONSENSUS_CONFIG = None  # type: POWConfig
    GENESIS = None  # ShardGenesis

    # Gas Limit
    GAS_LIMIT_EMA_DENOMINATOR = 1024
    GAS_LIMIT_ADJUSTMENT_FACTOR = 1024
    GAS_LIMIT_MINIMUM = 5000
    GAS_LIMIT_MAXIMUM = 2 ** 63 - 1

    GAS_LIMIT_USAGE_ADJUSTMENT_NUMERATOR = 3
    GAS_LIMIT_USAGE_ADJUSTMENT_DENOMINATOR = 2

    def __init__(self):
        self._root_config = None
        self.GENESIS = ShardGenesis()

    @property
    def root_config(self):
        return self._root_config

    @root_config.setter
    def root_config(self, value):
        self._root_config = value

    @property
    def max_blocks_per_shard_in_one_root_block(self) -> int:
        # TODO: need to add a constant to counter the block time variance
        return (
            int(
                self.root_config.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
                / self.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
            )
            + 3
        )

    @property
    def max_stale_minor_block_height_diff(self):
        return int(
            self.root_config.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF
            * self.root_config.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
            / self.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
        )

    @property
    def max_minor_blocks_in_memory(self):
        return self.max_stale_minor_block_height_diff * 2

    def to_dict(self):
        ret = super().to_dict()
        ret["CONSENSUS_TYPE"] = self.CONSENSUS_TYPE.name
        if self.CONSENSUS_TYPE == ConsensusType.NONE:
            del ret["CONSENSUS_CONFIG"]
            del ret["GENESIS"]
        else:
            ret["CONSENSUS_CONFIG"] = self.CONSENSUS_CONFIG.to_dict()
            if self.GENESIS:
                ret["GENESIS"] = self.GENESIS.to_dict()
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.CONSENSUS_TYPE = ConsensusType[config.CONSENSUS_TYPE]
        if config.CONSENSUS_TYPE in ConsensusType.pow_types():
            config.CONSENSUS_CONFIG = POWConfig.from_dict(config.CONSENSUS_CONFIG)
            if config.GENESIS:
                config.GENESIS = ShardGenesis.from_dict(config.GENESIS)
        return config


class RootConfig(BaseConfig):
    # To ignore super old blocks from peers
    # This means the network will fork permanently after a long partition
    MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF = 60

    CONSENSUS_TYPE = ConsensusType.NONE  # type: ConsensusType
    # Only set when CONSENSUS_TYPE is not NONE
    CONSENSUS_CONFIG = None  # type: POWConfig
    GENESIS = None  # RootGenesis

    def __init__(self):
        self.GENESIS = RootGenesis()

    @property
    def max_root_blocks_in_memory(self):
        return self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF * 2

    def to_dict(self):
        ret = super().to_dict()
        ret["CONSENSUS_TYPE"] = self.CONSENSUS_TYPE.name
        if self.CONSENSUS_TYPE == ConsensusType.NONE:
            del ret["CONSENSUS_CONFIG"]
            del ret["GENESIS"]
        else:
            ret["CONSENSUS_CONFIG"] = self.CONSENSUS_CONFIG.to_dict()
            ret["GENESIS"] = self.GENESIS.to_dict()
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.CONSENSUS_TYPE = ConsensusType[config.CONSENSUS_TYPE]
        if config.CONSENSUS_TYPE in ConsensusType.pow_types():
            config.CONSENSUS_CONFIG = POWConfig.from_dict(config.CONSENSUS_CONFIG)
            config.GENESIS = RootGenesis.from_dict(config.GENESIS)
        return config


class QuarkChainConfig(BaseConfig):
    SHARD_SIZE = 8

    MAX_NEIGHBORS = 32

    # Block reward
    MINOR_BLOCK_DEFAULT_REWARD = 100 * QUARKSH_TO_JIAOZI

    NETWORK_ID = NetworkId.TESTNET_PORSCHE
    TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD = 10000
    BLOCK_EXTRA_DATA_SIZE_LIMIT = 1024

    PROOF_OF_PROGRESS_BLOCKS = 1
    TESTNET_MASTER_ADDRESS = "199bcc2ebf71a851e388bd926595376a49bdaa329c6485f3"
    GUARDIAN_PUBLIC_KEY = "00" * 64  # TODO: update this

    # P2P
    P2P_PROTOCOL_VERSION = 0
    P2P_COMMAND_SIZE_LIMIT = (2 ** 32) - 1  # unlimited right now

    # Testing related
    SKIP_ROOT_DIFFICULTY_CHECK = False
    SKIP_MINOR_DIFFICULTY_CHECK = False

    ROOT = None  # type: RootConfig
    SHARD_LIST = None

    def __init__(self):
        self.loadtest_accounts = []  # for TransactionGenerator

        self.ROOT = RootConfig()
        self.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        self.ROOT.CONSENSUS_CONFIG = POWConfig()
        self.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 10
        self.ROOT.GENESIS.SHARD_SIZE = self.SHARD_SIZE

        self.SHARD_LIST = []  # type: List[ShardConfig]
        for i in range(self.SHARD_SIZE):
            s = ShardConfig()
            s.root_config = self.ROOT
            s.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
            s.CONSENSUS_CONFIG = POWConfig()
            s.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 3
            self.SHARD_LIST.append(s)

    def get_genesis_root_height(self, shard_id) -> int:
        """ Return the root block height at which the shard shall be created"""
        return self.SHARD_LIST[shard_id].GENESIS.ROOT_HEIGHT

    def get_genesis_shard_ids(self) -> List[int]:
        """ Return a list of ids for shards that have GENESIS"""
        return [i for i, config in enumerate(self.SHARD_LIST) if config.GENESIS]

    def get_initialized_shard_ids_before_root_height(self, root_height) -> List[int]:
        """ Return a list of ids of the shards that have been initialized before a certain root height"""
        ids = []
        for shard_id, config in enumerate(self.SHARD_LIST):
            if config.GENESIS and config.GENESIS.ROOT_HEIGHT < root_height:
                ids.append(shard_id)
        return ids

    @property
    def testnet_master_address(self):
        return Address.create_from(self.TESTNET_MASTER_ADDRESS)

    @property
    def guardian_public_key(self) -> KeyAPI.PublicKey:
        return KeyAPI.PublicKey(
            public_key_bytes=bytes.fromhex(self.GUARDIAN_PUBLIC_KEY)
        )

    def update(self, shard_size, root_block_time, minor_block_time):
        self.SHARD_SIZE = shard_size

        self.ROOT = RootConfig()
        self.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        self.ROOT.CONSENSUS_CONFIG = POWConfig()
        self.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = root_block_time
        self.ROOT.GENESIS.SHARD_SIZE = self.SHARD_SIZE

        self.SHARD_LIST = []
        for i in range(self.SHARD_SIZE):
            s = ShardConfig()
            s.root_config = self.ROOT
            s.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
            s.CONSENSUS_CONFIG = POWConfig()
            s.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = minor_block_time
            s.GENESIS.COINBASE_ADDRESS = (
                Address.create_empty_account(i).serialize().hex()
            )
            self.SHARD_LIST.append(s)

    def to_dict(self):
        ret = super().to_dict()
        ret["ROOT"] = self.ROOT.to_dict()
        ret["SHARD_LIST"] = [s.to_dict() for s in self.SHARD_LIST]
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.ROOT = RootConfig.from_dict(config.ROOT)
        config.SHARD_LIST = [ShardConfig.from_dict(s) for s in config.SHARD_LIST]
        for s in config.SHARD_LIST:
            s.root_config = config.ROOT
        return config


def get_default_evm_config():
    return dict(quarkchain.evm.config.config_metropolis)
