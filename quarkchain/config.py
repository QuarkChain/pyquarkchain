import copy
import json
from enum import Enum

import quarkchain.db
import quarkchain.evm.config
from quarkchain.core import Address
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.loadtest.accounts import LOADTEST_ACCOUNTS
from quarkchain.testnet.accounts_to_fund import ACCOUNTS_TO_FUND
from quarkchain.utils import is_p2, int_left_most_bit, sha3_256


class NetworkId:
    MAINNET = 1
    # TESTNET_FORD = 2
    TESTNET_PORSCHE = 3


class DefaultConfig:
    def __init__(self):
        self.SHARD_SIZE = 8

        # Difficulty related
        self.DIFF_MA_INTERVAL = 60
        self.ROOT_BLOCK_INTERVAL_SEC = 15
        self.MINOR_BLOCK_INTERVAL_SEC = 3
        # TODO: Use ASIC-resistent hash algorithm
        self.DIFF_HASH_FUNC = sha3_256

        self.MAX_BLOCKS_PER_SHARD_IN_ONE_ROOT_BLOCK = (
            self.ROOT_BLOCK_INTERVAL_SEC / self.MINOR_BLOCK_INTERVAL_SEC + 3
        )

        self.MAX_NEIGHBORS = 32

        # To ignore super old blocks from peers
        # This means the network will fork permanently after a long partition
        self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF = 60
        self.MAX_STALE_MINOR_BLOCK_HEIGHT_DIFF = int(
            self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF
            * self.ROOT_BLOCK_INTERVAL_SEC
            / self.MINOR_BLOCK_INTERVAL_SEC
        )

        self.MAX_ROOT_BLOCK_IN_MEMORY = self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF * 2
        self.MAX_MINOR_BLOCK_IN_MEMORY = self.MAX_STALE_MINOR_BLOCK_HEIGHT_DIFF * 2

        # Decimal level
        self.QUARKSH_TO_JIAOZI = 10 ** 18
        self.MINOR_BLOCK_DEFAULT_REWARD = 100 * self.QUARKSH_TO_JIAOZI

        # Distribute pre-mined quarkash into different shards for faster distribution
        self.GENESIS_ACCOUNT = Address.create_from(
            "199bcc2ebf71a851e388bd926595376a49bdaa329c6485f3"
        )
        # The key is only here to sign artificial transactions for load test
        self.GENESIS_KEY = bytes.fromhex(
            "c987d4506fb6824639f9a9e3b8834584f5165e94680501d1b0044071cd36c3b3"
        )
        self.GENESIS_COIN = 0
        self.GENESIS_MINOR_COIN = self.QUARKSH_TO_JIAOZI * (10 ** 10) // self.SHARD_SIZE
        self.GENESIS_DIFFICULTY = 1000000
        self.GENESIS_MINOR_DIFFICULTY = (
            self.GENESIS_DIFFICULTY
            * self.MINOR_BLOCK_INTERVAL_SEC
            // self.SHARD_SIZE
            // self.ROOT_BLOCK_INTERVAL_SEC
        )
        # 2018/2/2 5 am 7 min 38 sec
        self.GENESIS_CREATE_TIME = 1519147489
        self.PROOF_OF_PROGRESS_BLOCKS = 1
        self.SKIP_ROOT_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_COINBASE_CHECK = False
        self.ROOT_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=45, diff_factor=2048, minimum_diff=self.GENESIS_DIFFICULTY
        )
        self.MINOR_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=9, diff_factor=2048, minimum_diff=self.GENESIS_MINOR_DIFFICULTY
        )

        self.NETWORK_ID = NetworkId.TESTNET_PORSCHE
        self.TESTNET_MASTER_ACCOUNT = self.GENESIS_ACCOUNT

        self.TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD = 10000
        self.TRANSACTION_LIMIT_PER_BLOCK = 4096

        self.BLOCK_EXTRA_DATA_SIZE_LIMIT = 1024

        # testnet config
        self.ACCOUNTS_TO_FUND = [
            Address.create_from(item["address"]) for item in ACCOUNTS_TO_FUND
        ]
        self.ACCOUNTS_TO_FUND_COIN = 1000000 * self.QUARKSH_TO_JIAOZI
        self.LOADTEST_ACCOUNTS = [
            (Address.create_from(item["address"]), bytes.fromhex(item["key"]))
            for item in LOADTEST_ACCOUNTS
        ]
        self.LOADTEST_ACCOUNTS_COIN = self.ACCOUNTS_TO_FUND_COIN

    def set_shard_size(self, shard_size):
        assert is_p2(shard_size)
        self.SHARD_SIZE = shard_size

    def copy(self):
        return copy.copy(self)


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


class ShardConfig(BaseConfig):
    CONSENSUS_TYPE = ConsensusType.NONE
    CONSENSUS_CONFIG = None  # Only set when CONSENSUS_TYPE is not NONE

    def __init__(self):
        self._root_config = None

    @property
    def root_config(self):
        return self._root_config

    @root_config.setter
    def root_config(self, value):
        self._root_config = value

    @property
    def max_blocks_per_shard_in_one_root_block(self):
        return self.root_config.CONSENSUS_CONFIG.TARGET_BLOCK_TIME / self.CONSENSUS_CONFIG.TARGET_BLOCK_TIME

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
        else:
            ret["CONSENSUS_CONFIG"] = self.CONSENSUS_CONFIG.to_dict()
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.CONSENSUS_TYPE = ConsensusType[config.CONSENSUS_TYPE]
        if config.CONSENSUS_TYPE in ConsensusType.pow_types():
            config.CONSENSUS_CONFIG = POWConfig.from_dict(config.CONSENSUS_CONFIG)
        return config


class RootConfig(ShardConfig):
    # To ignore super old blocks from peers
    # This means the network will fork permanently after a long partition
    MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF = 60

    @property
    def max_root_blocks_in_memory(self):
        return self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF * 2


class QuarkChainConfig(BaseConfig):
    SHARD_SIZE = 8

    MAX_NEIGHBORS = 32

    # Decimal level
    QUARKSH_TO_JIAOZI = 10 ** 18

    # Block reward
    MINOR_BLOCK_DEFAULT_REWARD = 100 * QUARKSH_TO_JIAOZI

    NETWORK_ID = NetworkId.TESTNET_PORSCHE
    TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD = 10000
    BLOCK_EXTRA_DATA_SIZE_LIMIT = 1024

    # TODO: Genesis block should be imported from a file
    GENESIS_ADDRESS = "0x199bcc2ebf71a851e388bd926595376a49bdaa329c6485f3"
    GENESIS_KEY = "0xc987d4506fb6824639f9a9e3b8834584f5165e94680501d1b0044071cd36c3b3"

    # P2P
    P2P_PROTOCOL_VERSION = 0
    P2P_COMMAND_SIZE_LIMIT = (2 ** 32) - 1  # unlimited right now

    # Testing related
    SKIP_ROOT_DIFFICULTY_CHECK = False
    SKIP_MINOR_DIFFICULTY_CHECK = False
    SKIP_MINOR_COINBASE_CHECK = False

    ROOT = None
    SHARD_LIST = None

    def __init__(self):
        self.ROOT = RootConfig()
        self.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        self.ROOT.CONSENSUS_CONFIG = POWConfig()
        self.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 10

        self.SHARD_LIST = []
        for i in range(self.SHARD_SIZE):
            s = ShardConfig()
            s.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
            s.CONSENSUS_CONFIG = POWConfig()
            s.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 3
            self.SHARD_LIST.append(s)

    def update(self, shard_size, root_block_time, minor_block_time):
        self.SHARD_SIZE = shard_size

        self.ROOT = RootConfig()
        self.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        self.ROOT.CONSENSUS_CONFIG = POWConfig()
        self.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = root_block_time

        self.SHARD_LIST = []
        for i in range(self.SHARD_SIZE):
            s = ShardConfig()
            s.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
            s.CONSENSUS_CONFIG = POWConfig()
            s.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = minor_block_time
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
        return config


def get_default_evm_config():
    return dict(quarkchain.evm.config.config_metropolis)

