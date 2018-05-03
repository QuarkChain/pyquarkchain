import copy

import quarkchain.db
from quarkchain.core import Address
from quarkchain.utils import is_p2, int_left_most_bit, sha3_256
from quarkchain.diff import MADifficultyCalculator
import quarkchain.evm.config


class NetworkId:
    MAINNET = 1
    TESTNET_FORD = 2


class DefaultConfig:

    def __init__(self):
        self.P2P_PROTOCOL_VERSION = 0
        self.P2P_COMMAND_SIZE_LIMIT = (2 ** 32) - 1  # unlimited right now
        self.P2P_SERVER_PORT = 38291
        self.P2P_SEED_HOST = "127.0.0.1"
        self.P2P_SEED_PORT = self.P2P_SERVER_PORT
        self.LOCAL_SERVER_PORT = 38391
        self.LOCAL_SERVER_ENABLE = False
        self.SHARD_SIZE = 8
        self.SHARD_SIZE_BITS = 2

        # Difficulty related
        self.DIFF_MA_INTERVAL = 60
        self.ROOT_BLOCK_INTERVAL_SEC = 15
        self.MINOR_BLOCK_INTERVAL_SEC = 3
        # TODO: Use ASIC-resistent hash algorithm
        self.DIFF_HASH_FUNC = sha3_256

        # Decimal level
        self.QUARKSH_TO_JIAOZI = 10 ** 18
        self.MINOR_BLOCK_DEFAULT_REWARD = 100 * self.QUARKSH_TO_JIAOZI

        # Distribute pre-mined quarkash into different shards for faster distribution
        self.GENESIS_ACCOUNT = Address.createFrom("199bcc2ebf71a851e388bd926595376a49bdaa329c6485f3")
        # The key is only here to sign artificial transactions for load test
        self.GENESIS_KEY = bytes.fromhex("c987d4506fb6824639f9a9e3b8834584f5165e94680501d1b0044071cd36c3b3")
        self.GENESIS_COIN = 0
        self.GENESIS_MINOR_COIN = self.QUARKSH_TO_JIAOZI * (10 ** 10) // self.SHARD_SIZE
        self.GENESIS_DIFFICULTY = 1000000
        self.GENESIS_MINOR_DIFFICULTY = \
            self.GENESIS_DIFFICULTY * self.MINOR_BLOCK_INTERVAL_SEC // \
            self.SHARD_SIZE // self.ROOT_BLOCK_INTERVAL_SEC
        # 2018/2/2 5 am 7 min 38 sec
        self.GENESIS_CREATE_TIME = 1519147489
        self.PROOF_OF_PROGRESS_BLOCKS = 1
        self.SKIP_ROOT_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_COINBASE_CHECK = False
        self.ROOT_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=self.DIFF_MA_INTERVAL // self.ROOT_BLOCK_INTERVAL_SEC,
            targetIntervalSec=self.ROOT_BLOCK_INTERVAL_SEC,
            bootstrapSamples=self.DIFF_MA_INTERVAL // self.ROOT_BLOCK_INTERVAL_SEC,
            minimumDiff=self.GENESIS_DIFFICULTY)
        self.MINOR_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=self.DIFF_MA_INTERVAL // self.MINOR_BLOCK_INTERVAL_SEC,
            targetIntervalSec=self.MINOR_BLOCK_INTERVAL_SEC,
            bootstrapSamples=self.DIFF_MA_INTERVAL // self.MINOR_BLOCK_INTERVAL_SEC,
            minimumDiff=self.GENESIS_MINOR_DIFFICULTY)

        #  1 is mainnet
        self.NETWORK_ID = NetworkId.MAINNET
        self.TESTNET_MASTER_ACCOUNT = self.GENESIS_ACCOUNT

        # Unlimited yet
        self.TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD = 2 ** 32
        self.TRANSACTION_LIMIT_PER_BLOCK = 4096

        self.BLOCK_EXTRA_DATA_SIZE_LIMIT = 1024

    def setShardSize(self, shardSize):
        assert(is_p2(shardSize))
        self.SHARD_SIZE = shardSize
        self.SHARD_SIZE_BITS = int_left_most_bit(shardSize) - 1

    def copy(self):
        return copy.copy(self)


class DefaultClusterConfig:

    def __init__(self):
        self.NODE_PORT = 38290
        # Empty means Master
        self.SHARD_MASK_LIST = []
        self.ID = ""
        self.CONFIG = None
        self.MASTER_TO_SLAVE_CONNECT_RETRY_DELAY = 1.0

    def copy(self):
        return copy.copy(self)


def get_default_evm_config():
    return dict(quarkchain.evm.config.config_metropolis)


class Env:

    def __init__(self, db=None, config=None, evmConfig=None, clusterConfig=None):
        self.db = db or quarkchain.db.InMemoryDb()
        self.config = config or DefaultConfig()
        self.evmConfig = evmConfig or get_default_evm_config()
        self.evmEnv = quarkchain.evm.config.Env(db=self.db, config=self.evmConfig)
        self.clusterConfig = clusterConfig or DefaultClusterConfig()

    def copy(self):
        return Env(self.db, self.config.copy(), dict(self.evmConfig), self.clusterConfig.copy())


DEFAULT_ENV = Env()
