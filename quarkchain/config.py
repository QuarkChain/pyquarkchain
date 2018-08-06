import copy

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
        self.P2P_PROTOCOL_VERSION = 0
        self.P2P_COMMAND_SIZE_LIMIT = (2 ** 32) - 1  # unlimited right now
        self.P2P_SERVER_PORT = 38291
        self.P2P_SEED_HOST = "127.0.0.1"
        self.P2P_SEED_PORT = self.P2P_SERVER_PORT
        self.DEVP2P = False
        self.DEVP2P_IP = ""
        self.DEVP2P_PORT = 29000
        self.DEVP2P_BOOTSTRAP_HOST = "0.0.0.0"
        self.DEVP2P_BOOTSTRAP_PORT = 29000
        self.DEVP2P_MIN_PEERS = 2
        self.DEVP2P_MAX_PEERS = 10
        self.DEVP2P_ADDITIONAL_BOOTSTRAPS = ''
        self.LOCAL_SERVER_PORT = 38391  # TODO: cleanup
        self.LOCAL_SERVER_ENABLE = False  # TODO: cleanup
        self.PUBLIC_JSON_RPC_PORT = 38391
        self.PRIVATE_JSON_RPC_PORT = 38491

        self.SHARD_SIZE = 8
        self.SHARD_SIZE_BITS = int_left_most_bit(self.SHARD_SIZE) - 1

        # Difficulty related
        self.DIFF_MA_INTERVAL = 60
        self.ROOT_BLOCK_INTERVAL_SEC = 15
        self.MINOR_BLOCK_INTERVAL_SEC = 3
        # TODO: Use ASIC-resistent hash algorithm
        self.DIFF_HASH_FUNC = sha3_256

        # To ignore super old blocks from peers
        # This means the network will fork permanently after a long partition
        self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF = 60
        self.MAX_STALE_MINOR_BLOCK_HEIGHT_DIFF = int(
            self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF * self.ROOT_BLOCK_INTERVAL_SEC / self.MINOR_BLOCK_INTERVAL_SEC)

        self.MAX_ROOT_BLOCK_IN_MEMORY = self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF * 2
        self.MAX_MINOR_BLOCK_IN_MEMORY = self.MAX_STALE_MINOR_BLOCK_HEIGHT_DIFF * 2

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
        self.ROOT_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=45,
            diffFactor=2048,
            minimumDiff=self.GENESIS_DIFFICULTY)
        self.MINOR_DIFF_CALCULATOR = EthDifficultyCalculator(
            cutoff=9,
            diffFactor=2048,
            minimumDiff=self.GENESIS_MINOR_DIFFICULTY)

        self.NETWORK_ID = NetworkId.TESTNET_PORSCHE
        self.TESTNET_MASTER_ACCOUNT = self.GENESIS_ACCOUNT

        self.TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD = 10000
        self.TRANSACTION_LIMIT_PER_BLOCK = 4096

        self.BLOCK_EXTRA_DATA_SIZE_LIMIT = 1024

        # testnet config
        self.ACCOUNTS_TO_FUND = [Address.createFrom(item["address"][:40] + item["address"][0:2] + item["address"][10:12] + item["address"][20:22] + item["address"][30:32]) for item in ACCOUNTS_TO_FUND]
        self.ACCOUNTS_TO_FUND_COIN = 1000000 * self.QUARKSH_TO_JIAOZI
        self.LOADTEST_ACCOUNTS = [(Address.createFrom(item["address"][:40] + item["address"][0:2] + item["address"][10:12] + item["address"][20:22] + item["address"][30:32]), bytes.fromhex(item["key"])) for item in LOADTEST_ACCOUNTS]
        self.LOADTEST_ACCOUNTS_COIN = self.ACCOUNTS_TO_FUND_COIN

        # whether to index transaction by address
        self.ENABLE_TRANSACTION_HISTORY = True

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
        self.DB_PATH_ROOT = None
        self.DB_CLEAN = False

    def copy(self):
        return copy.copy(self)


def get_default_evm_config():
    return dict(quarkchain.evm.config.config_metropolis)


class Env:

    def __init__(self, db=None, config=None, evmConfig=None, clusterConfig=None):
        self.db = db or quarkchain.db.InMemoryDb()
        self.config = config or DefaultConfig()
        self.evmConfig = evmConfig or get_default_evm_config()
        self.evmConfig["NETWORK_ID"] = self.config.NETWORK_ID
        self.evmEnv = quarkchain.evm.config.Env(db=self.db, config=self.evmConfig)
        self.clusterConfig = clusterConfig or DefaultClusterConfig()

    def setNetworkId(self, networkId):
        self.config.NETWORK_ID = networkId
        self.evmConfig["NETWORK_ID"] = networkId

    def copy(self):
        return Env(self.db, self.config.copy(), dict(self.evmConfig), self.clusterConfig.copy())


DEFAULT_ENV = Env()