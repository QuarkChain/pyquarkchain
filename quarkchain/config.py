
import copy
import quarkchain.db
from quarkchain.core import Address
from quarkchain.utils import is_p2, int_left_most_bit, sha3_256
from quarkchain.diff import MADifficultyCalculator


class DefaultConfig:

    def __init__(self):
        self.P2P_PROTOCOL_VERSION = 0
        self.P2P_COMMAND_SIZE_LIMIT = (2 ** 32) - 1  # unlimited right now
        self.P2P_SERVER_PORT = 38291
        self.LOCAL_SERVER_PORT = 38391
        self.LOCAL_SERVER_ENABLE = False
        self.SHARD_SIZE = 4
        self.SHARD_SIZE_BITS = 2
        self.GENESIS_ACCOUNT = Address.createFrom(
            '2bd6cc571427aa46a5e413ccbab5b9a759d08fb5142cbcb8')
        self.GENESIS_COIN = 10 ** 28
        self.GENESIS_MINOR_COIN = 0
        self.GENESIS_DIFFICULTY = 100
        self.GENESIS_MINOR_DIFFICULTY = self.GENESIS_DIFFICULTY // self.SHARD_SIZE
        # 2018/2/2 5 am 7 min 38 sec
        self.GENESIS_CREATE_TIME = 1517547849
        self.PROOF_OF_PROGRESS_BLOCKS = 1
        self.SKIP_ROOT_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_COINBASE_CHECK = False
        #  0 is mainnet
        self.NETWORK_ID = 0
        self.TESTNET_MASTER_ACCOUNT = self.GENESIS_ACCOUNT
        self.ROOT_BLOCK_INTERVAL_SEC = 150
        self.ROOT_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=3600 // self.ROOT_BLOCK_INTERVAL_SEC,
            targetIntervalSec=self.ROOT_BLOCK_INTERVAL_SEC,
            bootstrapSamples=3600 // self.ROOT_BLOCK_INTERVAL_SEC)
        self.MINOR_BLOCK_INTERVAL_SEC = 15
        self.MINOR_DIFF_CALCULATOR = MADifficultyCalculator(
            maSamples=3600 // self.MINOR_BLOCK_INTERVAL_SEC,
            targetIntervalSec=self.MINOR_BLOCK_INTERVAL_SEC,
            bootstrapSamples=3600 // self.MINOR_BLOCK_INTERVAL_SEC)
        # TODO: Use ASIC-resistent hash algorithm
        self.DIFF_HASH_FUNC = sha3_256
        # Decimal level
        self.QUARKSH_TO_JIAOZI = 10 ** 18

    def setShardSize(self, shardSize):
        assert(is_p2(shardSize))
        self.SHARD_SIZE = shardSize
        self.SHARD_SIZE_BITS = int_left_most_bit(shardSize) - 1

    def copy(self):
        return copy.copy(self)


class Env:

    def __init__(self, db=None, config=None):
        self.db = db or quarkchain.db.InMemoryDb()
        self.config = config or DefaultConfig()

    def copy(self):
        return copy.copy(self)


DEFAULT_ENV = Env()
