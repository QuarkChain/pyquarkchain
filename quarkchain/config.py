
import copy
import quarkchain.db
from quarkchain.core import Address
from quarkchain.utils import is_p2, int_left_most_bit


class DefaultConfig:

    def __init__(self):
        self.P2P_PROTOCOL_VERSION = 0
        self.P2P_COMMAND_SIZE_LIMIT = (2 ** 32) - 1  # unlimited right now
        self.P2P_SERVER_PORT = 38291
        self.SHARD_SIZE = 4
        self.SHARD_SIZE_BITS = 2
        self.GENESIS_ACCOUNT = Address.createFrom(
            '2bd6cc571427aa46a5e413ccbab5b9a759d08fb5142cbcb8')
        self.GENESIS_COIN = 10 ** 28
        self.GENESIS_MINOR_COIN = 0
        self.GENESIS_DIFFICULTY = 100
        self.GENESIS_MINOR_DIFFICULTY = 25
        # 2018/2/2 5 am 7 min 38 sec
        self.GENESIS_CREATE_TIME = 1517547849
        self.PROOF_OF_PROGRESS_BLOCKS = 1
        self.SKIP_ROOT_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_COINBASE_CHECK = False
        #  0 is mainnet
        self.NETWORK_ID = 0
        self.TESTNET_MASTER_ACCOUNT = self.GENESIS_ACCOUNT

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
